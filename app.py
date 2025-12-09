import os
import json
import requests
from kafka import KafkaConsumer

# Read config from Kubernetes Environment Variables
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "my-cluster-kafka-bootstrap.kafka.svc:9092")
TOPIC = "my-notifications"
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK")
DISCORD_ALERTS_WEBHOOK = os.environ.get("DISCORD_ALERTS_WEBHOOK")
PD_ROUTING_KEY = os.environ.get("PD_ROUTING_KEY")

print(f"Starting Worker... listening to {TOPIC}")

# --- HELPER FUNCTION: Trigger PagerDuty ---
def trigger_pagerduty(filename, error_message):
    url = "https://events.pagerduty.com/v2/enqueue"
    payload = {
        "routing_key": PD_ROUTING_KEY,
        "event_action": "trigger",
        "dedup_key": f"backup_fail_{filename}",  # Prevents spamming the same alert
        "payload": {
            "summary": f"CRITICAL: Backup Failed for {filename}",
            "severity": "critical",
            "source": "talos-minio-worker",
            "component": "minio",
            "custom_details": {
                "error_details": error_message,
                "file": filename
            }
        }
    }
    try:
        r = requests.post(url, json=payload)
        print(f"PagerDuty Alert Sent: {r.status_code}")
    except Exception as e:
        print(f"Failed to send PagerDuty alert: {e}")
    # 2. ALSO Send to Discord #infra-alerts
    if DISCORD_ALERTS_WEBHOOK:
        alert_msg = {
            "content": f"🚨 **CRITICAL ALARM** 🚨\n**File:** `{filename}`\n**Error:** {error_message}\n<@&YOUR_ROLE_ID>" 
            # <--- adding <@userid> or <@&roleid> will PING you in Discord!
        }
        requests.post(DISCORD_ALERTS_WEBHOOK, json=alert_msg)        
# --- HELPER FUNCTION: Log to Discord ---
def log_to_discord(message):
    requests.post(DISCORD_WEBHOOK, json={"content": message})

# --- MAIN LOOP ---
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    data = message.value
    event_name = data.get("EventName", "")

    # Filter for Uploads
    if "s3:ObjectCreated" in event_name or "Put" in event_name:
        records = data.get("Records", [])
        if not records: continue
            
        record = records[0]
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']     # Filename
        size = record['s3']['object'].get('size', 0)   # Size in Bytes

        print(f"Processing: {key} ({size} bytes)")

        # === THE LOGIC ===
        
        # SCENARIO 1: File is too small (Corruption check)
        # Example: If a backup is smaller than 1KB, it's probably broken.
        if size < 1024: 
            error_msg = f"File {key} is only {size} bytes. Expected > 1KB."
            print("❌ Failure detected. Alerting PagerDuty...")
            trigger_pagerduty(key, error_msg)
        
        # SCENARIO 2: Success
        else:
            print("✅ Success. Logging to Discord.")
            log_to_discord(f"✅ **Backup Successful**\nFile: `{key}`\nSize: {size}")
