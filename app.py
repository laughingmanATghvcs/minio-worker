import os
import sys
import json
import requests
from kafka import KafkaConsumer

# --- CONFIGURATION ---
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "my-cluster-kafka-bootstrap.kafka.svc:9092")
TOPIC = "my-notifications"
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK")          # For Success Logs
DISCORD_ALERTS_WEBHOOK = os.environ.get("DISCORD_ALERTS_WEBHOOK") # For Critical Alerts
PD_ROUTING_KEY = os.environ.get("PD_ROUTING_KEY")

print(f"Starting Worker... listening to {TOPIC}", file=sys.stderr)

# --- HELPER: Trigger Alerts (PagerDuty + Discord Alarm) ---
def trigger_alarm(filename, error_message):
    print(f"❌ ALARM TRIGGERED: {filename}", file=sys.stderr)
    
    # 1. Send to PagerDuty
    if PD_ROUTING_KEY:
        url = "https://events.pagerduty.com/v2/enqueue"
        payload = {
            "routing_key": PD_ROUTING_KEY,
            "event_action": "trigger",
            "dedup_key": f"backup_fail_{filename}",
            "payload": {
                "summary": f"CRITICAL: Backup Failed for {filename}",
                "severity": "critical",
                "source": "talos-minio-worker",
                "component": "minio",
                "custom_details": {"error": error_message, "file": filename}
            }
        }
        try:
            requests.post(url, json=payload)
            print(" -> PagerDuty sent.", file=sys.stderr)
        except Exception as e:
            print(f" -> PagerDuty failed: {e}", file=sys.stderr)

    # 2. Send to Discord #infra-alerts
    if DISCORD_ALERTS_WEBHOOK:
        alert_msg = {
            # Change @here to <@YOUR_ID> if you want a direct ping
            "content": f"🚨 **CRITICAL ALARM** 🚨\n**File:** `{filename}`\n**Error:** {error_message}\n@here" 
        }
        try:
            requests.post(DISCORD_ALERTS_WEBHOOK, json=alert_msg)
            print(" -> Discord Alarm sent.", file=sys.stderr)
        except Exception as e:
            print(f" -> Discord Alarm failed: {e}", file=sys.stderr)

# --- HELPER: Log Success ---
def log_success(message):
    if DISCORD_WEBHOOK:
        try:
            requests.post(DISCORD_WEBHOOK, json={"content": message})
        except Exception as e:
            print(f"Discord Log failed: {e}", file=sys.stderr)

# --- MAIN LOOP ---
try:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
except Exception as e:
    print(f"CRITICAL ERROR connecting to Kafka: {e}", file=sys.stderr)
    sys.exit(1)

print("✅ Connected to Kafka.", file=sys.stderr)

for message in consumer:
    try:
        data = message.value
        event_name = data.get("EventName", "")

        # Filter for Uploads (Put) or Created
        if "s3:ObjectCreated" in event_name or "Put" in event_name:
            records = data.get("Records", [])
            if not records: continue
                
            record = records[0]
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            
            # FIX: Use .get() so 0-byte files don't crash the script
            size = record['s3']['object'].get('size', 0) 

            print(f"Processing: {key} ({size} bytes)", file=sys.stderr)

            # === LOGIC ===
            
            # SCENARIO 1: File too small (Corruption check)
            if size < 1024: 
                err = f"File is only {size} bytes. Minimum required is 1KB."
                trigger_alarm(key, err)
            
            # SCENARIO 2: Success
            else:
                log_success(f"✅ **Backup Successful**\nFile: `{key}`\nSize: {size} bytes\nBucket: {bucket}")
                
    except Exception as e:
        print(f"Error processing message: {e}", file=sys.stderr)
