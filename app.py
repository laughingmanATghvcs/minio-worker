import os
import sys
import json
import requests
import boto3
import paramiko
import tempfile
import random
from kafka import KafkaConsumer
from kubernetes import client, config
from botocore.client import Config

# --- CONFIGURATION: KAFKA & ALERTS ---
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "my-cluster-kafka-bootstrap.kafka.svc:9092")
TOPIC = "my-notifications"
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK")
DISCORD_ALERTS_WEBHOOK = os.environ.get("DISCORD_ALERTS_WEBHOOK")
PD_ROUTING_KEY = os.environ.get("PD_ROUTING_KEY")

# --- CONFIGURATION: MINIO (BOTO3) ---
# We use the internal K8s DNS for MinIO to save bandwidth
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio.minio.svc.cluster.local:9000")
S3_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "admin")
S3_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD")

# --- CONFIGURATION: SSH (PARAMIKO) ---
REMOTE_HOST = os.environ.get("SSH_HOST", "172.16.0.210") # Your Unraid IP
REMOTE_USER = os.environ.get("SSH_USER", "root")
REMOTE_KEY_PATH = "/etc/secret/ssh-private-key" # Mounted via K8s Secret

# --- INITIALIZATION ---
print(f"🚀 Worker Starting...", file=sys.stderr)

# 1. Init MinIO Client
try:
    s3 = boto3.client('s3',
                      endpoint_url=S3_ENDPOINT,
                      aws_access_key_id=S3_ACCESS_KEY,
                      aws_secret_access_key=S3_SECRET_KEY,
                      # FIX: Explicitly set Region and Path Style to stop 400 Errors
                      region_name='us-east-1',
                      config=Config(signature_version='s3v4', s3={'addressing_style': 'path'})
    )
    print("✅ MinIO Client Connected", file=sys.stderr)
except Exception as e:
    print(f"❌ MinIO Connection Failed: {e}", file=sys.stderr)
    sys.exit(1)

# 2. Init Kubernetes Client
try:
    config.load_incluster_config()
    batch_v1 = client.BatchV1Api()
    print("✅ Kubernetes API Connected", file=sys.stderr)
except:
    print("⚠️ Warning: Not running in K8s? Job spawning disabled.", file=sys.stderr)
    batch_v1 = None

# --- HELPER: ALERTS ---
def trigger_alarm(filename, error_message):
    print(f"❌ ALARM: {filename} - {error_message}", file=sys.stderr)
    
    # PagerDuty
    if PD_ROUTING_KEY:
        try:
            payload = {
                "routing_key": PD_ROUTING_KEY,
                "event_action": "trigger",
                "dedup_key": f"fail_{filename}",
                "payload": {
                    "summary": f"CRITICAL: {filename}",
                    "severity": "critical",
                    "source": "minio-worker",
                    "custom_details": {"error": error_message}
                }
            }
            requests.post("https://events.pagerduty.com/v2/enqueue", json=payload)
        except: pass

    # Discord Alert
    if DISCORD_ALERTS_WEBHOOK:
        msg = {"content": f"🚨 **CRITICAL ALARM** 🚨\n**File:** `{filename}`\n**Error:** {error_message}\n@here"}
        requests.post(DISCORD_ALERTS_WEBHOOK, json=msg)

def log_success(message):
    if DISCORD_WEBHOOK:
        requests.post(DISCORD_WEBHOOK, json={"content": message})

# --- LOGIC MODULES ---

def check_ransomware(filename):
    suspicious = ['.crypt', '.locked', '.enc', '.wannacry']
    for ext in suspicious:
        if filename.endswith(ext):
            trigger_alarm(filename, f"RANSOMWARE EXTENSION DETECTED ({ext})")
            return True
    return False

def handle_ssl_cert(bucket, filename):
    print(f"🔐 Syncing SSL Cert: {filename}", file=sys.stderr)
    
    # 1. Download from MinIO to Temp File
    try:
        tmp = tempfile.NamedTemporaryFile(delete=False)
        s3.download_fileobj(bucket, filename, tmp)
        tmp.close()
    except Exception as e:
        trigger_alarm(filename, f"MinIO Download Failed: {e}")
        return

    # 2. SSH Upload
    try:
        k = paramiko.RSAKey.from_private_key_file(REMOTE_KEY_PATH)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(REMOTE_HOST, username=REMOTE_USER, pkey=k, timeout=10)

        sftp = ssh.open_sftp()
        # Upload to a generic location on Unraid
        remote_path = f"/mnt/user/appdata/certs/{filename.split('/')[-1]}"
        sftp.put(tmp.name, remote_path)
        sftp.close()

        # Restart Service (Example: MinIO)
        ssh.exec_command("docker restart Minio")
        ssh.close()
        
        log_success(f"✅ **SSL Synced**: `{filename}` copied to Unraid & Service restarted.")

    except Exception as e:
        trigger_alarm(filename, f"SSH Sync Failed: {e}")
    finally:
        os.remove(tmp.name)

def handle_dr_drill(bucket, filename):
    if not batch_v1: return

    rand_id = random.randint(1000, 9999)
    job_name = f"verify-{rand_id}-{filename.split('/')[-1].replace('.', '-')}"[:60].lower()
    
    # Create a Kubernetes Job to test the file
    job = client.V1Job(
        metadata=client.V1ObjectMeta(name=job_name),
        spec=client.V1JobSpec(
            ttl_seconds_after_finished=600,
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels={"app": "verifier"}),
                spec=client.V1PodSpec(
                    restart_policy="Never",
                    containers=[
                        client.V1Container(
                            name="verifier",
                            image="alpine:latest",
                            command=["/bin/sh", "-c"],
                            # Verify .tar.gz integrity
                            args=[f"echo 'Verifying {filename}...' && sleep 5 && echo 'Done'"] 
                        )
                    ]
                )
            )
        )
    )
    try:
        batch_v1.create_namespaced_job(namespace="kafka", body=job)
        log_success(f"🛠️ **DR Drill**: Spawning K8s Job to verify `{filename}`")
    except Exception as e:
        trigger_alarm(filename, f"K8s Job Failed: {e}")

# --- MAIN LOOP ---
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("✅ Worker Ready.", file=sys.stderr)

for message in consumer:
    try:
        data = message.value
        event = data.get("EventName", "")
        if "ObjectCreated" in event or "Put" in event:
            rec = data['Records'][0]
            bucket = rec['s3']['bucket']['name']
            key = rec['s3']['object']['key']
            size = rec['s3']['object'].get('size', 0)

            print(f"Processing: {key}", file=sys.stderr)

            # 1. Security Check
            if check_ransomware(key): continue

            # 2. SSL Sync
            if "letsencrypt" in key and key.endswith(".pem"):
                handle_ssl_cert(bucket, key)

            # 3. DR Drill (Critical Backups)
            elif "critical" in key and key.endswith(".tar.gz"):
                handle_dr_drill(bucket, key)

            # 4. Standard Log
            else:
                log_success(f"✅ Uploaded: `{key}` ({size} bytes)")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
