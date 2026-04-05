
import json
import os
import threading
import time
from datetime import datetime, timezone

import boto3
import can
import paho.mqtt.client as mqtt

BASE = "/data/vgapp"
CERT = f"{BASE}/certs/device.pem.crt"
KEY = f"{BASE}/certs/private.pem.key"
CA = f"{BASE}/certs/AmazonRootCA1.pem"
CAN_IDS_FILE = f"{BASE}/can_ids.json"
AWS_STATUS_FILE = f"{BASE}/aws_status.json"

with open(f"{BASE}/config.json", encoding="utf-8") as f:
    cfg = json.load(f)

s3_cfg_path = f"{BASE}/s3.json"
if os.path.exists(s3_cfg_path):
    try:
        with open(s3_cfg_path, encoding="utf-8") as f:
            s3_cfg = json.load(f)
        cfg.update(
            {
                "s3_bucket": s3_cfg.get("s3_bucket"),
                "s3_prefix": s3_cfg.get("s3_prefix"),
                "s3_region": s3_cfg.get("s3_region"),
                "s3_flush_interval_sec": s3_cfg.get(
                    "s3_flush_interval_sec",
                    cfg.get("s3_flush_interval_sec", 30),
                ),
                "s3_batch_size": s3_cfg.get(
                    "s3_batch_size",
                    cfg.get("s3_batch_size", 100),
                ),
            }
        )
    except Exception as e:
        print(f"Error loading s3.json: {e}", flush=True)

DEVICE_ID = cfg["device_id"]
ENDPOINT = cfg["aws_endpoint"]
MQTT_PORT = int(cfg.get("mqtt_port", 8883))
TOPIC_PREFIX = cfg.get("mqtt_topic_prefix", "vg710")
HEARTBEAT_INTERVAL = int(cfg.get("heartbeat_interval_sec", 10))
CAN_CHANNEL = cfg.get("can_channel", "can0")
STATUS_TOPIC = f"{TOPIC_PREFIX}/{DEVICE_ID}/status"
HEARTBEAT_TOPIC = f"{TOPIC_PREFIX}/{DEVICE_ID}/heartbeat"
CAN_TOPIC = f"{TOPIC_PREFIX}/{DEVICE_ID}/can/raw"

S3_BUCKET = cfg.get("s3_bucket")
S3_PREFIX = cfg.get("s3_prefix", "vg710-raw")
S3_REGION = cfg.get("s3_region")
S3_FLUSH_INTERVAL_SEC = int(cfg.get("s3_flush_interval_sec", 30))
S3_BATCH_SIZE = int(cfg.get("s3_batch_size", 100))

CAN_RATES = cfg.get("can_upload_rates", [])


def parse_can_id(value):
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text.lower().startswith("0x"):
            return int(text, 16)
        return int(text, 10)
    raise ValueError(f"Unsupported CAN ID value: {value}")


CAN_RATE_MAP = {}
for item in CAN_RATES:
    try:
        can_id = parse_can_id(item["can_id"])
        interval_sec = int(item["interval_sec"])
        if interval_sec > 0:
            CAN_RATE_MAP[can_id] = interval_sec
    except Exception:
        pass

SEEN_IDS = set()
LATEST_MESSAGES = {}
LAST_PUBLISHED = {}
LOCK = threading.Lock()
MQTT_CONNECTED = False

S3_BUFFER = []
S3_LAST_FLUSH = time.time()
S3_CLIENT = boto3.client("s3", region_name=S3_REGION) if S3_BUCKET else None


def now():
    return datetime.now(timezone.utc).isoformat()


def save_seen_ids():
    try:
        formatted = []
        for can_id in sorted(SEEN_IDS):
            formatted.append(
                {
                    "id": can_id,
                    "id_hex": hex(can_id),
                    "rate_limit_sec": CAN_RATE_MAP.get(can_id),
                }
            )
        with open(CAN_IDS_FILE, "w", encoding="utf-8") as f:
            json.dump(formatted, f, indent=2)
    except Exception as e:
        print(f"Error saving CAN IDs: {e}", flush=True)


def save_aws_status(connected, message):
    try:
        payload = {
            "connected": connected,
            "message": message,
            "last_update": now(),
        }
        with open(AWS_STATUS_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
    except Exception as e:
        print(f"Error saving AWS status: {e}", flush=True)


def append_s3_record(payload):
    global S3_LAST_FLUSH
    if not S3_CLIENT or not S3_BUCKET:
        return

    with LOCK:
        S3_BUFFER.append(payload)
        should_flush = (
            len(S3_BUFFER) >= S3_BATCH_SIZE
            or (time.time() - S3_LAST_FLUSH) >= S3_FLUSH_INTERVAL_SEC
        )

    if should_flush:
        flush_s3_buffer()


def flush_s3_buffer(force=False):
    global S3_LAST_FLUSH
    if not S3_CLIENT or not S3_BUCKET:
        return

    with LOCK:
        if not S3_BUFFER:
            return
        if (
            not force
            and len(S3_BUFFER) < S3_BATCH_SIZE
            and (time.time() - S3_LAST_FLUSH) < S3_FLUSH_INTERVAL_SEC
        ):
            return
        batch = list(S3_BUFFER)
        S3_BUFFER.clear()
        S3_LAST_FLUSH = time.time()

    first_ts = batch[0].get("ts", now())
    ts_part = first_ts.replace(":", "-")
    date_part = first_ts[:10]
    hour_part = first_ts[11:13] if len(first_ts) >= 13 else "00"
    key = f"{S3_PREFIX}/{DEVICE_ID}/{date_part}/{hour_part}/{ts_part}.ndjson"
    body = "\n".join(json.dumps(item, separators=(",", ":")) for item in batch) + "\n"

    try:
        S3_CLIENT.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/x-ndjson",
        )
        save_aws_status(True, f"s3 upload ok: {key}")
    except Exception as e:
        save_aws_status(False, f"s3 upload error: {e}")
        print(f"S3 upload error: {e}", flush=True)
        with LOCK:
            S3_BUFFER[:0] = batch


client = mqtt.Client(client_id=DEVICE_ID, protocol=mqtt.MQTTv311)
client.reconnect_delay_set(min_delay=1, max_delay=30)


def on_connect(client, userdata, flags, rc):
    global MQTT_CONNECTED
    MQTT_CONNECTED = rc == 0
    print(f"MQTT connected rc={rc}", flush=True)
    save_aws_status(rc == 0, f"rc={rc}")


def on_disconnect(client, userdata, rc):
    global MQTT_CONNECTED
    MQTT_CONNECTED = False
    print(f"MQTT disconnected rc={rc}", flush=True)
    save_aws_status(False, f"rc={rc}")


def on_log(client, userdata, level, buf):
    print(f"MQTT log: {buf}", flush=True)


client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_log = on_log
client.tls_set(ca_certs=CA, certfile=CERT, keyfile=KEY)
client.will_set(
    STATUS_TOPIC,
    payload=json.dumps({"device_id": DEVICE_ID, "status": "offline", "ts": now()}),
    qos=1,
    retain=False,
)

print(f"Connecting to AWS endpoint={ENDPOINT} port={MQTT_PORT}", flush=True)
client.connect(ENDPOINT, MQTT_PORT, 60)
client.loop_start()
print("MQTT loop started", flush=True)

result = client.publish(
    STATUS_TOPIC,
    payload=json.dumps({"device_id": DEVICE_ID, "status": "online", "ts": now()}),
    qos=1,
    retain=False,
)
print(f"Initial status publish rc={result.rc}", flush=True)

if result.rc == mqtt.MQTT_ERR_SUCCESS:
    save_aws_status(True, "initial publish ok")
else:
    save_aws_status(False, f"initial publish rc={result.rc}")


def publish(topic, payload, qos=0):
    if not MQTT_CONNECTED:
        save_aws_status(False, f"skip publish, not connected: {topic}")
        return

    result = client.publish(
        topic,
        payload=json.dumps(payload),
        qos=qos,
        retain=False,
    )
    if result.rc == mqtt.MQTT_ERR_SUCCESS:
        save_aws_status(True, f"publish ok: {topic}")
    else:
        save_aws_status(False, f"publish rc={result.rc}: {topic}")

    if topic == CAN_TOPIC:
        append_s3_record(payload)


def heartbeat():
    while True:
        publish(
            HEARTBEAT_TOPIC,
            {
                "device_id": DEVICE_ID,
                "type": "heartbeat",
                "status": "online",
                "ts": now(),
            },
            qos=1,
        )
        time.sleep(HEARTBEAT_INTERVAL)


def can_reader_loop():
    print(f"Opening CAN channel={CAN_CHANNEL}", flush=True)
    bus = can.interface.Bus(channel=CAN_CHANNEL, interface="socketcan")
    print("CAN opened", flush=True)

    while True:
        msg = bus.recv()
        if msg is None:
            continue

        with LOCK:
            if msg.arbitration_id not in SEEN_IDS:
                SEEN_IDS.add(msg.arbitration_id)
                save_seen_ids()

            LATEST_MESSAGES[msg.arbitration_id] = {
                "device_id": DEVICE_ID,
                "channel": CAN_CHANNEL,
                "id": msg.arbitration_id,
                "id_hex": hex(msg.arbitration_id),
                "extended": bool(msg.is_extended_id),
                "remote": bool(msg.is_remote_frame),
                "error": bool(msg.is_error_frame),
                "dlc": msg.dlc,
                "data": list(msg.data),
                "data_hex": msg.data.hex().upper(),
                "ts": now(),
                "rate_limit_sec": CAN_RATE_MAP.get(msg.arbitration_id),
            }


def can_publisher_loop():
    while True:
        now_ts = time.time()

        with LOCK:
            configured_ids = list(CAN_RATE_MAP.items())
            messages_snapshot = dict(LATEST_MESSAGES)

        for can_id, interval_sec in configured_ids:
            payload = messages_snapshot.get(can_id)
            if payload is None:
                continue

            last_ts = LAST_PUBLISHED.get(can_id, 0)
            if now_ts - last_ts < interval_sec:
                continue

            publish(CAN_TOPIC, payload, qos=0)
            LAST_PUBLISHED[can_id] = now_ts

        time.sleep(0.1)


def s3_flush_loop():
    while True:
        flush_s3_buffer()
        time.sleep(1)


threading.Thread(target=heartbeat, daemon=True).start()
threading.Thread(target=can_reader_loop, daemon=True).start()
threading.Thread(target=can_publisher_loop, daemon=True).start()
threading.Thread(target=s3_flush_loop, daemon=True).start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    pass
finally:
    flush_s3_buffer(force=True)
    client.publish(
        STATUS_TOPIC,
        payload=json.dumps({"device_id": DEVICE_ID, "status": "stopping", "ts": now()}),
        qos=1,
        retain=False,
    )
    client.loop_stop()
    client.disconnect()

import json
import os

from flask import Flask, redirect, render_template_string, request, url_for
from werkzeug.utils import secure_filename

app = Flask(__name__)

BASE_DIR = "/data/vgapp"
CERT_DIR = os.path.join(BASE_DIR, "certs")
os.makedirs(BASE_DIR, exist_ok=True)
os.makedirs(CERT_DIR, exist_ok=True)

HTML = """
<!DOCTYPE html>
<html>
<head>
  <title>VG710 Config Upload</title>
  <meta http-equiv="refresh" content="30">
</head>
<body>
  <h1>VG710 Config Upload</h1>
  <div id="aws-status" style="margin: 12px 0; padding: 10px; border: 1px solid #999; display: inline-block;">
    AWS status: <strong>{{ aws_status_text }}</strong>
  </div>

  <h2>Upload Config</h2>
  <form method="post" enctype="multipart/form-data" action="/upload_config">
    <input type="file" name="file">
    <input type="submit" value="Upload">
  </form>

  <h2>Upload Certs</h2>
  <form method="post" enctype="multipart/form-data" action="/upload_cert">
    <input type="file" name="file">
    <input type="submit" value="Upload">
  </form>

  <h2>CAN Update Rates</h2>
  <form method="post" action="/save_rates">
    <table border="1" cellpadding="6" cellspacing="0">
      <tr>
        <th>CAN ID</th>
        <th>Interval (sec)</th>
      </tr>
      {% if rate_rows %}
        {% for row in rate_rows %}
        <tr>
          <td><input type="text" name="can_id_{{ loop.index0 }}" value="{{ row.can_id }}"></td>
          <td><input type="number" min="1" name="interval_{{ loop.index0 }}" value="{{ row.interval_sec }}"></td>
        </tr>
        {% endfor %}
      {% else %}
        <tr>
          <td><input type="text" name="can_id_0" value=""></td>
          <td><input type="number" min="1" name="interval_0" value="1"></td>
        </tr>
      {% endif %}
      {% for idx in range(3) %}
      <tr>
        <td><input type="text" name="new_can_id_{{ idx }}" value=""></td>
        <td><input type="number" min="1" name="new_interval_{{ idx }}" value="1"></td>
      </tr>
      {% endfor %}
    </table>
    <p><small>Gebruik CAN ID in decimaal of hex, bijvoorbeeld 914 of 0x392.</small></p>
    <input type="submit" value="Save CAN Rates">
  </form>

  <h2>Detected CAN IDs</h2>
  <ul>
  {% for cid in can_ids %}
    <li>
      {% if cid.id_hex is defined %}
        {{ cid.id_hex }}
      {% elif cid.id is defined %}
        {{ cid.id }}
      {% else %}
        {{ cid }}
      {% endif %}
    </li>
  {% else %}
    <li>No CAN IDs detected yet</li>
  {% endfor %}
  </ul>

  <h2>Status</h2>
  <ul>
  <li>config.json: {{ config }}</li>
  <li>device.pem.crt: {{ crt }}</li>
  <li>private.pem.key: {{ key }}</li>
  <li>AmazonRootCA1.pem: {{ ca }}</li>
  </ul>

<script>
async function refreshStatus() {
  try {
    const response = await fetch('/status_json', { cache: 'no-store' });
    if (!response.ok) {
      return;
    }
    const data = await response.json();

    const awsStatus = document.getElementById('aws-status');
    if (awsStatus) {
      awsStatus.innerHTML = 'AWS status: <strong>' + data.aws_status_text + '</strong>';
    }
  } catch (e) {
    // ignore polling errors
  }
}

setInterval(refreshStatus, 5000);
</script>
</body>
</html>
"""


def exists(path):
    return "✅" if os.path.exists(path) else "❌"


def load_config_data():
    config_path = f"{BASE_DIR}/config.json"
    if not os.path.exists(config_path):
        return {}
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_config_data(data):
    with open(f"{BASE_DIR}/config.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def normalize_can_id(value):
    text = value.strip()
    if not text:
        return None
    try:
        if text.lower().startswith("0x"):
            return hex(int(text, 16))
        return str(int(text, 10))
    except ValueError:
        return None


def current_can_rates():
    config = load_config_data()
    rates = config.get("can_upload_rates", [])
    if not isinstance(rates, list):
        return []
    return rates


def current_can_ids():
    path = f"{BASE_DIR}/can_ids.json"
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
    except Exception:
        pass
    return []


def build_rate_rows():
    detected = current_can_ids()
    config_rates = current_can_rates()

    interval_map = {}
    for item in config_rates:
        can_id = item.get("can_id")
        interval_sec = item.get("interval_sec")
        if can_id is not None:
            interval_map[str(can_id)] = interval_sec

    rows = []
    seen = set()

    for item in detected:
        can_id = item.get("id_hex") or item.get("id")
        if can_id is None:
            continue
        can_id = str(can_id)
        seen.add(can_id)
        rows.append(
            {
                "can_id": can_id,
                "interval_sec": interval_map.get(can_id, item.get("rate_limit_sec") or 1),
            }
        )

    for item in config_rates:
        can_id = item.get("can_id")
        if can_id is None:
            continue
        can_id = str(can_id)
        if can_id in seen:
            continue
        rows.append(
            {
                "can_id": can_id,
                "interval_sec": item.get("interval_sec", 1),
            }
        )

    return rows


def aws_status_data():
    path = f"{BASE_DIR}/aws_status.json"
    if not os.path.exists(path):
        return {"connected": False, "last_update": None, "message": "unknown"}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {"connected": False, "last_update": None, "message": "unknown"}


def aws_status_text():
    data = aws_status_data()
    if data.get("connected"):
        last_update = data.get("last_update") or "n/a"
        return f"online (last update: {last_update})"
    message = data.get("message") or "offline"
    return f"offline ({message})"


def get_uploaded_file():
    if "file" not in request.files:
        return None, ("No file field in request", 400)
    uploaded = request.files["file"]
    if uploaded.filename == "":
        return None, ("No file selected", 400)
    return uploaded, None


def resolve_cert_target(filename):
    safe_name = secure_filename(filename)
    if safe_name == "AmazonRootCA1.pem":
        return "AmazonRootCA1.pem"
    if safe_name == "private.pem.key" or safe_name.endswith("-private.pem.key"):
        return "private.pem.key"
    if safe_name == "device.pem.crt" or safe_name.endswith("-certificate.pem.crt"):
        return "device.pem.crt"
    return None


@app.route("/")
def index():
    return render_template_string(
        HTML,
        config=exists(f"{BASE_DIR}/config.json"),
        crt=exists(f"{CERT_DIR}/device.pem.crt"),
        key=exists(f"{CERT_DIR}/private.pem.key"),
        ca=exists(f"{CERT_DIR}/AmazonRootCA1.pem"),
        rates=current_can_rates(),
        can_ids=current_can_ids(),
        rate_rows=build_rate_rows(),
        aws_status_text=aws_status_text(),
        range=range,
    )


@app.route("/status_json")
def status_json():
    return {
        "aws_status_text": aws_status_text(),
        "config": exists(f"{BASE_DIR}/config.json"),
        "crt": exists(f"{CERT_DIR}/device.pem.crt"),
        "key": exists(f"{CERT_DIR}/private.pem.key"),
        "ca": exists(f"{CERT_DIR}/AmazonRootCA1.pem"),
        "can_ids": current_can_ids(),
        "rate_rows": build_rate_rows(),
    }


@app.route("/upload_config", methods=["POST"])
def upload_config():
    uploaded, error = get_uploaded_file()
    if error:
        return error
    filename = secure_filename(uploaded.filename)
    if filename != "config.json":
        return "Upload the file as config.json", 400
    uploaded.save(f"{BASE_DIR}/config.json")
    return redirect(url_for("index"))


@app.route("/upload_cert", methods=["POST"])
def upload_cert():
    uploaded, error = get_uploaded_file()
    if error:
        return error
    filename = secure_filename(uploaded.filename)
    if filename == "config.json":
        uploaded.save(f"{BASE_DIR}/config.json")
        return redirect(url_for("index"))
    target_name = resolve_cert_target(filename)
    if target_name is None:
        return (
            "Unknown filename. Use config.json or valid certificate files.",
            400,
        )
    uploaded.save(os.path.join(CERT_DIR, target_name))
    return redirect(url_for("index"))


@app.route("/save_rates", methods=["POST"])
def save_rates():
    config = load_config_data()
    new_rates = []

    index = 0
    while True:
        can_id_key = f"can_id_{index}"
        interval_key = f"interval_{index}"
        if can_id_key not in request.form:
            break

        can_id = normalize_can_id(request.form.get(can_id_key, ""))
        interval_raw = request.form.get(interval_key, "").strip()
        if can_id and interval_raw:
            try:
                interval_sec = int(interval_raw)
                if interval_sec > 0:
                    new_rates.append(
                        {
                            "can_id": can_id,
                            "interval_sec": interval_sec,
                        }
                    )
            except ValueError:
                pass
        index += 1

    for index in range(3):
        can_id = normalize_can_id(request.form.get(f"new_can_id_{index}", ""))
        interval_raw = request.form.get(f"new_interval_{index}", "").strip()
        if can_id and interval_raw:
            try:
                interval_sec = int(interval_raw)
                if interval_sec > 0:
                    new_rates.append(
                        {
                            "can_id": can_id,
                            "interval_sec": interval_sec,
                        }
                    )
            except ValueError:
                pass

    config["can_upload_rates"] = new_rates
    save_config_data(config)
    return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)