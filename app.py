
import base64
import json
import os
import socket
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
S3_NMEA_BATCH_SIZE = int(cfg.get("s3_nmea_batch_size", 100))
S3_NMEA_FLUSH_INTERVAL_SEC = int(cfg.get("s3_nmea_flush_interval_sec", 30))

CAN_RATES = cfg.get("can_upload_rates", [])
NTRIP_CFG = cfg.get("ntrip", {})
SEPTENTRIO_CFG = cfg.get("septentrio", {})

NTRIP_ENABLED = bool(NTRIP_CFG.get("enabled", False))
NTRIP_HOST = NTRIP_CFG.get("host", "").strip()
NTRIP_PORT = int(NTRIP_CFG.get("port", 2101) or 2101)
NTRIP_MOUNTPOINT = NTRIP_CFG.get("mountpoint", "").strip()
NTRIP_USERNAME = NTRIP_CFG.get("username", "").strip()
NTRIP_PASSWORD = NTRIP_CFG.get("password", "")
SEPTENTRIO_IP = SEPTENTRIO_CFG.get("ip", "192.168.127.250").strip() or "192.168.127.250"
SEPTENTRIO_RTCM_PORT = int(SEPTENTRIO_CFG.get("port", 28784) or 28784)
NTRIP_RECONNECT_SEC = int(NTRIP_CFG.get("reconnect_sec", 5) or 5)


def build_nmea_sources():
    sources = []

    configured = SEPTENTRIO_CFG.get("nmea_sources", [])
    if isinstance(configured, list):
        for idx, item in enumerate(configured):
            if not isinstance(item, dict):
                continue
            port = item.get("port")
            if not port:
                continue
            try:
                port = int(port)
            except Exception:
                continue
            sources.append(
                {
                    "name": item.get("name", f"nmea_{idx + 1}"),
                    "host": item.get("host", SEPTENTRIO_IP) or SEPTENTRIO_IP,
                    "port": port,
                }
            )

    if sources:
        return sources

    legacy_ports = SEPTENTRIO_CFG.get("nmea_ports", [])
    if isinstance(legacy_ports, list):
        for idx, port in enumerate(legacy_ports):
            try:
                port = int(port)
            except Exception:
                continue
            sources.append(
                {
                    "name": f"nmea_{idx + 1}",
                    "host": SEPTENTRIO_IP,
                    "port": port,
                }
            )

    return sources


NMEA_SOURCES = build_nmea_sources()


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

NMEA_LOCK = threading.Lock()
NMEA_BUFFER = []
NMEA_LAST_FLUSH = time.time()


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


def upload_batch_to_s3(key, batch):
    if not S3_CLIENT or not S3_BUCKET or not batch:
        return

    body = "\n".join(json.dumps(item, separators=(",", ":")) for item in batch) + "\n"
    S3_CLIENT.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )


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
    key = f"{S3_PREFIX}/{DEVICE_ID}/can/{date_part}/{hour_part}/{ts_part}.ndjson"

    try:
        upload_batch_to_s3(key, batch)
        save_aws_status(True, f"s3 upload ok: {key}")
    except Exception as e:
        save_aws_status(False, f"s3 upload error: {e}")
        print(f"S3 upload error: {e}", flush=True)
        with LOCK:
            S3_BUFFER[:0] = batch


def append_nmea_record(payload):
    global NMEA_LAST_FLUSH
    if not S3_CLIENT or not S3_BUCKET:
        return

    with NMEA_LOCK:
        NMEA_BUFFER.append(payload)
        should_flush = (
            len(NMEA_BUFFER) >= S3_NMEA_BATCH_SIZE
            or (time.time() - NMEA_LAST_FLUSH) >= S3_NMEA_FLUSH_INTERVAL_SEC
        )

    if should_flush:
        flush_nmea_buffer()


def flush_nmea_buffer(force=False):
    global NMEA_LAST_FLUSH
    if not S3_CLIENT or not S3_BUCKET:
        return

    with NMEA_LOCK:
        if not NMEA_BUFFER:
            return
        if (
            not force
            and len(NMEA_BUFFER) < S3_NMEA_BATCH_SIZE
            and (time.time() - NMEA_LAST_FLUSH) < S3_NMEA_FLUSH_INTERVAL_SEC
        ):
            return
        batch = list(NMEA_BUFFER)
        NMEA_BUFFER.clear()
        NMEA_LAST_FLUSH = time.time()

    first_ts = batch[0].get("ts", now())
    ts_part = first_ts.replace(":", "-")
    date_part = first_ts[:10]
    hour_part = first_ts[11:13] if len(first_ts) >= 13 else "00"
    key = f"{S3_PREFIX}/{DEVICE_ID}/nmea/{date_part}/{hour_part}/{ts_part}.ndjson"

    try:
        upload_batch_to_s3(key, batch)
        save_aws_status(True, f"s3 upload ok: {key}")
    except Exception as e:
        save_aws_status(False, f"s3 upload error: {e}")
        print(f"S3 upload error: {e}", flush=True)
        with NMEA_LOCK:
            NMEA_BUFFER[:0] = batch


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
        flush_nmea_buffer()
        time.sleep(1)


def connect_ntrip_socket():
    if not (NTRIP_HOST and NTRIP_MOUNTPOINT):
        raise RuntimeError("NTRIP host or mountpoint is not configured")

    sock = socket.create_connection((NTRIP_HOST, NTRIP_PORT), timeout=15)
    auth = base64.b64encode(f"{NTRIP_USERNAME}:{NTRIP_PASSWORD}".encode("utf-8")).decode("ascii")
    request = (
        f"GET /{NTRIP_MOUNTPOINT} HTTP/1.0\r\n"
        f"User-Agent: VG710-NTRIP\r\n"
        f"Authorization: Basic {auth}\r\n"
        f"Ntrip-Version: Ntrip/2.0\r\n"
        f"Connection: close\r\n\r\n"
    )
    sock.sendall(request.encode("ascii"))

    response = b""
    while b"\r\n\r\n" not in response and len(response) < 8192:
        chunk = sock.recv(1024)
        if not chunk:
            break
        response += chunk

    header = response.decode("latin1", errors="ignore")
    if not (
        "200 OK" in header
        or "ICY 200 OK" in header
        or header.startswith("ICY 200")
    ):
        sock.close()
        raise RuntimeError(f"NTRIP connect failed: {header.strip()}")

    return sock, response.split(b"\r\n\r\n", 1)[1]


def relay_ntrip_loop():
    if not NTRIP_ENABLED:
        print("NTRIP disabled; relay loop not started", flush=True)
        return

    while True:
        ntrip_sock = None
        sept_sock = None
        try:
            print(
                f"Connecting NTRIP caster {NTRIP_HOST}:{NTRIP_PORT}/{NTRIP_MOUNTPOINT}",
                flush=True,
            )
            ntrip_sock, initial_payload = connect_ntrip_socket()
            sept_sock = socket.create_connection((SEPTENTRIO_IP, SEPTENTRIO_RTCM_PORT), timeout=15)
            print(
                f"Forwarding RTCM to Septentrio {SEPTENTRIO_IP}:{SEPTENTRIO_RTCM_PORT}",
                flush=True,
            )

            if initial_payload:
                sept_sock.sendall(initial_payload)

            while True:
                data = ntrip_sock.recv(4096)
                if not data:
                    raise RuntimeError("NTRIP stream closed")
                sept_sock.sendall(data)
        except Exception as e:
            print(f"NTRIP relay error: {e}", flush=True)
        finally:
            try:
                if ntrip_sock:
                    ntrip_sock.close()
            except Exception:
                pass
            try:
                if sept_sock:
                    sept_sock.close()
            except Exception:
                pass

        time.sleep(NTRIP_RECONNECT_SEC)


def nmea_reader_loop(source):
    host = source["host"]
    port = source["port"]
    name = source["name"]

    while True:
        sock = None
        file_obj = None
        try:
            print(f"Connecting NMEA source {name} at {host}:{port}", flush=True)
            sock = socket.create_connection((host, port), timeout=15)
            sock.settimeout(60)
            file_obj = sock.makefile("r", encoding="ascii", errors="ignore", newline="\n")

            while True:
                line = file_obj.readline()
                if not line:
                    raise RuntimeError("NMEA stream closed")
                line = line.strip()
                if not line:
                    continue
                append_nmea_record(
                    {
                        "device_id": DEVICE_ID,
                        "source": name,
                        "host": host,
                        "port": port,
                        "sentence": line,
                        "ts": now(),
                    }
                )
        except Exception as e:
            print(f"NMEA reader error for {name}: {e}", flush=True)
        finally:
            try:
                if file_obj:
                    file_obj.close()
            except Exception:
                pass
            try:
                if sock:
                    sock.close()
            except Exception:
                pass

        time.sleep(5)


threading.Thread(target=heartbeat, daemon=True).start()
threading.Thread(target=can_reader_loop, daemon=True).start()
threading.Thread(target=can_publisher_loop, daemon=True).start()
threading.Thread(target=s3_flush_loop, daemon=True).start()

if NTRIP_ENABLED:
    threading.Thread(target=relay_ntrip_loop, daemon=True).start()

for source in NMEA_SOURCES:
    threading.Thread(target=nmea_reader_loop, args=(source,), daemon=True).start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    pass
finally:
    flush_s3_buffer(force=True)
    flush_nmea_buffer(force=True)
    client.publish(
        STATUS_TOPIC,
        payload=json.dumps({"device_id": DEVICE_ID, "status": "stopping", "ts": now()}),
        qos=1,
        retain=False,
    )
    client.loop_stop()
    client.disconnect()