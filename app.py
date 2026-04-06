
import base64
import collections
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
CAN_LATEST_FILE = f"{BASE}/can_latest.json"
AWS_STATUS_FILE = f"{BASE}/aws_status.json"
S3_STATUS_FILE = f"{BASE}/s3_status.json"
GNSS_STATUS_FILE = f"{BASE}/gnss_status.json"

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
NTRIP_RECONNECT_SEC = int(NTRIP_CFG.get("reconnect_sec", 5) or 5)

SEPTENTRIO_IP = SEPTENTRIO_CFG.get("ip", "192.168.127.250").strip() or "192.168.127.250"

NTRIP_PROXY_CFG = cfg.get("ntrip_proxy", {})
NTRIP_PROXY_HOST = NTRIP_PROXY_CFG.get("host", "0.0.0.0").strip() or "0.0.0.0"
NTRIP_PROXY_PORT = int(NTRIP_PROXY_CFG.get("port", 7791) or 7791)
NTRIP_PROXY_USERNAME = NTRIP_PROXY_CFG.get("username", "proxyuser")
NTRIP_PROXY_PASSWORD = NTRIP_PROXY_CFG.get("password", "proxypass")
NTRIP_PROXY_MOUNTPOINT = NTRIP_PROXY_CFG.get("mountpoint", "proxymountpoint")


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

S3_STATS_LOCK = threading.Lock()
S3_CAN_STATS = {"total_records": 0, "total_uploads": 0, "last_key": None, "last_upload": None}
S3_NMEA_STATS = {"total_records": 0, "total_uploads": 0, "last_key": None, "last_upload": None}

CAN_LOG_LOCK = threading.Lock()
CAN_LOG = collections.deque(maxlen=300)
CAN_LOG_SEQ = 0

GNSS_LOCK = threading.Lock()
GNSS_STATUS = {
    "fix_quality": 0, "fix_label": "Geen fix",
    "lat": None, "lon": None, "satellites": None,
    "hdop": None, "vdop": None, "pdop": None,
    "altitude": None,
    "acc_lat": None, "acc_lon": None, "acc_alt": None,
    "ts": None,
}


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


def save_s3_status():
    try:
        with S3_STATS_LOCK:
            payload = {
                "can": dict(S3_CAN_STATS),
                "nmea": dict(S3_NMEA_STATS),
            }
        with open(S3_STATUS_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
    except Exception as e:
        print(f"Error saving S3 status: {e}", flush=True)


GGA_FIX_LABELS = {
    0: "Geen fix",
    1: "Standalone",
    2: "DGPS",
    3: "PPS",
    4: "RTK Fixed",
    5: "RTK Float",
    6: "Geschat",
}


def nmea_to_decimal(value, direction):
    if not value:
        return None
    dot = value.index(".")
    degrees = int(value[: dot - 2])
    minutes = float(value[dot - 2:])
    decimal = degrees + minutes / 60.0
    if direction in ("S", "W"):
        decimal = -decimal
    return round(decimal, 8)


def parse_gga(sentence):
    try:
        if "*" in sentence:
            sentence = sentence[: sentence.index("*")]
        parts = sentence.split(",")
        if len(parts) < 10 or not parts[0].endswith("GGA"):
            return None
        fix_quality = int(parts[6]) if parts[6] else 0
        fix_label = GGA_FIX_LABELS.get(fix_quality, f"Fix {fix_quality}")
        if fix_quality == 0:
            return {"fix_quality": 0, "fix_label": fix_label,
                    "lat": None, "lon": None, "satellites": None,
                    "hdop": None, "altitude": None}
        return {
            "fix_quality": fix_quality,
            "fix_label": fix_label,
            "lat": nmea_to_decimal(parts[2], parts[3]),
            "lon": nmea_to_decimal(parts[4], parts[5]),
            "satellites": int(parts[7]) if parts[7] else None,
            "hdop": float(parts[8]) if parts[8] else None,
            "altitude": float(parts[9]) if parts[9] else None,
        }
    except Exception:
        return None


def parse_gsa(sentence):
    """GSA: PDOP, HDOP, VDOP."""
    try:
        if "*" in sentence:
            sentence = sentence[: sentence.index("*")]
        parts = sentence.split(",")
        if len(parts) < 17 or not parts[0].endswith("GSA"):
            return None
        return {
            "pdop": float(parts[15]) if parts[15] else None,
            "hdop": float(parts[16]) if parts[16] else None,
            "vdop": float(parts[17]) if len(parts) > 17 and parts[17] else None,
        }
    except Exception:
        return None


def parse_gst(sentence):
    """GST: positienauwkeurigheid in meters (1-sigma)."""
    try:
        if "*" in sentence:
            sentence = sentence[: sentence.index("*")]
        parts = sentence.split(",")
        if len(parts) < 8 or not parts[0].endswith("GST"):
            return None
        return {
            "acc_lat": float(parts[6]) if parts[6] else None,
            "acc_lon": float(parts[7]) if parts[7] else None,
            "acc_alt": float(parts[8]) if len(parts) > 8 and parts[8] else None,
        }
    except Exception:
        return None


def update_gnss_status(sentence, ts):
    parsed = None
    if "GGA" in sentence:
        parsed = parse_gga(sentence)
    elif "GSA" in sentence:
        parsed = parse_gsa(sentence)
    elif "GST" in sentence:
        parsed = parse_gst(sentence)

    if parsed is None:
        return

    with GNSS_LOCK:
        GNSS_STATUS.update(parsed)
        GNSS_STATUS["ts"] = ts

    try:
        with GNSS_LOCK:
            payload = dict(GNSS_STATUS)
        with open(GNSS_STATUS_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
    except Exception as e:
        print(f"Error saving GNSS status: {e}", flush=True)


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
        with S3_STATS_LOCK:
            S3_CAN_STATS["total_records"] += len(batch)
            S3_CAN_STATS["total_uploads"] += 1
            S3_CAN_STATS["last_key"] = key
            S3_CAN_STATS["last_upload"] = now()
        save_s3_status()
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
        with S3_STATS_LOCK:
            S3_NMEA_STATS["total_records"] += len(batch)
            S3_NMEA_STATS["total_uploads"] += 1
            S3_NMEA_STATS["last_key"] = key
            S3_NMEA_STATS["last_upload"] = now()
        save_s3_status()
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

        ts = now()
        payload = {
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
            "ts": ts,
            "rate_limit_sec": CAN_RATE_MAP.get(msg.arbitration_id),
        }

        with LOCK:
            if msg.arbitration_id not in SEEN_IDS:
                SEEN_IDS.add(msg.arbitration_id)
                save_seen_ids()
            LATEST_MESSAGES[msg.arbitration_id] = payload

        with CAN_LOG_LOCK:
            global CAN_LOG_SEQ
            CAN_LOG_SEQ += 1
            CAN_LOG.append({
                "seq": CAN_LOG_SEQ,
                "id_hex": payload["id_hex"],
                "dlc": payload["dlc"],
                "data_hex": payload["data_hex"],
                "ts": ts,
            })

        append_s3_record(payload)


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


def save_can_latest():
    try:
        with LOCK:
            snapshot = list(LATEST_MESSAGES.values())
        snapshot.sort(key=lambda m: m.get("id", 0))
        with CAN_LOG_LOCK:
            log_snapshot = list(CAN_LOG)
        with open(CAN_LATEST_FILE, "w", encoding="utf-8") as f:
            json.dump({"latest": snapshot, "log": log_snapshot}, f, separators=(",", ":"))
    except Exception as exc:
        print(f"Error saving can_latest.json: {exc}", flush=True)


def s3_flush_loop():
    while True:
        flush_s3_buffer()
        flush_nmea_buffer()
        save_can_latest()
        time.sleep(1)


def get_current_ntrip_upstream():
    """Lees actuele upstream NTRIP-configuratie uit config.json (ondersteunt runtime-updates)."""
    try:
        with open(f"{BASE}/config.json", encoding="utf-8") as f:
            c = json.load(f)
        n = c.get("ntrip", {})
        return (
            n.get("host", "").strip(),
            int(n.get("port", 2101) or 2101),
            n.get("mountpoint", "").strip(),
            n.get("username", "").strip(),
            n.get("password", ""),
        )
    except Exception:
        return NTRIP_HOST, NTRIP_PORT, NTRIP_MOUNTPOINT, NTRIP_USERNAME, NTRIP_PASSWORD


def connect_ntrip_socket(gga_sentence=None):
    host, port, mountpoint, username, password = get_current_ntrip_upstream()
    if not (host and mountpoint):
        raise RuntimeError("NTRIP host or mountpoint is not configured")

    sock = socket.create_connection((host, port), timeout=15)
    auth = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    request = (
        f"GET /{mountpoint} HTTP/1.0\r\n"
        f"User-Agent: NTRIP PythonClient/1.0\r\n"
        f"Authorization: Basic {auth}\r\n"
        f"Accept: */*\r\n"
        f"Connection: close\r\n"
    )
    if gga_sentence:
        request += f"Ntrip-GGA: {gga_sentence}\r\n"
    request += "\r\n"
    sock.sendall(request.encode("ascii", errors="ignore"))

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

    initial_payload = b""
    if b"\r\n\r\n" in response:
        initial_payload = response.split(b"\r\n\r\n", 1)[1]

    return sock, initial_payload


def extract_gga_from_request(raw_request):
    try:
        text = raw_request.decode("latin1", errors="ignore")
    except Exception:
        return None

    for line in text.split("\r\n"):
        if line.startswith("$") and "GGA" in line:
            return line.strip()
        if line.lower().startswith("ntrip-gga:"):
            value = line.split(":", 1)[1].strip()
            if value.startswith("$") and "GGA" in value:
                return value
    return None



def bridge_upstream_to_client(client_sock, upstream_sock):
    while True:
        data = upstream_sock.recv(4096)
        if not data:
            raise RuntimeError("Upstream NTRIP stream gesloten")
        client_sock.sendall(data)



def bridge_client_to_upstream(client_sock, upstream_sock):
    while True:
        data = client_sock.recv(1024)
        if not data:
            raise RuntimeError("NTRIP client verbinding gesloten")
        if b"GGA" in data:
            upstream_sock.sendall(data)


def proxy_rtcm_to_client(client_sock, gga_sentence=None):
    while True:
        host, _port, mountpoint, _user, _pass = get_current_ntrip_upstream()
        if not (host and mountpoint):
            print("Upstream NTRIP niet geconfigureerd; proxy wacht...", flush=True)
            time.sleep(NTRIP_RECONNECT_SEC)
            continue

        upstream_sock = None
        upstream_to_client_thread = None
        client_to_upstream_thread = None
        stop_event = threading.Event()

        try:
            print(
                f"Upstream NTRIP verbinden: {NTRIP_HOST}:{NTRIP_PORT}/{NTRIP_MOUNTPOINT}",
                flush=True,
            )
            upstream_sock, initial = connect_ntrip_socket(gga_sentence=gga_sentence)

            if initial:
                client_sock.sendall(initial)

            def upstream_worker():
                try:
                    bridge_upstream_to_client(client_sock, upstream_sock)
                except Exception as e:
                    print(f"Upstream->client gestopt: {e}", flush=True)
                finally:
                    stop_event.set()

            def client_worker():
                try:
                    bridge_client_to_upstream(client_sock, upstream_sock)
                except Exception as e:
                    print(f"Client->upstream gestopt: {e}", flush=True)
                finally:
                    stop_event.set()

            threading.Thread(target=upstream_worker, daemon=True).start()
            threading.Thread(target=client_worker, daemon=True).start()

            while not stop_event.is_set():
                time.sleep(0.2)

            return
        except OSError:
            print("NTRIP proxy: client verbinding verbroken", flush=True)
            return
        except Exception as e:
            print(f"Upstream NTRIP fout: {e}", flush=True)
        finally:
            if upstream_sock:
                try:
                    upstream_sock.close()
                except Exception:
                    pass

        time.sleep(NTRIP_RECONNECT_SEC)


def handle_ntrip_client(client_sock, addr):
    """Verwerk één inkomende NTRIP-clientverbinding."""
    try:
        raw = b""
        while b"\r\n\r\n" not in raw and len(raw) < 8192:
            chunk = client_sock.recv(1024)
            if not chunk:
                return
            raw += chunk

        request_str = raw.decode("latin1", errors="ignore")
        gga_sentence = extract_gga_from_request(raw)
        lines = request_str.split("\r\n")
        parts = lines[0].split()

        if not parts or parts[0] != "GET":
            client_sock.sendall(b"HTTP/1.0 400 Bad Request\r\n\r\n")
            return

        mountpoint = parts[1].lstrip("/") if len(parts) > 1 else ""

        username = ""
        password = ""
        for line in lines[1:]:
            if line.lower().startswith("authorization:"):
                auth_val = line.split(":", 1)[1].strip()
                if auth_val.startswith("Basic "):
                    try:
                        decoded = base64.b64decode(auth_val[6:]).decode("utf-8", errors="ignore")
                        username, _, password = decoded.partition(":")
                    except Exception:
                        pass
                break

        # Alleen afwijzen als de client wél credentials stuurt maar deze kloppen niet.
        # Als er geen credentials zijn gestuurd (username leeg), toestaan (lokaal netwerk).
        if username and (username != NTRIP_PROXY_USERNAME or password != NTRIP_PROXY_PASSWORD):
            client_sock.sendall(
                b"HTTP/1.0 401 Unauthorized\r\n"
                b"WWW-Authenticate: Basic realm=\"NTRIP\"\r\n\r\n"
            )
            print(f"NTRIP proxy: ongeldig login van {addr} (user={username})", flush=True)
            return

        if not mountpoint:
            sourcetable = (
                f"SOURCETABLE 200 OK\r\nContent-Type: text/plain\r\n\r\n"
                f"STR;{NTRIP_PROXY_MOUNTPOINT};VG710 RTCM Proxy;RTCM 3.2;;;1;1;NLD;;0.0;0.0;1;1;;;\r\n"
                f"ENDSOURCETABLE\r\n"
            )
            client_sock.sendall(sourcetable.encode("ascii"))
            return

        if mountpoint != NTRIP_PROXY_MOUNTPOINT:
            client_sock.sendall(b"HTTP/1.0 404 Not Found\r\n\r\n")
            print(f"NTRIP proxy: onbekend mountpoint '{mountpoint}' van {addr}", flush=True)
            return

        client_sock.sendall(b"ICY 200 OK\r\nContent-Type: gnss/data\r\n\r\n")
        print(
            f"NTRIP proxy: client {addr} verbonden op mountpoint '{mountpoint}', gga_present={bool(gga_sentence)}",
            flush=True,
        )
        proxy_rtcm_to_client(client_sock, gga_sentence=gga_sentence)

    except Exception as e:
        print(f"NTRIP proxy client fout ({addr}): {e}", flush=True)
    finally:
        try:
            client_sock.close()
        except Exception:
            pass


def ntrip_proxy_server():
    if not NTRIP_ENABLED:
        print("NTRIP uitgeschakeld; proxy-server niet gestart", flush=True)
        return

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((NTRIP_PROXY_HOST, NTRIP_PROXY_PORT))
    server_sock.listen(5)
    print(f"NTRIP proxy-server luistert op poort {NTRIP_PROXY_PORT}", flush=True)

    while True:
        try:
            client_sock, addr = server_sock.accept()
            threading.Thread(
                target=handle_ntrip_client,
                args=(client_sock, addr),
                daemon=True,
            ).start()
        except Exception as e:
            print(f"NTRIP proxy accept fout: {e}", flush=True)
            time.sleep(1)


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
                ts = now()
                if "GGA" in line or "GSA" in line or "GST" in line:
                    update_gnss_status(line, ts)
                append_nmea_record(
                    {
                        "device_id": DEVICE_ID,
                        "source": name,
                        "host": host,
                        "port": port,
                        "sentence": line,
                        "ts": ts,
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
    threading.Thread(target=ntrip_proxy_server, daemon=True).start()

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