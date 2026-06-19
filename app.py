
import base64
import collections
import json
import math
import os
import socket
import threading
import time
import uuid
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
AWS_CREDENTIALS_FILE = f"{BASE}/aws_credentials.json"

# Override AWS creds uit /data/vgapp/aws_credentials.json wanneer aanwezig.
# Dit gebeurt vóór elke boto3.client(...) aanroep zodat de UI-edits direct werken
# na een container-herstart, zonder dat docker-compose .env nodig is.
if os.path.exists(AWS_CREDENTIALS_FILE):
    try:
        with open(AWS_CREDENTIALS_FILE, encoding="utf-8") as f:
            _aws_creds = json.load(f)
        if isinstance(_aws_creds, dict):
            for env_key, json_key in (
                ("AWS_ACCESS_KEY_ID", "access_key_id"),
                ("AWS_SECRET_ACCESS_KEY", "secret_access_key"),
                ("AWS_DEFAULT_REGION", "region"),
            ):
                value = _aws_creds.get(json_key)
                if value:
                    os.environ[env_key] = str(value)
    except Exception as e:
        print(f"Error loading aws_credentials.json: {e}", flush=True)

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
S3_QUEUE_DIR = f"{BASE}/s3_queue"
S3_UPLOAD_INTERVAL_SEC = int(cfg.get("s3_upload_interval_sec", 5))
S3_QUEUE_MAX_BYTES = int(cfg.get("s3_queue_max_mb", 500)) * 1024 * 1024
# Minimale tijd tussen twee S3-records van dezelfde CAN-ID (downsampling).
# Default 1.0s = max 1 Hz per ID; tragere per-ID/groep-rates blijven gelden.
S3_CAN_MIN_INTERVAL_SEC = float(cfg.get("s3_can_min_interval_sec", 1.0))
# Idem voor NMEA, per zin-type (GNGGA, GNHDT, ...). Default 1.0s = 1 Hz per type.
S3_NMEA_MIN_INTERVAL_SEC = float(cfg.get("s3_nmea_min_interval_sec", 1.0))
# Interval tussen periodieke NMT Start-broadcasts (sensoren activeren). De aan/uit-
# toggle (nmt_autostart_enabled) wordt live uit config.json gelezen, zie helper.
NMT_AUTOSTART_INTERVAL_SEC = int(cfg.get("nmt_autostart_interval_sec", 30))

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

# Tweede (externe) GNSS-ontvanger, bv. Stonex S599, gekoppeld als NTRIP-client
# naar onze proxy. Wordt herkend op WiFi-IP; krijgt dezelfde RTCM-correcties.
DUAL_GNSS_STATUS_FILE = f"{BASE}/dual_gnss_status.json"


def get_dual_gnss_config():
    """Lees de dual-GNSS-koppeling live uit config.json (runtime-update zonder restart)."""
    try:
        with open(f"{BASE}/config.json", encoding="utf-8") as f:
            c = json.load(f)
        d = c.get("dual_gnss", {})
        return bool(d.get("enabled", False)), (d.get("wifi_ip", "") or "").strip()
    except Exception:
        d = cfg.get("dual_gnss", {})
        return bool(d.get("enabled", False)), (d.get("wifi_ip", "") or "").strip()


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

CAN_SENSOR_GROUPS = []
for _g in cfg.get("can_sensor_groups", []):
    try:
        _id_start = parse_can_id(_g["id_start"]) if _g.get("id_start") else None
        _id_end = parse_can_id(_g["id_end"]) if _g.get("id_end") else None
        CAN_SENSOR_GROUPS.append({
            "name": _g.get("name", ""),
            "id_start": _id_start,
            "id_end": _id_end,
            "upload_rate_sec": int(_g.get("upload_rate_sec", 10)),
        })
    except Exception:
        pass


def find_can_group(can_id):
    for group in CAN_SENSOR_GROUPS:
        if group["id_start"] is None or group["id_end"] is None:
            continue
        if group["id_start"] <= can_id <= group["id_end"]:
            return group
    return None

SEEN_IDS = set()
LATEST_MESSAGES = {}
LAST_PUBLISHED = {}
S3_CAN_DUE = {}
S3_NMEA_DUE = {}
# Sample-and-hold buffer voor S3: per CAN-ID de laatst ontvangen frame.
# Wordt op een vast rooster door can_s3_sampler_loop gecaptured en gereset.
S3_CAN_PENDING = {}
S3_CAN_PENDING_LOCK = threading.Lock()
# Idem voor NMEA: per zin-type (GNGGA, GNHDT, ...) de laatst ontvangen zin.
# Wordt op dezelfde tik als CAN gecaptured zodat S3-records tijd-uitgelijnd zijn.
S3_NMEA_PENDING = {}
S3_NMEA_PENDING_LOCK = threading.Lock()
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
LATEST_GGA = None   # meest recente ruwe GGA-zin (voor upstream NTRIP als Septentrio geen GGA meestuurt)
LATEST_GNSS_UTC = None  # UTC-tijd (hhmmss.ss) uit de laatste GGA, voor tijdkoppeling van CAN/NMEA
GNSS_STATUS = {
    "fix_quality": 0, "fix_label": "Geen fix",
    "lat": None, "lon": None, "satellites": None,
    "hdop": None, "vdop": None, "pdop": None,
    "altitude": None,
    "acc_lat": None, "acc_lon": None, "acc_alt": None,
    "ts": None,
}

# Positie B = de externe ontvanger (S599); gevuld uit de GGA die hij als
# NTRIP-client over de proxy stuurt.
POSITION_B_LOCK = threading.Lock()
POSITION_B = {
    "connected": False, "fix_quality": 0, "fix_label": "Geen fix",
    "lat": None, "lon": None, "altitude": None, "ts": None,
}

# Lokale NTRIP-clients die RTCM-correcties van de gedeelde upstream ontvangen.
PROXY_CLIENTS_LOCK = threading.Lock()
PROXY_CLIENTS = set()


def now():
    return datetime.now(timezone.utc).isoformat()


def _write_atomic(path, text):
    """Schrijf naar een temp-bestand en hernoem, zodat lezers nooit een halve write zien."""
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)


def _rate_gate(store, key, interval, mono, tol_frac=0.15):
    """Laat per 'key' ~1/interval samples door op een vast rooster (geen drift).

    Een kleine tolerantie laat frames toe die net vóór de deadline aankomen,
    zodat een ~2 Hz-bron met jitter niet onder 1 Hz zakt. Na een gap wordt het
    rooster opnieuw op 'mono' gebaseerd om bursts te voorkomen.
    """
    due = store.get(key)
    if due is None:
        store[key] = mono + interval
        return True
    if mono >= due - interval * tol_frac:
        nxt = due + interval
        if nxt <= mono:
            nxt = mono + interval
        store[key] = nxt
        return True
    return False


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
        _write_atomic(CAN_IDS_FILE, json.dumps(formatted, indent=2))
    except Exception as e:
        print(f"Error saving CAN IDs: {e}", flush=True)


def save_aws_status(connected, message):
    try:
        payload = {
            "connected": connected,
            "message": message,
            "last_update": now(),
        }
        _write_atomic(AWS_STATUS_FILE, json.dumps(payload, indent=2))
    except Exception as e:
        print(f"Error saving AWS status: {e}", flush=True)


def save_s3_status():
    try:
        with S3_STATS_LOCK:
            payload = {
                "can": dict(S3_CAN_STATS),
                "nmea": dict(S3_NMEA_STATS),
            }
        _write_atomic(S3_STATUS_FILE, json.dumps(payload, indent=2))
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
        utc = parts[1] or None
        if fix_quality == 0:
            return {"fix_quality": 0, "fix_label": fix_label, "utc": utc,
                    "lat": None, "lon": None, "satellites": None,
                    "hdop": None, "altitude": None}
        return {
            "fix_quality": fix_quality,
            "fix_label": fix_label,
            "utc": utc,
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
    global LATEST_GGA, LATEST_GNSS_UTC
    parsed = None
    if "GGA" in sentence:
        parsed = parse_gga(sentence)
        if parsed:
            if parsed.get("utc"):
                with GNSS_LOCK:
                    LATEST_GNSS_UTC = parsed["utc"]
            if parsed.get("fix_quality", 0) > 0:
                with GNSS_LOCK:
                    LATEST_GGA = sentence.strip()
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
        _write_atomic(GNSS_STATUS_FILE, json.dumps(payload, indent=2))
    except Exception as e:
        print(f"Error saving GNSS status: {e}", flush=True)


def ensure_queue_dirs():
    os.makedirs(os.path.join(S3_QUEUE_DIR, "can"), exist_ok=True)
    os.makedirs(os.path.join(S3_QUEUE_DIR, "nmea"), exist_ok=True)


def write_batch_to_queue(batch, data_type):
    """Schrijf een batch atomisch naar de schijfwachtrij. Geeft True bij succes."""
    if not batch:
        return True
    ts = batch[0].get("ts", now()).replace(":", "-")
    uid = uuid.uuid4().hex[:8]
    path = os.path.join(S3_QUEUE_DIR, data_type, f"{ts}_{uid}.ndjson")
    tmp_path = path + ".tmp"
    try:
        body = "\n".join(json.dumps(item, separators=(",", ":")) for item in batch) + "\n"
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(body)
        os.replace(tmp_path, path)
        return True
    except Exception as e:
        print(f"Queue schrijffout ({data_type}): {e}", flush=True)
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        return False


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


def write_device_startup_record():
    """Schrijf bij elke opstart een apparaatregel naar een vaste S3-key
    (devices/{device_id}.njson). De vorige opstartregel wordt overschreven,
    zodat de bucket een actuele device-registry bevat."""
    if not S3_CLIENT or not S3_BUCKET:
        return
    key = f"{S3_PREFIX}/devices/{DEVICE_ID}.njson"
    record = {
        "device_id": DEVICE_ID,
        "asset_id": cfg.get("asset_id"),
        "app_version": os.environ.get("APP_VERSION", "onbekend"),
        "ts": now(),
    }
    body = json.dumps(record, separators=(",", ":")) + "\n"
    try:
        S3_CLIENT.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/x-ndjson",
        )
        print(f"Wrote device startup record to s3://{S3_BUCKET}/{key}", flush=True)
    except Exception as e:
        print(f"Device startup record failed: {e}", flush=True)


def enforce_queue_limit():
    """Verwijder de oudste wachtrijbestanden totdat het totaal onder S3_QUEUE_MAX_BYTES valt."""
    entries = []
    for data_type in ("can", "nmea"):
        queue_dir = os.path.join(S3_QUEUE_DIR, data_type)
        try:
            for filename in os.listdir(queue_dir):
                if not filename.endswith(".ndjson") or filename.endswith(".tmp"):
                    continue
                path = os.path.join(queue_dir, filename)
                try:
                    size = os.path.getsize(path)
                    entries.append((filename, path, size))
                except Exception:
                    pass
        except Exception:
            pass

    total = sum(e[2] for e in entries)
    if total <= S3_QUEUE_MAX_BYTES:
        return

    # Oudste bestanden eerst verwijderen (bestandsnaam begint met timestamp)
    entries.sort(key=lambda e: e[0])
    for filename, path, size in entries:
        if total <= S3_QUEUE_MAX_BYTES:
            break
        try:
            os.remove(path)
            total -= size
            print(f"Queue limiet: oudste bestand verwijderd: {path}", flush=True)
        except Exception as e:
            print(f"Queue limiet: fout bij verwijderen {path}: {e}", flush=True)


def s3_upload_worker():
    """Upload bestanden uit de schijfwachtrij naar S3; verwijder pas na succes."""
    if not S3_CLIENT or not S3_BUCKET:
        return
    while True:
        enforce_queue_limit()
        for data_type in ("can", "nmea"):
            queue_dir = os.path.join(S3_QUEUE_DIR, data_type)
            try:
                filenames = sorted(
                    f for f in os.listdir(queue_dir)
                    if f.endswith(".ndjson") and not f.endswith(".tmp")
                )
            except Exception:
                continue

            for filename in filenames:
                path = os.path.join(queue_dir, filename)
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        content = f.read()
                    batch = [
                        json.loads(line)
                        for line in content.strip().splitlines()
                        if line
                    ]
                    if not batch:
                        os.remove(path)
                        continue

                    first_ts = batch[0].get("ts", now())
                    ts_part = first_ts.replace(":", "-")
                    date_part = first_ts[:10]
                    hour_part = first_ts[11:13] if len(first_ts) >= 13 else "00"
                    key = f"{S3_PREFIX}/{DEVICE_ID}/{data_type}/{date_part}/{hour_part}/{ts_part}.ndjson"

                    upload_batch_to_s3(key, batch)
                    os.remove(path)

                    stats = S3_CAN_STATS if data_type == "can" else S3_NMEA_STATS
                    with S3_STATS_LOCK:
                        stats["total_records"] += len(batch)
                        stats["total_uploads"] += 1
                        stats["last_key"] = key
                        stats["last_upload"] = now()
                    save_s3_status()
                    save_aws_status(True, f"s3 upload ok: {key}")

                except Exception as e:
                    save_aws_status(False, f"s3 upload error: {e}")
                    print(f"S3 upload fout voor {filename}: {e}", flush=True)

        time.sleep(S3_UPLOAD_INTERVAL_SEC)


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

    if not write_batch_to_queue(batch, "can"):
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

    if not write_batch_to_queue(batch, "nmea"):
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


def _is_can_command(can_id):
    """True voor busmanagement-frames (geen sensordata): NMT-broadcasts (0x000,
    o.a. de sensor-activatie) en SDO-requests (0x600-0x67F, node-ID/baudrate).
    Deze worden uit S3 gehouden; in de live-log blijven ze zichtbaar."""
    return can_id == 0x000 or 0x600 <= can_id <= 0x67F


def _nmt_autostart_enabled():
    """Lees de NMT-autostart-toggle vers uit config.json zodat de webinterface
    deze live kan aan/uitzetten zonder container-herstart. Default True."""
    try:
        with open(f"{BASE}/config.json", encoding="utf-8") as f:
            return bool(json.load(f).get("nmt_autostart_enabled", True))
    except Exception:
        return True


def can_reader_loop():
    nmt_start = can.Message(arbitration_id=0x000, data=[0x01, 0x00], is_extended_id=False)
    while True:
        bus = None
        try:
            print(f"Opening CAN channel={CAN_CHANNEL}", flush=True)
            bus = can.interface.Bus(channel=CAN_CHANNEL, interface="socketcan")
            print("CAN opened", flush=True)

            # Eenmalige NMT Start bij bus-bring-up zodat sensoren bij aanschakelen
            # direct data sturen.
            try:
                bus.send(nmt_start)
                print("Sent NMT Start broadcast", flush=True)
            except Exception as e:
                print(f"NMT broadcast failed: {e}", flush=True)
            last_nmt = time.time()

            while True:
                msg = bus.recv(timeout=1.0)

                # Periodieke NMT Start; aan/uit via config (nmt_autostart_enabled,
                # default True). Vers ingelezen zodat de webinterface-toggle direct
                # werkt zonder container-herstart.
                if time.time() - last_nmt > NMT_AUTOSTART_INTERVAL_SEC:
                    if _nmt_autostart_enabled():
                        try:
                            bus.send(nmt_start)
                        except Exception as e:
                            print(f"Periodic NMT failed: {e}", flush=True)
                    last_nmt = time.time()

                if msg is None:
                    continue

                ts = now()
                _group = find_can_group(msg.arbitration_id)
                with GNSS_LOCK:
                    gnss_utc = LATEST_GNSS_UTC
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
                    "gnss_utc": gnss_utc,
                    "group_name": _group["name"] if _group else None,
                    "rate_limit_sec": CAN_RATE_MAP.get(msg.arbitration_id)
                        or (_group["upload_rate_sec"] if _group else None),
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

                # Sample-and-hold: laatste frame per CAN-ID onthouden. De vaste
                # 1 Hz-tik in can_s3_sampler_loop captured en reset deze buffer.
                # Busmanagement-commando's (NMT/SDO) niet naar S3.
                if not _is_can_command(msg.arbitration_id):
                    with S3_CAN_PENDING_LOCK:
                        S3_CAN_PENDING[msg.arbitration_id] = payload

        except Exception as e:
            print(f"CAN reader error on {CAN_CHANNEL}: {e}", flush=True)
        finally:
            if bus is not None:
                try:
                    bus.shutdown()
                except Exception:
                    pass

        time.sleep(5)


def can_publisher_loop():
    while True:
        now_ts = time.time()

        with LOCK:
            messages_snapshot = dict(LATEST_MESSAGES)

        for can_id, payload in messages_snapshot.items():
            interval_sec = CAN_RATE_MAP.get(can_id)
            if interval_sec is None:
                group = find_can_group(can_id)
                if group:
                    interval_sec = group["upload_rate_sec"]
            if interval_sec is None:
                continue

            last_ts = LAST_PUBLISHED.get(can_id, 0)
            if now_ts - last_ts < interval_sec:
                continue

            publish(CAN_TOPIC, payload, qos=0)
            LAST_PUBLISHED[can_id] = now_ts

        time.sleep(0.1)


def can_s3_sampler_loop():
    """Captured per CAN-ID en per NMEA-zin-type de laatste waarde op een vast rooster.

    Op elke tik (default 1 Hz) wordt per ID/zin-type de meest recente waarde uit
    S3_CAN_PENDING / S3_NMEA_PENDING gepakt, naar S3 geschreven en de buffer
    gereset, zodat ongewijzigde data niet opnieuw wordt verstuurd. CAN en NMEA
    delen dezelfde tik en zijn daardoor tijd-uitgelijnd. Een tragere per-ID/groep-
    rate krijgt voorrang via S3_CAN_DUE; tussen tikken wint de laatste waarde.
    """
    next_tick = time.monotonic()
    while True:
        next_tick += S3_CAN_MIN_INTERVAL_SEC
        sleep_for = next_tick - time.monotonic()
        if sleep_for > 0:
            time.sleep(sleep_for)
        else:
            # Achtergelopen (lange gap/load): rooster opnieuw verankeren.
            next_tick = time.monotonic()

        mono = time.monotonic()
        with S3_CAN_PENDING_LOCK:
            pending_ids = list(S3_CAN_PENDING.keys())

        for can_id in pending_ids:
            # Bepaal interval voor deze ID: default 1 Hz, trager indien geconfigureerd.
            s3_interval = S3_CAN_MIN_INTERVAL_SEC
            cfg_rate = CAN_RATE_MAP.get(can_id)
            if cfg_rate is None:
                group = find_can_group(can_id)
                if group:
                    cfg_rate = group.get("upload_rate_sec")
            if cfg_rate is not None and cfg_rate > s3_interval:
                s3_interval = cfg_rate

            # Tragere ID's alleen op hun eigen due-moment doorlaten; latere tikken
            # laten de pending-waarde staan zodat hij blijft verversen.
            if s3_interval > S3_CAN_MIN_INTERVAL_SEC:
                if not _rate_gate(S3_CAN_DUE, can_id, s3_interval, mono):
                    continue

            with S3_CAN_PENDING_LOCK:
                payload = S3_CAN_PENDING.pop(can_id, None)
            if payload is not None:
                append_s3_record(payload)

        # NMEA op dezelfde tik: per zin-type de laatste zin capturen + resetten.
        with S3_NMEA_PENDING_LOCK:
            nmea_records = list(S3_NMEA_PENDING.values())
            S3_NMEA_PENDING.clear()
        for record in nmea_records:
            append_nmea_record(record)


def save_can_latest():
    try:
        with LOCK:
            snapshot = list(LATEST_MESSAGES.values())
        snapshot.sort(key=lambda m: m.get("id", 0))
        with CAN_LOG_LOCK:
            log_snapshot = list(CAN_LOG)
        _write_atomic(CAN_LATEST_FILE, json.dumps({"latest": snapshot, "log": log_snapshot}, separators=(",", ":")))
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


def register_proxy_client(sock):
    with PROXY_CLIENTS_LOCK:
        PROXY_CLIENTS.add(sock)


def unregister_proxy_client(sock):
    with PROXY_CLIENTS_LOCK:
        PROXY_CLIENTS.discard(sock)


def proxy_client_count():
    with PROXY_CLIENTS_LOCK:
        return len(PROXY_CLIENTS)


def broadcast_rtcm(data):
    """Stuur RTCM naar alle verbonden lokale clients; ruim dode verbindingen op."""
    with PROXY_CLIENTS_LOCK:
        clients = list(PROXY_CLIENTS)
    for sock in clients:
        try:
            sock.sendall(data)
        except Exception:
            unregister_proxy_client(sock)
            try:
                sock.close()
            except Exception:
                pass


def _local_offset(lat_a, lon_a, alt_a, lat_b, lon_b, alt_b):
    """Verschil B - A in meters via lokaal tangentvlak (nauwkeurig op korte afstand)."""
    R = 6378137.0  # WGS84 grote halve as
    lat_mid = math.radians((lat_a + lat_b) / 2.0)
    d_north = math.radians(lat_b - lat_a) * R
    d_east = math.radians(lon_b - lon_a) * R * math.cos(lat_mid)
    d_up = (alt_b - alt_a) if (alt_a is not None and alt_b is not None) else None
    return d_north, d_east, d_up


def write_dual_gnss_status():
    """Bereken het live verschil tussen ontvanger A (Septentrio) en B (S599) en schrijf naar schijf."""
    with GNSS_LOCK:
        a = dict(GNSS_STATUS)
    with POSITION_B_LOCK:
        b = dict(POSITION_B)
    payload = {"a": a, "b": b, "diff": None, "ts": now()}
    try:
        if a.get("lat") is not None and b.get("lat") is not None:
            d_north, d_east, d_up = _local_offset(
                a["lat"], a["lon"], a.get("altitude"),
                b["lat"], b["lon"], b.get("altitude"),
            )
            dist_2d = math.sqrt(d_north * d_north + d_east * d_east)
            dist_3d = (
                math.sqrt(d_north * d_north + d_east * d_east + d_up * d_up)
                if d_up is not None else None
            )
            payload["diff"] = {
                "d_north": round(d_north, 4),
                "d_east": round(d_east, 4),
                "d_up": round(d_up, 4) if d_up is not None else None,
                "dist_2d": round(dist_2d, 4),
                "dist_3d": round(dist_3d, 4) if dist_3d is not None else None,
            }
    except Exception as e:
        print(f"Dual GNSS diff fout: {e}", flush=True)
    try:
        _write_atomic(DUAL_GNSS_STATUS_FILE, json.dumps(payload, indent=2))
    except Exception as e:
        print(f"Error saving dual GNSS status: {e}", flush=True)


def update_position_b(raw):
    """Parse de GGA van de externe ontvanger (S599) en werk positie B bij."""
    try:
        text = raw.decode("latin1", errors="ignore")
    except Exception:
        return
    for line in text.replace("\r", "\n").split("\n"):
        line = line.strip()
        if not line or "GGA" not in line or not line.startswith("$"):
            continue
        parsed = parse_gga(line)
        if not parsed:
            continue
        with POSITION_B_LOCK:
            POSITION_B.update({
                "connected": True,
                "fix_quality": parsed.get("fix_quality", 0),
                "fix_label": parsed.get("fix_label"),
                "lat": parsed.get("lat"),
                "lon": parsed.get("lon"),
                "altitude": parsed.get("altitude"),
                "ts": now(),
            })
        write_dual_gnss_status()


def ntrip_upstream_manager():
    """Eén gedeelde upstream-verbinding met de caster; RTCM wordt naar alle clients verdeeld.

    GGA naar de caster komt uitsluitend van de primaire ontvanger (LATEST_GGA van de
    Septentrio); de GGA van de externe ontvanger gaat NIET naar de caster.
    """
    while True:
        if proxy_client_count() == 0:
            time.sleep(1)
            continue

        host, _port, mountpoint, _user, _pass = get_current_ntrip_upstream()
        if not (host and mountpoint):
            print("Upstream NTRIP niet geconfigureerd; wacht...", flush=True)
            time.sleep(NTRIP_RECONNECT_SEC)
            continue

        upstream_sock = None
        stop_event = threading.Event()
        try:
            with GNSS_LOCK:
                effective_gga = LATEST_GGA
            print(
                f"Upstream NTRIP verbinden: {host}:{_port}/{mountpoint} gga={bool(effective_gga)}",
                flush=True,
            )
            upstream_sock, initial = connect_ntrip_socket(gga_sentence=effective_gga)
            upstream_sock.settimeout(60)
            if initial:
                broadcast_rtcm(initial)

            def gga_worker():
                """Stuur elke 10 s de primaire GGA naar de caster (VRS-vereiste)."""
                while not stop_event.is_set():
                    stop_event.wait(10)
                    if stop_event.is_set():
                        break
                    with GNSS_LOCK:
                        gga = LATEST_GGA
                    if gga:
                        try:
                            upstream_sock.sendall((gga + "\r\n").encode("ascii", errors="ignore"))
                        except Exception:
                            break

            threading.Thread(target=gga_worker, daemon=True).start()

            while True:
                data = upstream_sock.recv(4096)
                if not data:
                    raise RuntimeError("Upstream NTRIP stream gesloten")
                broadcast_rtcm(data)
                if proxy_client_count() == 0:
                    print("Geen NTRIP-clients meer; upstream sluiten", flush=True)
                    break
        except socket.timeout:
            print("Upstream NTRIP timeout; opnieuw verbinden", flush=True)
        except Exception as e:
            print(f"Upstream NTRIP fout: {e}", flush=True)
        finally:
            stop_event.set()
            if upstream_sock:
                try:
                    upstream_sock.close()
                except Exception:
                    pass

        time.sleep(NTRIP_RECONNECT_SEC)


def handle_ntrip_client(client_sock, addr):
    """Verwerk één inkomende NTRIP-clientverbinding."""
    is_secondary = False
    try:
        raw = b""
        while b"\r\n\r\n" not in raw and len(raw) < 8192:
            chunk = client_sock.recv(1024)
            if not chunk:
                return
            raw += chunk

        request_str = raw.decode("latin1", errors="ignore")
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

        enabled, s599_ip = get_dual_gnss_config()
        is_secondary = bool(enabled and s599_ip and addr[0] == s599_ip)
        print(
            f"NTRIP proxy: client {addr} verbonden op '{mountpoint}' (secondary={is_secondary})",
            flush=True,
        )

        register_proxy_client(client_sock)
        if is_secondary:
            with POSITION_B_LOCK:
                POSITION_B["connected"] = True
            write_dual_gnss_status()

        # Lees inkomende data (GGA) van de client. Die gaat NIET naar de caster;
        # voor de externe ontvanger gebruiken we de GGA wel lokaal voor positie B.
        while True:
            data = client_sock.recv(1024)
            if not data:
                break
            if is_secondary and b"GGA" in data:
                update_position_b(data)

    except Exception as e:
        print(f"NTRIP proxy client fout ({addr}): {e}", flush=True)
    finally:
        unregister_proxy_client(client_sock)
        if is_secondary:
            with POSITION_B_LOCK:
                POSITION_B["connected"] = False
            write_dual_gnss_status()
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
                # Sample-and-hold: per zin-type de laatste zin onthouden. De S3-
                # capture gebeurt op dezelfde 1 Hz-tik als CAN (zie nmea-blok in
                # can_s3_sampler_loop), zodat NMEA en CAN tijd-uitgelijnd zijn.
                # Live-status/NTRIP blijven elke zin verwerken.
                ntype = line.split(",", 1)[0].lstrip("$") or "?"
                with GNSS_LOCK:
                    gnss_utc = LATEST_GNSS_UTC
                with S3_NMEA_PENDING_LOCK:
                    S3_NMEA_PENDING[ntype] = {
                        "device_id": DEVICE_ID,
                        "source": name,
                        "host": host,
                        "port": port,
                        "sentence": line,
                        "ts": ts,
                        "gnss_utc": gnss_utc,
                    }
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


ensure_queue_dirs()

# Apparaatregel bij opstart naar S3 (overschrijft de vorige). In een thread
# zodat een trage/ontbrekende netwerkverbinding de opstart niet blokkeert.
threading.Thread(target=write_device_startup_record, daemon=True).start()

threading.Thread(target=heartbeat, daemon=True).start()
threading.Thread(target=can_reader_loop, daemon=True).start()
threading.Thread(target=can_publisher_loop, daemon=True).start()
threading.Thread(target=can_s3_sampler_loop, daemon=True).start()
threading.Thread(target=s3_flush_loop, daemon=True).start()
threading.Thread(target=s3_upload_worker, daemon=True).start()

if NTRIP_ENABLED:
    threading.Thread(target=ntrip_proxy_server, daemon=True).start()
    threading.Thread(target=ntrip_upstream_manager, daemon=True).start()

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