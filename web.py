import base64
import json
import os
import socket
import subprocess
import tempfile
import threading
import time
import zipfile

from flask import Flask, redirect, render_template_string, request, send_file, url_for
from werkzeug.utils import secure_filename

app = Flask(__name__)

BASE_DIR = "/data/vgapp"
CERT_DIR = os.path.join(BASE_DIR, "certs")
os.makedirs(BASE_DIR, exist_ok=True)
os.makedirs(CERT_DIR, exist_ok=True)

GITHUB_REPO = "AdriaanBakker74/vg710-aws-uploader"
RELEASE_TAR_URL = f"https://github.com/{GITHUB_REPO}/releases/latest/download/vg710-web-aws.tar"
UPDATE_TAR_PATH = "/tmp/vg710-web-aws-update.tar"

_update_status = {"running": False, "log": [], "done": False, "success": None}
_update_lock = threading.Lock()

_gh_version_cache = {"tag_name": None}
_gh_version_lock = threading.Lock()


def _gh_version_fetch_loop():
    import urllib.request
    while True:
        try:
            url = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
            req = urllib.request.Request(url, headers={"User-Agent": "vg710-updater"})
            with urllib.request.urlopen(req, timeout=8) as resp:
                data = json.loads(resp.read().decode())
            tag = data.get("tag_name") or "onbekend"
        except Exception:
            tag = None
        with _gh_version_lock:
            if tag:
                _gh_version_cache["tag_name"] = tag
        time.sleep(300)


threading.Thread(target=_gh_version_fetch_loop, daemon=True).start()


def _run_update():
    import urllib.request

    def log(msg):
        with _update_lock:
            _update_status["log"].append(msg)

    def run(cmd):
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.stdout.strip():
            log(result.stdout.strip())
        if result.stderr.strip():
            log(result.stderr.strip())
        return result.returncode

    try:
        log(f"Downloaden van {RELEASE_TAR_URL} ...")
        urllib.request.urlretrieve(RELEASE_TAR_URL, UPDATE_TAR_PATH)
        size_mb = round(os.path.getsize(UPDATE_TAR_PATH) / 1024 / 1024, 1)
        log(f"Download geslaagd ({size_mb} MB).")

        log("Docker image laden...")
        rc = run(["docker", "load", "-i", UPDATE_TAR_PATH])
        if rc != 0:
            log(f"docker load mislukt (exit {rc}).")
            log("Controleer of /var/run/docker.sock gemount is in de container.")
            with _update_lock:
                _update_status.update({"running": False, "done": True, "success": False})
            return

        container_id = socket.gethostname()
        log(f"Container herstarten ({container_id})...")
        run(["docker", "restart", container_id])

        with _update_lock:
            _update_status.update({"running": False, "done": True, "success": True})
    except Exception as e:
        with _update_lock:
            _update_status["log"].append(f"Fout: {e}")
            _update_status.update({"running": False, "done": True, "success": False})

# --- Systeem statistieken (CPU + geheugen) ---
_sys_stats = {"cpu_percent": None, "mem_used_mb": None, "mem_total_mb": None,
              "mem_percent": None, "load_1": None, "load_5": None}
_sys_stats_lock = threading.Lock()


def _sys_stats_loop():
    def read_cpu_stat():
        with open("/proc/stat") as f:
            parts = f.readline().split()[1:]
        vals = list(map(int, parts))
        return vals[3], sum(vals)

    while True:
        try:
            idle1, total1 = read_cpu_stat()
            time.sleep(1)
            idle2, total2 = read_cpu_stat()
            dt = total2 - total1
            cpu = round((1 - (idle2 - idle1) / dt) * 100) if dt else 0

            mem = {}
            with open("/proc/meminfo") as f:
                for line in f:
                    p = line.split()
                    if len(p) >= 2:
                        mem[p[0].rstrip(":")] = int(p[1])
            total_kb = mem.get("MemTotal", 0)
            used_kb = total_kb - mem.get("MemAvailable", 0)

            with open("/proc/loadavg") as f:
                load = f.read().split()

            with _sys_stats_lock:
                _sys_stats.update({
                    "cpu_percent": cpu,
                    "mem_used_mb": round(used_kb / 1024),
                    "mem_total_mb": round(total_kb / 1024),
                    "mem_percent": round(used_kb / total_kb * 100) if total_kb else 0,
                    "load_1": float(load[0]),
                    "load_5": float(load[1]),
                })
        except Exception:
            pass
        time.sleep(4)


threading.Thread(target=_sys_stats_loop, daemon=True).start()


def system_stats():
    with _sys_stats_lock:
        return dict(_sys_stats)

HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>VG710 Control Panel</title>
  <meta http-equiv="refresh" content="30">
  <style>
    :root {
      --bg: #f4f7fb;
      --card: #ffffff;
      --text: #1d2733;
      --muted: #6b7785;
      --line: #dbe3ec;
      --accent: #0b69ff;
      --accent-2: #084ec0;
      --ok-bg: #edf9f1;
      --ok-text: #127a3d;
      --bad-bg: #fff1f1;
      --bad-text: #b42318;
      --warn-bg: #fff7e6;
      --warn-text: #9a6700;
      --shadow: 0 8px 24px rgba(16, 24, 40, 0.08);
      --radius: 16px;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      background: var(--bg);
      color: var(--text);
    }
    .page {
      max-width: 1320px;
      margin: 0 auto;
      padding: 24px;
    }
    .hero {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 16px;
      margin-bottom: 24px;
      padding: 24px;
      border-radius: var(--radius);
      background: linear-gradient(135deg, #0b69ff 0%, #084ec0 100%);
      color: #fff;
      box-shadow: var(--shadow);
    }
    .hero h1 {
      margin: 0 0 6px 0;
      font-size: 28px;
      line-height: 1.2;
    }
    .hero p {
      margin: 0;
      color: rgba(255, 255, 255, 0.88);
    }
    .hero-actions {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
    }
    .button,
    button,
    input[type="submit"] {
      appearance: none;
      border: 0;
      border-radius: 12px;
      background: var(--accent);
      color: #fff;
      padding: 10px 16px;
      font-weight: 600;
      cursor: pointer;
      text-decoration: none;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 42px;
    }
    .button.secondary,
    button.secondary,
    input[type="submit"].secondary {
      background: #e7eef9;
      color: var(--accent-2);
    }
    .button:hover,
    button:hover,
    input[type="submit"]:hover {
      filter: brightness(0.97);
    }
    .grid {
      display: grid;
      gap: 20px;
    }
    .grid.top {
      grid-template-columns: 1.3fr 1fr;
      margin-bottom: 20px;
    }
    .grid.bottom {
      grid-template-columns: 1.1fr 0.9fr;
      margin-top: 20px;
    }
    .card {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      padding: 20px;
    }
    .card h2 {
      margin: 0 0 8px 0;
      font-size: 20px;
    }
    .card .sub {
      margin: 0 0 18px 0;
      color: var(--muted);
      font-size: 14px;
    }
    .status-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }
    .status-item {
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
      background: #fbfdff;
    }
    .status-item strong {
      display: block;
      margin-bottom: 6px;
      font-size: 14px;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      padding: 6px 10px;
      border-radius: 999px;
      font-size: 13px;
      font-weight: 700;
    }
    .pill.ok {
      background: var(--ok-bg);
      color: var(--ok-text);
    }
    .pill.bad {
      background: var(--bad-bg);
      color: var(--bad-text);
    }
    .aws-box {
      margin-top: 16px;
      padding: 14px 16px;
      border-radius: 14px;
      background: #f8fbff;
      border: 1px solid var(--line);
    }
    .upload-grid {
      display: grid;
      grid-template-columns: 1fr 1fr 1fr;
      gap: 14px;
      margin-top: 16px;
    }
    .upload-card {
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
      background: #fbfdff;
    }
    .upload-card h3 {
      margin: 0 0 10px 0;
      font-size: 16px;
    }
    .upload-card p {
      margin: 0 0 12px 0;
      color: var(--muted);
      font-size: 13px;
    }
    .upload-card form {
      display: grid;
      gap: 10px;
    }
    input[type="file"],
    input[type="text"],
    input[type="number"],
    textarea {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px 12px;
      font: inherit;
      background: #fff;
      color: var(--text);
    }
    table {
      width: 100%;
      border-collapse: collapse;
      overflow: hidden;
      border-radius: 14px;
      border: 1px solid var(--line);
    }
    th, td {
      padding: 10px 12px;
      text-align: left;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
    }
    th {
      background: #f7faff;
      font-size: 14px;
    }
    tr:last-child td {
      border-bottom: 0;
    }
    .can-list {
      list-style: none;
      padding: 0;
      margin: 0;
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
    }
    .can-list li {
      padding: 10px 12px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: #fbfdff;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 13px;
    }
    .shell-form {
      display: grid;
      gap: 12px;
    }
    .shell-buttons {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }
    pre {
      margin: 0;
      border-radius: 14px;
      background: #0f1720;
      color: #d7e3f4;
      padding: 16px;
      overflow: auto;
      min-height: 260px;
      font-size: 13px;
      line-height: 1.45;
      white-space: pre-wrap;
      word-break: break-word;
    }
    .note {
      margin-top: 12px;
      padding: 12px 14px;
      border-radius: 12px;
      background: var(--warn-bg);
      color: var(--warn-text);
      font-size: 13px;
    }
    .muted {
      color: var(--muted);
      font-size: 13px;
    }
    @media (max-width: 1080px) {
      .grid.top,
      .grid.bottom,
      .upload-grid,
      .status-grid,
      .can-list {
        grid-template-columns: 1fr;
      }
      .hero {
        flex-direction: column;
        align-items: flex-start;
      }
    }
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <div>
        <h1>VG710 Control Panel</h1>
        <h2 style="margin: 4px 0 8px 0; font-size: 18px; font-weight: 400; color: rgba(255,255,255,0.85);">
        </h2>
        <p>Configuratie, certificaten, CAN upload rates, AWS-status en container shell in één overzicht.</p>
        {% if device_id %}
        <p style="margin-top: 10px; font-size: 13px; color: rgba(255,255,255,0.75);">
          Device ID: <strong style="color:#fff;">{{ device_id }}</strong>
          {% if asset_id %}&nbsp;&nbsp;·&nbsp;&nbsp;Asset ID: <strong style="color:#fff;">{{ asset_id }}</strong>{% endif %}
          &nbsp;&nbsp;·&nbsp;&nbsp;Versie: <strong style="color:#fff;">{{ app_version }}</strong>
        </p>
        {% endif %}
      </div>
      <div class="hero-actions">
        <a class="button secondary" href="/download_config">Download Config + S3 + Certs</a>
      </div>
    </section>

    <div class="grid top">
      <section class="card">
        <h2>Status & uploads</h2>
        <p class="sub">Upload configuratie en certificaten direct naast de actuele status van de containerbestanden.</p>

        <div class="status-grid">
          <div class="status-item">
            <strong>config.json</strong>
            <span class="pill {{ 'ok' if config == '✅' else 'bad' }}">{{ config }}</span>
          </div>
          <div class="status-item">
            <strong>s3.json</strong>
            <span class="pill {{ 'ok' if s3cfg == '✅' else 'bad' }}">{{ s3cfg }}</span>
          </div>
          <div class="status-item">
            <strong>device.pem.crt</strong>
            <span class="pill {{ 'ok' if crt == '✅' else 'bad' }}">{{ crt }}</span>
          </div>
          <div class="status-item">
            <strong>private.pem.key</strong>
            <span class="pill {{ 'ok' if key == '✅' else 'bad' }}">{{ key }}</span>
          </div>
          <div class="status-item">
            <strong>AmazonRootCA1.pem</strong>
            <span class="pill {{ 'ok' if ca == '✅' else 'bad' }}">{{ ca }}</span>
          </div>
        </div>

        <div id="aws-status" class="aws-box">
          <strong>AWS status</strong>
          <div class="muted">{{ aws_status_text }}</div>
        </div>

        <div class="upload-grid">
          <div class="upload-card">
            <h3>Upload config.json</h3>
            <p>Algemene apparaatconfiguratie zoals device ID, MQTT endpoint en CAN kanaal.</p>
            <form method="post" enctype="multipart/form-data" action="/upload_config">
              <input type="file" name="file">
              <input type="submit" value="Upload config.json">
            </form>
          </div>
          <div class="upload-card">
            <h3>Upload s3.json</h3>
            <p>S3 bucket-, prefix- en flush-instellingen voor opslag van ruwe CAN-data.</p>
            <form method="post" enctype="multipart/form-data" action="/upload_config">
              <input type="file" name="file">
              <input type="submit" value="Upload s3.json">
            </form>
          </div>
          <div class="upload-card">
            <h3>Upload certificaten</h3>
            <p>Gebruik AmazonRootCA1.pem, een bestand eindigend op <code>-certificate.pem.crt</code> of <code>-private.pem.key</code>.</p>
            <form method="post" enctype="multipart/form-data" action="/upload_cert">
              <input type="file" name="file">
              <input type="submit" value="Upload certificaat">
            </form>
          </div>
        </div>
      </section>

      <section class="card">
        <h2>Detected CAN IDs</h2>
        <p class="sub">Automatisch gevonden CAN IDs op basis van ontvangen berichten.</p>
        <ul class="can-list">
        {% for cid in can_ids %}
          <li>
            <strong>{{ cid.id_hex if cid.id_hex is defined else cid.id }}</strong>
            {% if cid.group_name %}
              <span class="muted" style="font-size:11px;display:block;">{{ cid.group_name }}</span>
            {% endif %}
            {% if cid.rate_limit_sec is defined and cid.rate_limit_sec %}
              <span class="muted" style="font-size:11px;display:block;">{{ cid.rate_limit_sec }}s</span>
            {% endif %}
          </li>
        {% else %}
          <li>No CAN IDs detected yet</li>
        {% endfor %}
        </ul>
      </section>
    </div>

    <section class="card" style="margin-top: 20px;">
      <h2>GNSS positie</h2>
      <p class="sub">Laatste ontvangen positie uit de NMEA GGA-stroom. Wordt bijgewerkt zodra een geldig GGA-bericht binnenkomt.</p>
      <div id="gnss-card" class="status-grid" style="grid-template-columns: repeat(4, minmax(0,1fr));">
        <div class="status-item">
          <strong>Status</strong>
          {% set fq = gnss.fix_quality %}
          <span class="pill"
                style="{% if fq == 4 %}background:var(--ok-bg);color:var(--ok-text);
                       {%- elif fq == 5 %}background:var(--warn-bg);color:var(--warn-text);
                       {%- elif fq in (1,2,3) %}background:#eef0f3;color:#4a5568;
                       {%- else %}background:var(--bad-bg);color:var(--bad-text);{% endif %}"
                id="gnss-fix">{{ gnss.fix_label }}</span>
        </div>
        <div class="status-item">
          <strong>Coördinaten</strong>
          <div class="muted" id="gnss-coords">
            {% if gnss.lat is not none and gnss.lon is not none %}
              {{ "%.8f"|format(gnss.lat) }}<br>{{ "%.8f"|format(gnss.lon) }}
            {% else %}
              —
            {% endif %}
          </div>
        </div>
        <div class="status-item">
          <strong>Hoogte</strong>
          <div class="muted" id="gnss-alt">
            {% if gnss.altitude is not none %}
              {{ gnss.altitude }} m
            {% else %}
              —
            {% endif %}
          </div>
        </div>
        <div class="status-item">
          <strong>Satellieten</strong>
          <div class="muted" id="gnss-sat">
            {% if gnss.satellites is not none %}{{ gnss.satellites }}{% else %}—{% endif %}
          </div>
        </div>
        <div class="status-item">
          <strong>HDOP / VDOP / PDOP</strong>
          <div class="muted" id="gnss-dop">
            {% if gnss.hdop is not none %}
              H: {{ gnss.hdop }}
              &nbsp;·&nbsp; V: {{ gnss.vdop if gnss.vdop is not none else '—' }}
              &nbsp;·&nbsp; P: {{ gnss.pdop if gnss.pdop is not none else '—' }}
            {% else %}
              —
            {% endif %}
          </div>
        </div>
        <div class="status-item">
          <strong>Nauwkeurigheid (1σ)</strong>
          <div class="muted" id="gnss-acc">
            {% if gnss.acc_lat is not none %}
              N: {{ "%.3f"|format(gnss.acc_lat) }} m<br>
              E: {{ "%.3f"|format(gnss.acc_lon) }} m<br>
              H: {{ "%.3f"|format(gnss.acc_alt) if gnss.acc_alt is not none else '—' }} m
            {% else %}
              —
            {% endif %}
          </div>
        </div>
        <div class="status-item">
          <strong>Laatste update</strong>
          <div class="muted" id="gnss-ts">
            {% if gnss.ts %}{{ gnss.ts[:19] | replace("T"," ") }}{% else %}—{% endif %}
          </div>
        </div>
      </div>
    </section>

    <section class="card" style="margin-top: 20px;">
      <h2>AWS IoT & S3 uploads</h2>
      <p class="sub">Live verbindingsstatus en uploadstatistieken per datatype.</p>
      <div id="aws-s3-status" class="status-grid" style="grid-template-columns: repeat(3, minmax(0,1fr));">
        <div class="status-item">
          <strong>AWS IoT</strong>
          <div class="muted" id="s3-iot-status">{{ aws_status_text }}</div>
        </div>
        <div class="status-item">
          <strong>S3 CAN uploads</strong>
          <div class="muted" id="s3-can-info">
            {% if s3_status.can.last_upload %}
              {{ s3_status.can.total_uploads }} uploads &middot; {{ s3_status.can.total_records }} records<br>
              <small>Laatste: {{ s3_status.can.last_upload[:19] | replace("T"," ") }}</small>
            {% else %}
              Nog geen uploads
            {% endif %}
          </div>
        </div>
        <div class="status-item">
          <strong>S3 NMEA uploads</strong>
          <div class="muted" id="s3-nmea-info">
            {% if s3_status.nmea.last_upload %}
              {{ s3_status.nmea.total_uploads }} uploads &middot; {{ s3_status.nmea.total_records }} records<br>
              <small>Laatste: {{ s3_status.nmea.last_upload[:19] | replace("T"," ") }}</small>
            {% else %}
              Nog geen uploads
            {% endif %}
          </div>
        </div>
      </div>
    </section>

    <section class="card" style="margin-top: 20px;">
      <h2>Systeemstatus</h2>
      <p class="sub">CPU- en geheugengebruik van de VG710 container. Wordt elke 5 seconden bijgewerkt.</p>
      <div class="status-grid" style="grid-template-columns: repeat(3, minmax(0,1fr));">
        <div class="status-item">
          <strong>CPU gebruik</strong>
          <div style="margin-top:6px;">
            <div style="background:#e9eef5;border-radius:999px;height:10px;overflow:hidden;">
              <div id="cpu-bar" style="height:10px;border-radius:999px;transition:width 0.5s;width:{{ sys.cpu_percent or 0 }}%;background:{% if (sys.cpu_percent or 0) > 80 %}var(--bad-text){% elif (sys.cpu_percent or 0) > 50 %}#e6a817{% else %}var(--ok-text){% endif %};"></div>
            </div>
            <div class="muted" id="cpu-text" style="margin-top:4px;font-size:13px;">
              {% if sys.cpu_percent is not none %}{{ sys.cpu_percent }}%{% else %}—{% endif %}
            </div>
          </div>
        </div>
        <div class="status-item">
          <strong>Geheugen</strong>
          <div style="margin-top:6px;">
            <div style="background:#e9eef5;border-radius:999px;height:10px;overflow:hidden;">
              <div id="mem-bar" style="height:10px;border-radius:999px;transition:width 0.5s;width:{{ sys.mem_percent or 0 }}%;background:{% if (sys.mem_percent or 0) > 80 %}var(--bad-text){% elif (sys.mem_percent or 0) > 50 %}#e6a817{% else %}var(--ok-text){% endif %};"></div>
            </div>
            <div class="muted" id="mem-text" style="margin-top:4px;font-size:13px;">
              {% if sys.mem_used_mb is not none %}{{ sys.mem_used_mb }} / {{ sys.mem_total_mb }} MB ({{ sys.mem_percent }}%){% else %}—{% endif %}
            </div>
          </div>
        </div>
        <div class="status-item">
          <strong>Load average</strong>
          <div class="muted" id="load-text" style="margin-top:6px;font-size:13px;">
            {% if sys.load_1 is not none %}1m: {{ sys.load_1 }} &nbsp;·&nbsp; 5m: {{ sys.load_5 }}{% else %}—{% endif %}
          </div>
        </div>
      </div>
    </section>

    <section class="card" style="margin-top: 20px;">
      <h2>CAN interface beheer</h2>
      <p class="sub">Beheer de CAN bus interface: status opvragen, interface aan/uitzetten en baudrate wijzigen.</p>
      <div style="display:flex;flex-wrap:wrap;gap:10px;align-items:flex-end;margin-bottom:16px;">
        <button type="button" class="secondary" onclick="canControl('status')">Status</button>
        <button type="button" class="secondary" onclick="canControl('down')">CAN Down</button>
        <button type="button" class="secondary" onclick="canControl('up')">CAN Up</button>
        <div style="display:flex;gap:8px;align-items:center;">
          <select id="can-bitrate" style="height:36px;padding:0 10px;border-radius:8px;border:1px solid var(--line);background:var(--bg2);color:var(--fg);font-size:13px;">
            <option value="125000">125 kbps</option>
            <option value="250000" selected>250 kbps</option>
            <option value="500000">500 kbps</option>
            <option value="1000000">1 Mbps</option>
          </select>
          <button type="button" onclick="canControl('set_bitrate')">Baudrate instellen</button>
        </div>
      </div>
      <pre id="can-ctrl-output" style="min-height:60px;background:var(--bg2);border:1px solid var(--line);border-radius:10px;padding:12px;font-size:12px;white-space:pre-wrap;color:var(--fg);margin:0;"></pre>
    </section>

    <section class="card" style="margin-top: 20px;">
      <h2>Völkel ASB Sensor — CAN ID configuratie</h2>
      <p class="sub">Detecteer aangesloten ASB sensoren (CANopen) en wijzig het node ID. Sluit slechts 1 sensor aan tijdens het wijzigen.</p>

      <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:16px;">
        <button type="button" onclick="volkelScan()">Detecteer sensoren</button>
        <span id="volkel-scan-status" class="muted" style="font-size:13px;"></span>
      </div>

      <div id="volkel-result" style="display:none;">
        <div id="volkel-device-info" style="margin-bottom:16px;"></div>

        <div id="volkel-change-form" style="display:none;">
          <div style="font-size:13px;font-weight:700;margin-bottom:10px;padding-bottom:6px;border-bottom:1px solid var(--line);">Node ID wijzigen</div>
          <div style="display:grid;grid-template-columns:1fr 1fr auto;gap:10px;align-items:end;max-width:480px;">
            <div>
              <label style="font-size:13px;font-weight:600;display:block;margin-bottom:4px;">Huidig node ID</label>
              <input type="number" id="volkel-current-id" min="1" max="127" readonly
                     style="background:#f7faff;cursor:default;">
            </div>
            <div>
              <label style="font-size:13px;font-weight:600;display:block;margin-bottom:4px;">Nieuw node ID (1–127)</label>
              <input type="number" id="volkel-new-id" min="1" max="127" placeholder="bijv. 5">
            </div>
            <button type="button" onclick="volkelChangeId()">Wijzig node ID</button>
          </div>
          <p class="muted" style="margin-top:8px;font-size:12px;">
            De sensor wordt automatisch herstart. Nieuwe CAN IDs zijn actief na ~2 seconden.<br>
            TPDO1: <code>0x180 + node_id</code> &nbsp;·&nbsp; Heartbeat: <code>0x700 + node_id</code>
          </p>
        </div>
      </div>

      <pre id="volkel-output" style="display:none;min-height:40px;margin-top:12px;font-size:12px;"></pre>
    </section>

    <section class="card" style="margin-top: 20px;">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
        <div>
          <h2 style="margin:0;">CAN berichten</h2>
          <p class="sub" style="margin:4px 0 0 0;">Live stroom van inkomende CAN frames. Laatste 300 frames bewaard.</p>
        </div>
        <div style="display:flex;gap:8px;align-items:center;">
          <button type="button" id="can-pause-btn" class="secondary" onclick="toggleCanPause()" style="white-space:nowrap;display:none;">Pauzeer</button>
          <button type="button" id="can-clear-btn" class="secondary" onclick="clearCanLog()" style="white-space:nowrap;display:none;">Leeg</button>
          <button type="button" id="can-toggle-btn" class="secondary" onclick="toggleCanWindow()" style="white-space:nowrap;">Toon</button>
        </div>
      </div>
      <div id="can-window" style="display:none;">
        <pre id="can-log" style="height:380px;overflow-y:auto;margin:0;font-size:12px;line-height:1.5;">Wachten op CAN data…</pre>
        <p class="muted" id="can-row-count" style="margin-top:8px;font-size:12px;"></p>
      </div>
    </section>

    <section class="card" style="margin-top: 20px;">
      <h2>S3 upload instellingen</h2>
      <p class="sub">Stel de flush-interval en batchgrootte in voor CAN- en NMEA-data. Wijzigingen worden opgeslagen in config.json en zijn actief na de volgende container-herstart.</p>
      <form method="post" action="/save_s3_settings" style="display:grid;gap:16px;">
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;">
          <div>
            <div style="font-size:13px;font-weight:700;margin-bottom:10px;padding-bottom:6px;border-bottom:1px solid var(--line);">CAN data</div>
            <div style="display:grid;gap:10px;">
              <div>
                <label style="font-size:13px;font-weight:600;display:block;margin-bottom:4px;">Flush interval (sec)</label>
                <input type="number" min="1" name="s3_flush_interval_sec" value="{{ s3_settings.s3_flush_interval_sec }}">
              </div>
              <div>
                <label style="font-size:13px;font-weight:600;display:block;margin-bottom:4px;">Batch grootte (records)</label>
                <input type="number" min="1" name="s3_batch_size" value="{{ s3_settings.s3_batch_size }}">
              </div>
            </div>
          </div>
          <div>
            <div style="font-size:13px;font-weight:700;margin-bottom:10px;padding-bottom:6px;border-bottom:1px solid var(--line);">NMEA data</div>
            <div style="display:grid;gap:10px;">
              <div>
                <label style="font-size:13px;font-weight:600;display:block;margin-bottom:4px;">Flush interval (sec)</label>
                <input type="number" min="1" name="s3_nmea_flush_interval_sec" value="{{ s3_settings.s3_nmea_flush_interval_sec }}">
              </div>
              <div>
                <label style="font-size:13px;font-weight:600;display:block;margin-bottom:4px;">Batch grootte (records)</label>
                <input type="number" min="1" name="s3_nmea_batch_size" value="{{ s3_settings.s3_nmea_batch_size }}">
              </div>
            </div>
          </div>
        </div>
        <div><input type="submit" value="Opslaan"></div>
      </form>
    </section>

    <section class="card" style="margin-top: 20px;">
      <h2>CAN sensor groepen</h2>
      <p class="sub">Definieer sensor groepen op basis van CAN ID-bereik. Alle IDs binnen het bereik krijgen de naam en upload rate van de groep. ID-bereik kan later worden ingevuld zodra de sensoren bekend zijn.</p>
      <form method="post" action="/save_can_groups">
        <table>
          <tr>
            <th>Naam</th>
            <th>ID van</th>
            <th>ID t/m</th>
            <th>Upload rate (sec)</th>
            <th>Verwijder</th>
          </tr>
          {% for group in can_groups %}
          <tr>
            <td><input type="text" name="name_{{ loop.index0 }}" value="{{ group.name }}"></td>
            <td><input type="text" name="id_start_{{ loop.index0 }}" value="{{ group.id_start }}" placeholder="bijv. 0x180"></td>
            <td><input type="text" name="id_end_{{ loop.index0 }}" value="{{ group.id_end }}" placeholder="bijv. 0x183"></td>
            <td><input type="number" min="1" name="rate_{{ loop.index0 }}" value="{{ group.upload_rate_sec }}"></td>
            <td style="text-align:center;"><input type="checkbox" name="delete_{{ loop.index0 }}" value="1"></td>
          </tr>
          {% endfor %}
          {% for idx in range(4) %}
          <tr>
            <td><input type="text" name="new_name_{{ idx }}" value="" placeholder="bijv. Temperatuursensor"></td>
            <td><input type="text" name="new_id_start_{{ idx }}" value="" placeholder="0x180"></td>
            <td><input type="text" name="new_id_end_{{ idx }}" value="" placeholder="0x183"></td>
            <td><input type="number" min="1" name="new_rate_{{ idx }}" value="10"></td>
            <td></td>
          </tr>
          {% endfor %}
        </table>
        <p class="muted" style="margin-top:10px;">Gebruik hex (0x180) of decimaal (384). Laat ID-velden leeg als het bereik nog niet bekend is.</p>
        <input type="submit" value="Opslaan">
      </form>
    </section>

    <div class="grid bottom">
      <section class="card">
        <h2>CAN update rates</h2>
        <p class="sub">Stel per CAN ID het uploadinterval in. Nieuw gevonden IDs krijgen standaard 1 seconde.</p>
        <form method="post" action="/save_rates">
          <table>
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
          <p class="muted">Gebruik CAN ID in decimaal of hex, bijvoorbeeld 914 of 0x392.</p>
          <input type="submit" value="Save CAN Rates">
        </form>
      </section>

      <section class="card">
        <h2>NTRIP instellingen</h2>
        <p class="sub">De app fungeert als NTRIP proxy. De Septentrio verbindt als client met de proxy; de app haalt zelf correcties op bij de upstream caster.</p>
        <form method="post" action="/save_ntrip" style="display:grid;gap:20px;">

          <div>
            <div style="font-size:13px;font-weight:700;margin-bottom:10px;padding-bottom:6px;border-bottom:1px solid var(--line);">
              Proxy server <span class="muted" style="font-weight:400;">(Septentrio verbindt hiermee)</span>
            </div>
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
              <div>
                <label style="font-size:13px;font-weight:600;display:block;margin-bottom:4px;">Luisteradres</label>
                <input type="text" name="proxy_host" value="{{ ntrip.proxy_host }}" placeholder="0.0.0.0">
              </div>
              <div>
                <label style="font-size:13px;font-weight:600;display:block;margin-bottom:4px;">Poort</label>
                <input type="number" name="proxy_port" value="{{ ntrip.proxy_port }}" placeholder="7791">
              </div>
              <div>
                <label style="font-size:13px;font-weight:600;display:block;margin-bottom:4px;">Gebruikersnaam</label>
                <input type="text" name="proxy_username" value="{{ ntrip.proxy_username }}" placeholder="proxyuser">
              </div>
              <div>
                <label style="font-size:13px;font-weight:600;display:block;margin-bottom:4px;">Wachtwoord</label>
                <input type="text" name="proxy_password" value="{{ ntrip.proxy_password }}" placeholder="proxypass">
              </div>
              <div>
                <label style="font-size:13px;font-weight:600;display:block;margin-bottom:4px;">Mountpoint</label>
                <input type="text" name="proxy_mountpoint" value="{{ ntrip.proxy_mountpoint }}" placeholder="proxymountpoint">
              </div>
            </div>
          </div>

          <div>
            <div style="font-size:13px;font-weight:700;margin-bottom:10px;padding-bottom:6px;border-bottom:1px solid var(--line);">
              Upstream caster <span class="muted" style="font-weight:400;">(app haalt hier RTCM correcties op)</span>
            </div>
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
              <div>
                <label style="font-size:13px;font-weight:600;display:block;margin-bottom:4px;">Host</label>
                <input type="text" name="host" value="{{ ntrip.host }}" placeholder="ntrip.example.com">
              </div>
              <div>
                <label style="font-size:13px;font-weight:600;display:block;margin-bottom:4px;">Poort</label>
                <input type="number" name="port" value="{{ ntrip.port }}" placeholder="2101">
              </div>
              <div>
                <label style="font-size:13px;font-weight:600;display:block;margin-bottom:4px;">Gebruikersnaam</label>
                <input type="text" name="username" value="{{ ntrip.username }}" placeholder="gebruiker">
              </div>
              <div>
                <label style="font-size:13px;font-weight:600;display:block;margin-bottom:4px;">Wachtwoord</label>
                <input type="text" name="password" value="{{ ntrip.password }}" placeholder="wachtwoord">
              </div>
            </div>
            <div style="margin-top:10px;">
              <label style="font-size:13px;font-weight:600;display:block;margin-bottom:4px;">
                Mountpoint
                <button type="button" class="secondary" id="fetch-mp-btn" onclick="fetchMountpoints()" style="margin-left:10px;min-height:32px;font-size:12px;padding:4px 12px;">Haal mountpoints op</button>
              </label>
              <select name="mountpoint" id="mountpoint-select" style="width:100%;border:1px solid var(--line);border-radius:12px;padding:10px 12px;font:inherit;background:#fff;color:var(--text);">
                <option value="{{ ntrip.mountpoint }}">{{ ntrip.mountpoint if ntrip.mountpoint else '— selecteer na ophalen —' }}</option>
              </select>
              <div id="mp-status" class="muted" style="margin-top:6px;font-size:12px;"></div>
            </div>
          </div>

          <div style="display:flex;align-items:center;gap:10px;">
            <label style="font-size:13px;font-weight:600;">NTRIP ingeschakeld</label>
            <input type="checkbox" name="enabled" value="1" {% if ntrip.enabled %}checked{% endif %} style="width:auto;accent-color:var(--accent);">
          </div>
          <div>
            <input type="submit" value="Opslaan">
          </div>
        </form>
      </section>

      <section class="card">
        <h2>Container shell</h2>
        <p class="sub">Voer shell-commando's uit binnen de container voor snelle diagnose van volumes, env vars en bestanden.</p>
        <form method="post" action="/shell" class="shell-form">
          <textarea name="command" rows="4" placeholder="Bijvoorbeeld: ls -la /data/vgapp&#10;of: ls -la /data/vgapp/certs">{{ shell_command }}</textarea>
          <div class="shell-buttons">
            <button type="submit">Run command</button>
            <button type="submit" name="preset" value="list_config" class="secondary">List /data/vgapp</button>
            <button type="submit" name="preset" value="list_certs" class="secondary">List certs</button>
            <button type="submit" name="preset" value="show_aws_env" class="secondary">Show AWS env</button>
          </div>
        </form>
        <div class="note">
          Let op: deze shell draait in de container. Gebruik dit alleen voor beheer en diagnose.
        </div>
        <div style="margin-top: 14px;">
          <pre>{{ shell_output }}</pre>
        </div>
      </section>
    </div>

    <section class="card" style="margin-top: 20px;">
      <h2>Software update</h2>
      <p class="sub">Download de laatste versie van GitHub, laad de Docker image en herstart de container automatisch. Vereist dat <code>/var/run/docker.sock</code> gemount is.</p>
      <div class="status-grid" style="grid-template-columns: repeat(2, minmax(0,1fr)); margin-bottom:16px;">
        <div class="status-item">
          <strong>Huidige versie</strong>
          <div class="muted">{{ app_version }}</div>
        </div>
        <div class="status-item">
          <strong>Nieuwste versie op GitHub</strong>
          <div class="muted" id="gh-latest-version">Ophalen…</div>
        </div>
      </div>
      <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:16px;">
        <button type="button" id="update-btn" onclick="startUpdate()">Download &amp; installeer nieuwste versie</button>
        <span id="update-status" class="muted" style="font-size:13px;"></span>
      </div>
      <pre id="update-log" style="min-height:60px;font-size:12px;"></pre>
    </section>
  </div>

  <script>
    async function refreshStatus() {
      try {
        const response = await fetch('/status_json', { cache: 'no-store' });
        if (!response.ok) return;
        const data = await response.json();

        const iotEl = document.getElementById('s3-iot-status');
        if (iotEl) iotEl.textContent = data.aws_status_text;

        const canEl = document.getElementById('s3-can-info');
        if (canEl && data.s3_status && data.s3_status.can) {
          const c = data.s3_status.can;
          canEl.innerHTML = c.last_upload
            ? c.total_uploads + ' uploads · ' + c.total_records + ' records<br><small>Laatste: ' + c.last_upload.slice(0,19).replace('T',' ') + '</small>'
            : 'Nog geen uploads';
        }

        const nmeaEl = document.getElementById('s3-nmea-info');
        if (nmeaEl && data.s3_status && data.s3_status.nmea) {
          const n = data.s3_status.nmea;
          nmeaEl.innerHTML = n.last_upload
            ? n.total_uploads + ' uploads · ' + n.total_records + ' records<br><small>Laatste: ' + n.last_upload.slice(0,19).replace('T',' ') + '</small>'
            : 'Nog geen uploads';
        }

        if (data.sys) {
          const s = data.sys;
          const cpuBar = document.getElementById('cpu-bar');
          const cpuText = document.getElementById('cpu-text');
          const memBar = document.getElementById('mem-bar');
          const memText = document.getElementById('mem-text');
          const loadText = document.getElementById('load-text');
          if (cpuBar && s.cpu_percent !== null) {
            cpuBar.style.width = s.cpu_percent + '%';
            cpuBar.style.background = s.cpu_percent > 80 ? 'var(--bad-text)' : s.cpu_percent > 50 ? '#e6a817' : 'var(--ok-text)';
            if (cpuText) cpuText.textContent = s.cpu_percent + '%';
          }
          if (memBar && s.mem_percent !== null) {
            memBar.style.width = s.mem_percent + '%';
            memBar.style.background = s.mem_percent > 80 ? 'var(--bad-text)' : s.mem_percent > 50 ? '#e6a817' : 'var(--ok-text)';
            if (memText) memText.textContent = s.mem_used_mb + ' / ' + s.mem_total_mb + ' MB (' + s.mem_percent + '%)';
          }
          if (loadText && s.load_1 !== null)
            loadText.innerHTML = '1m: ' + s.load_1 + ' &nbsp;·&nbsp; 5m: ' + s.load_5;
        }

        if (data.gnss) {
          const g = data.gnss;
          const fixEl = document.getElementById('gnss-fix');
          if (fixEl) {
            fixEl.textContent = g.fix_label;
            fixEl.className = 'pill';
            if (g.fix_quality === 4)
              fixEl.style.cssText = 'background:var(--ok-bg);color:var(--ok-text);';
            else if (g.fix_quality === 5)
              fixEl.style.cssText = 'background:var(--warn-bg);color:var(--warn-text);';
            else if (g.fix_quality > 0)
              fixEl.style.cssText = 'background:#eef0f3;color:#4a5568;';
            else
              fixEl.style.cssText = 'background:var(--bad-bg);color:var(--bad-text);';
          }
          const coordEl = document.getElementById('gnss-coords');
          if (coordEl) coordEl.innerHTML = (g.lat !== null && g.lon !== null)
            ? g.lat.toFixed(8) + '<br>' + g.lon.toFixed(8) : '—';
          const altEl = document.getElementById('gnss-alt');
          if (altEl) altEl.textContent = g.altitude !== null ? g.altitude + ' m' : '—';
          const satEl = document.getElementById('gnss-sat');
          if (satEl) satEl.textContent = g.satellites !== null ? g.satellites : '—';
          const dopEl = document.getElementById('gnss-dop');
          if (dopEl) dopEl.innerHTML = g.hdop !== null
            ? 'H: ' + g.hdop + ' &nbsp;·&nbsp; V: ' + (g.vdop ?? '—') + ' &nbsp;·&nbsp; P: ' + (g.pdop ?? '—') : '—';
          const accEl = document.getElementById('gnss-acc');
          if (accEl) accEl.innerHTML = g.acc_lat !== null
            ? 'N: ' + g.acc_lat.toFixed(3) + ' m<br>E: ' + g.acc_lon.toFixed(3) + ' m<br>H: ' + (g.acc_alt !== null ? g.acc_alt.toFixed(3) + ' m' : '—') : '—';
          const tsEl = document.getElementById('gnss-ts');
          if (tsEl) tsEl.textContent = g.ts ? g.ts.slice(0,19).replace('T',' ') : '—';
        }
      } catch (e) {
        // ignore polling errors
      }
    }

    setInterval(refreshStatus, 5000);

    let canWindowVisible = false;
    let canPollTimer = null;
    let canLastSeq = 0;
    let canTotalFrames = 0;
    let canPaused = false;

    function toggleCanWindow() {
      const win = document.getElementById('can-window');
      const btn = document.getElementById('can-toggle-btn');
      const pauseBtn = document.getElementById('can-pause-btn');
      const clearBtn = document.getElementById('can-clear-btn');
      canWindowVisible = !canWindowVisible;
      win.style.display = canWindowVisible ? 'block' : 'none';
      btn.textContent = canWindowVisible ? 'Verberg' : 'Toon';
      pauseBtn.style.display = canWindowVisible ? 'inline-flex' : 'none';
      clearBtn.style.display = canWindowVisible ? 'inline-flex' : 'none';
      if (canWindowVisible) {
        canLastSeq = 0;
        document.getElementById('can-log').textContent = '';
        pollCanLog();
        canPollTimer = setInterval(pollCanLog, 1000);
      } else {
        if (canPollTimer) { clearInterval(canPollTimer); canPollTimer = null; }
      }
    }

    async function canControl(action) {
      const out = document.getElementById('can-ctrl-output');
      out.textContent = 'Bezig…';
      const body = new FormData();
      body.append('action', action);
      if (action === 'set_bitrate') {
        body.append('bitrate', document.getElementById('can-bitrate').value);
      }
      try {
        const resp = await fetch('/can_control', { method: 'POST', body });
        const data = await resp.json();
        out.textContent = data.output || JSON.stringify(data);
      } catch (e) {
        out.textContent = 'Fout: ' + e;
      }
    }

    function toggleCanPause() {
      canPaused = !canPaused;
      document.getElementById('can-pause-btn').textContent = canPaused ? 'Hervat' : 'Pauzeer';
    }

    function clearCanLog() {
      document.getElementById('can-log').textContent = '';
      canTotalFrames = 0;
      document.getElementById('can-row-count').textContent = '';
    }

    function fmtHex(hex) {
      var m = (hex || '').match(/.{1,2}/g); return m ? m.join(' ') : '\u2014';
    }

    async function pollCanLog() {
      if (canPaused) return;
      try {
        const resp = await fetch('/can_log?since=' + canLastSeq, { cache: 'no-store' });
        if (!resp.ok) return;
        const data = await resp.json();
        const frames = data.frames || [];
        if (!frames.length) return;

        const logEl = document.getElementById('can-log');
        const countEl = document.getElementById('can-row-count');
        const atBottom = logEl.scrollHeight - logEl.scrollTop - logEl.clientHeight < 40;

        const lines = frames.map(f => {
          const ts = f.ts ? f.ts.slice(11, 19) : '??:??:??';
          const id = (f.id_hex || '???').padEnd(7);
          const dlc = String(f.dlc !== undefined ? f.dlc : '?').padStart(1);
          const data = fmtHex(f.data_hex);
          return '[' + ts + ']  ' + id + '  DLC=' + dlc + '  ' + data;
        });

        logEl.textContent += (logEl.textContent ? '\n' : '') + lines.join('\n');

        // Begrens het zichtbare log tot ~600 regels
        const all = logEl.textContent.split('\n');
        if (all.length > 600) logEl.textContent = all.slice(-500).join('\n');

        canLastSeq = frames[frames.length - 1].seq;
        canTotalFrames += frames.length;
        if (countEl) countEl.textContent = canTotalFrames.toLocaleString() + ' frames ontvangen';

        if (atBottom) logEl.scrollTop = logEl.scrollHeight;
      } catch (e) {
        // ignore polling errors
      }
    }

    async function volkelScan() {
      const statusEl = document.getElementById('volkel-scan-status');
      const resultEl = document.getElementById('volkel-result');
      const infoEl = document.getElementById('volkel-device-info');
      const formEl = document.getElementById('volkel-change-form');
      const outEl = document.getElementById('volkel-output');
      statusEl.textContent = 'Scannen\u2026';
      outEl.style.display = 'none';
      try {
        const resp = await fetch('/volkel_scan', { cache: 'no-store' });
        const data = await resp.json();
        resultEl.style.display = 'block';
        statusEl.textContent = '';
        if (data.count === 0) {
          infoEl.innerHTML = '<span class="pill bad">Geen V\u00f6lkel ASB sensor gevonden</span>' +
            '<p class="muted" style="margin-top:8px;font-size:13px;">Controleer of de sensor is aangesloten en berichten verstuurt (heartbeat 0x701\u20130x77F of TPDO1 0x181\u20130x1FF).</p>';
          formEl.style.display = 'none';
        } else if (data.count === 1) {
          const nodeId = data.detected_nodes[0];
          const tpdo1 = '0x' + (0x180 + nodeId).toString(16).toUpperCase();
          const hb = '0x' + (0x700 + nodeId).toString(16).toUpperCase();
          infoEl.innerHTML = '<span class="pill ok" style="margin-bottom:10px;">1 sensor gevonden</span>' +
            '<div style="display:grid;grid-template-columns:auto auto auto;gap:6px 20px;font-size:13px;margin-top:10px;">' +
            '<span style="color:var(--muted)">Detectie</span><span style="color:var(--muted)">Node ID</span><span style="color:var(--muted)">TPDO1 / Heartbeat CAN ID</span>' +
            '<strong>' + data.detection_method + '</strong>' +
            '<strong>' + nodeId + '</strong>' +
            '<strong>' + tpdo1 + ' / ' + hb + '</strong></div>';
          document.getElementById('volkel-current-id').value = nodeId;
          formEl.style.display = 'block';
        } else {
          const nodeList = data.detected_nodes.map(n =>
            'Node\u00a0' + n + '\u00a0(0x' + (0x180 + n).toString(16).toUpperCase() + ')'
          ).join(', ');
          infoEl.innerHTML = '<span class="pill bad" style="margin-bottom:8px;">' + data.count + ' sensoren gevonden \u2014 sluit slechts 1 sensor aan</span>' +
            '<p class="muted" style="font-size:13px;margin-top:8px;">Gevonden nodes: ' + nodeList + '</p>';
          formEl.style.display = 'none';
        }
      } catch(e) {
        statusEl.textContent = 'Fout: ' + e.message;
      }
    }

    async function volkelChangeId() {
      const currentId = document.getElementById('volkel-current-id').value;
      const newId = document.getElementById('volkel-new-id').value;
      const outEl = document.getElementById('volkel-output');
      const n = parseInt(newId);
      if (!newId || isNaN(n) || n < 1 || n > 127) {
        alert('Voer een geldig nieuw node ID in (1\u2013127)');
        return;
      }
      if (!confirm('Node ID wijzigen van ' + currentId + ' naar ' + n + '?\nDe sensor wordt herstart.')) return;
      outEl.style.display = 'block';
      outEl.textContent = 'Bezig\u2026';
      const body = new FormData();
      body.append('current_node_id', currentId);
      body.append('new_node_id', n);
      try {
        const resp = await fetch('/volkel_change_id', { method: 'POST', body });
        const data = await resp.json();
        outEl.textContent = data.message + '\n\n' + (data.output || '');
        if (data.success) setTimeout(volkelScan, 3000);
      } catch(e) {
        outEl.textContent = 'Fout: ' + e.message;
      }
    }

    async function fetchLatestVersion() {
      const el = document.getElementById('gh-latest-version');
      try {
        const resp = await fetch('/gh_latest_version', { cache: 'no-store' });
        const data = await resp.json();
        if (el) el.textContent = data.tag_name || data.error || 'onbekend';
      } catch(e) {
        if (el) el.textContent = 'ophalen mislukt';
      }
    }

    fetchLatestVersion();

    let _updatePollTimer = null;

    async function startUpdate() {
      const btn = document.getElementById('update-btn');
      const statusEl = document.getElementById('update-status');
      const logEl = document.getElementById('update-log');
      if (!btn || !statusEl || !logEl) { alert('UI-elementen niet gevonden'); return; }
      btn.disabled = true;
      statusEl.textContent = 'Bezig met starten…';
      logEl.style.display = 'block';
      logEl.textContent = '';
      try {
        const resp = await fetch('/gh_update', { method: 'POST' });
        if (!resp.ok) {
          const d = await resp.json().catch(() => ({}));
          statusEl.textContent = 'Fout: ' + (d.error || resp.status);
          btn.disabled = false;
          return;
        }
      } catch(e) {
        statusEl.textContent = 'Netwerkfout: ' + e.message;
        btn.disabled = false;
        return;
      }
      _updatePollTimer = setInterval(pollUpdateStatus, 2000);
    }

    async function pollUpdateStatus() {
      try {
        const resp = await fetch('/gh_update_status', { cache: 'no-store' });
        const data = await resp.json();
        const logEl = document.getElementById('update-log');
        const statusEl = document.getElementById('update-status');
        const btn = document.getElementById('update-btn');
        logEl.textContent = (data.log || []).join('\n');
        logEl.scrollTop = logEl.scrollHeight;
        if (data.done) {
          clearInterval(_updatePollTimer);
          btn.disabled = false;
          statusEl.textContent = data.success ? 'Gereed — herstart de container.' : 'Mislukt.';
        } else {
          statusEl.textContent = 'Bezig…';
        }
      } catch(e) { /* negeer pollingfouten */ }
    }

    async function fetchMountpoints() {
      const btn = document.getElementById('fetch-mp-btn');
      const status = document.getElementById('mp-status');
      const select = document.getElementById('mountpoint-select');
      btn.disabled = true;
      status.textContent = 'Ophalen…';

      const host = document.querySelector('input[name="host"]').value.trim();
      const port = document.querySelector('input[name="port"]').value.trim();
      const username = document.querySelector('input[name="username"]').value.trim();
      const password = document.querySelector('input[name="password"]').value.trim();
      const current = select.value;

      try {
        const params = new URLSearchParams({ host, port, username, password });
        const resp = await fetch('/ntrip_sourcetable?' + params.toString(), { cache: 'no-store' });
        const data = await resp.json();
        if (!resp.ok || data.error) {
          status.textContent = 'Fout: ' + (data.error || resp.status);
          return;
        }
        select.innerHTML = '';
        data.mountpoints.forEach(mp => {
          const opt = document.createElement('option');
          opt.value = mp.name;
          opt.textContent = mp.name + (mp.format ? '  (' + mp.format + ')' : '');
          if (mp.name === current) opt.selected = true;
          select.appendChild(opt);
        });
        status.textContent = data.mountpoints.length + ' mountpoints gevonden.';
      } catch (e) {
        status.textContent = 'Netwerkfout: ' + e.message;
      } finally {
        btn.disabled = false;
      }
    }
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


def load_can_groups():
    groups = load_config_data().get("can_sensor_groups", [])
    if not isinstance(groups, list):
        return []
    return groups


def save_config_with_groups(groups):
    cfg = load_config_data()
    cfg["can_sensor_groups"] = groups
    with open(f"{BASE_DIR}/config.json", "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)


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


def _parse_can_id_int(value):
    if not value:
        return None
    try:
        text = str(value).strip()
        if text.lower().startswith("0x"):
            return int(text, 16)
        return int(text, 10)
    except ValueError:
        return None


def current_can_ids():
    path = f"{BASE_DIR}/can_ids.json"
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            return []
        groups = load_can_groups()
        for item in data:
            can_int = item.get("id")
            item["group_name"] = None
            if can_int is not None:
                for g in groups:
                    g_start = _parse_can_id_int(g.get("id_start"))
                    g_end = _parse_can_id_int(g.get("id_end"))
                    if g_start is not None and g_end is not None:
                        if g_start <= can_int <= g_end:
                            item["group_name"] = g.get("name")
                            break
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


def gnss_status_data():
    path = f"{BASE_DIR}/gnss_status.json"
    default = {"fix_quality": 0, "fix_label": "No fix", "lat": None, "lon": None,
               "satellites": None, "hdop": None, "altitude": None, "ts": None}
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return default


def s3_status_data():
    path = f"{BASE_DIR}/s3_status.json"
    default = {
        "can": {"total_records": 0, "total_uploads": 0, "last_key": None, "last_upload": None},
        "nmea": {"total_records": 0, "total_uploads": 0, "last_key": None, "last_upload": None},
    }
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return default


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


def s3_settings():
    cfg = load_config_data()
    return {
        "s3_flush_interval_sec": int(cfg.get("s3_flush_interval_sec", 30)),
        "s3_batch_size": int(cfg.get("s3_batch_size", 100)),
        "s3_nmea_flush_interval_sec": int(cfg.get("s3_nmea_flush_interval_sec", 30)),
        "s3_nmea_batch_size": int(cfg.get("s3_nmea_batch_size", 100)),
    }


def ntrip_config():
    cfg = load_config_data()
    n = cfg.get("ntrip", {})
    p = cfg.get("ntrip_proxy", {})
    return {
        "enabled": bool(n.get("enabled", False)),
        "host": n.get("host", ""),
        "port": int(n.get("port", 2101) or 2101),
        "mountpoint": n.get("mountpoint", ""),
        "username": n.get("username", ""),
        "password": n.get("password", ""),
        "proxy_host": p.get("host", "0.0.0.0"),
        "proxy_port": int(p.get("port", 7791) or 7791),
        "proxy_username": p.get("username", "proxyuser"),
        "proxy_password": p.get("password", "proxypass"),
        "proxy_mountpoint": p.get("mountpoint", "proxymountpoint"),
    }


def fetch_ntrip_sourcetable(host, port, username, password):
    auth = base64.b64encode(f"{username}:{password}".encode()).decode("ascii")
    req = (
        f"GET / HTTP/1.0\r\n"
        f"Host: {host}\r\n"
        f"User-Agent: NTRIP VG710/1.0\r\n"
        f"Authorization: Basic {auth}\r\n"
        f"Connection: close\r\n\r\n"
    )
    sock = socket.create_connection((host, int(port)), timeout=10)
    sock.settimeout(10)
    try:
        sock.sendall(req.encode("ascii"))
        raw = b""
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            raw += chunk
            if b"ENDSOURCETABLE" in raw or len(raw) > 512 * 1024:
                break
    finally:
        sock.close()

    text = raw.decode("latin1", errors="ignore")
    # strip HTTP/ICY header
    if "\r\n\r\n" in text:
        text = text.split("\r\n\r\n", 1)[1]

    mountpoints = []
    for line in text.splitlines():
        if not line.startswith("STR;"):
            continue
        parts = line.split(";")
        name = parts[1] if len(parts) > 1 else ""
        fmt = parts[3] if len(parts) > 3 else ""
        country = parts[8] if len(parts) > 8 else ""
        if name:
            mountpoints.append({"name": name, "format": fmt, "country": country})
    return sorted(mountpoints, key=lambda x: x["name"])


def shell_presets():
    return {
        "list_config": "ls -la /data/vgapp",
        "list_certs": "ls -la /data/vgapp/certs",
        "show_aws_env": "env | sort | grep '^AWS_'",
    }


def run_shell_command(command):
    try:
        result = subprocess.run(
            ["sh", "-lc", command],
            capture_output=True,
            text=True,
            timeout=20,
        )
        output = []
        output.append(f"$ {command}")
        if result.stdout:
            output.append(result.stdout.rstrip())
        if result.stderr:
            output.append(result.stderr.rstrip())
        output.append(f"\nExit code: {result.returncode}")
        return "\n".join(output)
    except subprocess.TimeoutExpired:
        return f"$ {command}\n\nCommand timed out after 20 seconds."
    except Exception as e:
        return f"$ {command}\n\nError: {e}"


def render_page(shell_command="", shell_output="No command executed yet."):
    cfg = load_config_data()
    return render_template_string(
        HTML,
        config=exists(f"{BASE_DIR}/config.json"),
        s3cfg=exists(f"{BASE_DIR}/s3.json"),
        crt=exists(f"{CERT_DIR}/device.pem.crt"),
        key=exists(f"{CERT_DIR}/private.pem.key"),
        ca=exists(f"{CERT_DIR}/AmazonRootCA1.pem"),
        can_ids=current_can_ids(),
        can_groups=load_can_groups(),
        rate_rows=build_rate_rows(),
        aws_status_text=aws_status_text(),
        s3_status=s3_status_data(),
        gnss=gnss_status_data(),
        ntrip=ntrip_config(),
        s3_settings=s3_settings(),
        device_id=cfg.get("device_id"),
        asset_id=cfg.get("asset_id"),
        app_version=os.environ.get("APP_VERSION", "onbekend"),
        sys=system_stats(),
        shell_command=shell_command,
        shell_output=shell_output,
        range=range,
    )


@app.route("/")
def index():
    return render_page()


@app.route("/status_json")
def status_json():
    return {
        "aws_status_text": aws_status_text(),
        "config": exists(f"{BASE_DIR}/config.json"),
        "s3cfg": exists(f"{BASE_DIR}/s3.json"),
        "crt": exists(f"{CERT_DIR}/device.pem.crt"),
        "key": exists(f"{CERT_DIR}/private.pem.key"),
        "ca": exists(f"{CERT_DIR}/AmazonRootCA1.pem"),
        "can_ids": current_can_ids(),
        "rate_rows": build_rate_rows(),
        "s3_status": s3_status_data(),
        "gnss": gnss_status_data(),
        "sys": system_stats(),
    }


@app.route("/upload_config", methods=["POST"])
def upload_config():
    uploaded, error = get_uploaded_file()
    if error:
        return error
    filename = secure_filename(uploaded.filename)
    if filename == "config.json":
        uploaded.save(f"{BASE_DIR}/config.json")
    elif filename == "s3.json":
        uploaded.save(f"{BASE_DIR}/s3.json")
    else:
        return "Upload config.json or s3.json", 400
    return redirect(url_for("index"))


@app.route("/upload_cert", methods=["POST"])
def upload_cert():
    uploaded, error = get_uploaded_file()
    if error:
        return error
    filename = secure_filename(uploaded.filename)
    target_name = resolve_cert_target(filename)
    if target_name is None:
        return (
            "Unknown certificate filename. Use AmazonRootCA1.pem, a file ending in -certificate.pem.crt, or a file ending in -private.pem.key.",
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


@app.route("/save_can_groups", methods=["POST"])
def save_can_groups():
    groups = []
    idx = 0
    while True:
        name = request.form.get(f"name_{idx}", "").strip()
        if f"name_{idx}" not in request.form:
            break
        delete = request.form.get(f"delete_{idx}", "")
        if not delete and name:
            id_start = request.form.get(f"id_start_{idx}", "").strip()
            id_end = request.form.get(f"id_end_{idx}", "").strip()
            rate_raw = request.form.get(f"rate_{idx}", "10").strip()
            try:
                rate = int(rate_raw) if rate_raw else 10
            except ValueError:
                rate = 10
            groups.append({
                "name": name,
                "id_start": id_start,
                "id_end": id_end,
                "upload_rate_sec": rate,
            })
        idx += 1

    for new_idx in range(4):
        name = request.form.get(f"new_name_{new_idx}", "").strip()
        if not name:
            continue
        id_start = request.form.get(f"new_id_start_{new_idx}", "").strip()
        id_end = request.form.get(f"new_id_end_{new_idx}", "").strip()
        rate_raw = request.form.get(f"new_rate_{new_idx}", "10").strip()
        try:
            rate = int(rate_raw) if rate_raw else 10
        except ValueError:
            rate = 10
        groups.append({
            "name": name,
            "id_start": id_start,
            "id_end": id_end,
            "upload_rate_sec": rate,
        })

    save_config_with_groups(groups)
    return redirect(url_for("index"))


@app.route("/save_s3_settings", methods=["POST"])
def save_s3_settings():
    cfg = load_config_data()
    for key in ("s3_flush_interval_sec", "s3_batch_size",
                "s3_nmea_flush_interval_sec", "s3_nmea_batch_size"):
        try:
            value = int(request.form.get(key, 0))
            if value > 0:
                cfg[key] = value
        except ValueError:
            pass
    save_config_data(cfg)
    return redirect(url_for("index"))


@app.route("/save_ntrip", methods=["POST"])
def save_ntrip():
    cfg = load_config_data()
    cfg["ntrip"] = {
        "enabled": request.form.get("enabled") == "1",
        "host": request.form.get("host", "").strip(),
        "port": int(request.form.get("port", 2101) or 2101),
        "mountpoint": request.form.get("mountpoint", "").strip(),
        "username": request.form.get("username", "").strip(),
        "password": request.form.get("password", ""),
    }
    cfg["ntrip_proxy"] = {
        "host": request.form.get("proxy_host", "0.0.0.0").strip(),
        "port": int(request.form.get("proxy_port", 7791) or 7791),
        "username": request.form.get("proxy_username", "proxyuser").strip(),
        "password": request.form.get("proxy_password", "proxypass"),
        "mountpoint": request.form.get("proxy_mountpoint", "proxymountpoint").strip(),
    }
    save_config_data(cfg)
    return redirect(url_for("index"))


@app.route("/ntrip_sourcetable")
def ntrip_sourcetable():
    host = request.args.get("host", "").strip()
    port = request.args.get("port", "2101").strip()
    username = request.args.get("username", "").strip()
    password = request.args.get("password", "")
    if not host:
        return {"error": "Geen host opgegeven"}, 400
    try:
        mountpoints = fetch_ntrip_sourcetable(host, port, username, password)
        return {"mountpoints": mountpoints}
    except Exception as e:
        return {"error": str(e)}, 502


@app.route("/shell", methods=["GET", "POST"])
def shell():
    if request.method == "GET":
        return render_page()
    preset = request.form.get("preset", "").strip()
    command = request.form.get("command", "").strip()

    if preset:
        command = shell_presets().get(preset, command)

    if not command:
        return render_page(shell_command="", shell_output="No command provided.")

    output = run_shell_command(command)
    return render_page(shell_command=command, shell_output=output)


def _read_can_file():
    path = f"{BASE_DIR}/can_latest.json"
    if not os.path.exists(path):
        return {}, []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data.get("latest", []), data.get("log", [])
        if isinstance(data, list):
            return data, []
    except Exception:
        pass
    return [], []


@app.route("/can_data")
def can_data():
    latest, _ = _read_can_file()
    return latest


@app.route("/can_log")
def can_log():
    since = int(request.args.get("since", 0))
    _, log = _read_can_file()
    frames = [f for f in log if f.get("seq", 0) > since]
    return {"frames": frames}


@app.route("/can_control", methods=["POST"])
def can_control():
    action = request.form.get("action", "")
    channel = load_config_data().get("can_channel", "can0")
    bitrate = request.form.get("bitrate", "250000")

    allowed_bitrates = ("125000", "250000", "500000", "1000000")
    if action == "status":
        cmd = f"ip -details link show {channel}"
    elif action == "down":
        cmd = f"ip link set {channel} down"
    elif action == "up":
        cmd = f"ip link set {channel} up"
    elif action == "set_bitrate":
        if bitrate not in allowed_bitrates:
            return {"output": f"Ongeldige baudrate: {bitrate}"}, 400
        cmd = (
            f"ip link set {channel} down && "
            f"ip link set {channel} type can bitrate {bitrate} && "
            f"ip link set {channel} up"
        )
    else:
        return {"output": "Onbekende actie"}, 400

    output = run_shell_command(cmd)
    return {"output": output}


@app.route("/volkel_scan")
def volkel_scan():
    """Detecteer Völkel ASB sensoren op basis van CANopen heartbeat/TPDO1 CAN IDs."""
    can_ids = current_can_ids()

    heartbeat_nodes = []
    tpdo1_nodes = []

    for item in can_ids:
        can_id_int = item.get("id")
        if can_id_int is None:
            continue
        if 0x701 <= can_id_int <= 0x77F:
            heartbeat_nodes.append(can_id_int - 0x700)
        elif 0x181 <= can_id_int <= 0x1FF:
            tpdo1_nodes.append(can_id_int - 0x180)

    if heartbeat_nodes:
        nodes = sorted(set(heartbeat_nodes))
        method = "heartbeat (0x700+node_id)"
    else:
        nodes = sorted(set(tpdo1_nodes))
        method = "TPDO1 (0x180+node_id)"

    return {
        "detected_nodes": nodes,
        "count": len(nodes),
        "detection_method": method,
        "single_device": len(nodes) == 1,
    }


@app.route("/volkel_change_id", methods=["POST"])
def volkel_change_id():
    """Wijzig het CAN node ID van een Völkel ASB sensor via CANopen SDO."""
    current_node_raw = request.form.get("current_node_id", "").strip()
    new_node_raw = request.form.get("new_node_id", "").strip()

    try:
        current_id = int(current_node_raw)
        new_id = int(new_node_raw)
    except ValueError:
        return {"success": False, "error": "Ongeldig node ID"}, 400

    if not (1 <= current_id <= 127):
        return {"success": False, "error": "Huidig node ID moet 1–127 zijn"}, 400
    if not (1 <= new_id <= 127):
        return {"success": False, "error": "Nieuw node ID moet 1–127 zijn"}, 400

    channel = load_config_data().get("can_channel", "can0")
    sdo_cob = 0x600 + current_id

    # SDO schrijf object 3000h:02h (node ID), 1 byte
    write_cmd = f"cansend {channel} {sdo_cob:03X}#2F003002{new_id:02X}000000"
    # SDO sla op in EEPROM: object 1010h:01h = "save" (0x65766173 LE)
    save_cmd = f"cansend {channel} {sdo_cob:03X}#2310100173617665"
    # NMT reset node zodat nieuwe node ID actief wordt
    reset_cmd = f"cansend {channel} 000#81{current_id:02X}"

    outputs = []
    for cmd in [write_cmd, save_cmd, reset_cmd]:
        outputs.append(run_shell_command(cmd))
        time.sleep(0.15)

    return {
        "success": True,
        "commands": [write_cmd, save_cmd, reset_cmd],
        "output": "\n".join(outputs),
        "message": (
            f"Node ID gewijzigd van {current_id} naar {new_id}. "
            f"Sensor wordt herstart — nieuwe CAN IDs zijn actief na ~2 seconden."
        ),
    }


@app.route("/download_config")
def download_config():
    try:
        tmp = tempfile.NamedTemporaryFile(prefix="vg710_config_backup_", suffix=".zip", delete=False)
        tmp.close()
        zip_path = tmp.name

        with zipfile.ZipFile(zip_path, "w") as zipf:
            config_path = f"{BASE_DIR}/config.json"
            if os.path.exists(config_path):
                zipf.write(config_path, arcname="config.json")

            s3_path = f"{BASE_DIR}/s3.json"
            if os.path.exists(s3_path):
                zipf.write(s3_path, arcname="s3.json")

            cert_files = [
                ("device.pem.crt", "certs/device.pem.crt"),
                ("private.pem.key", "certs/private.pem.key"),
                ("AmazonRootCA1.pem", "certs/AmazonRootCA1.pem"),
            ]
            for filename, arcname in cert_files:
                full_path = os.path.join(CERT_DIR, filename)
                if os.path.exists(full_path):
                    zipf.write(full_path, arcname=arcname)

        return send_file(zip_path, as_attachment=True, download_name="vg710_config_backup.zip")
    except Exception as e:
        return f"Error creating backup: {e}", 500



@app.route("/gh_latest_version")
def gh_latest_version():
    with _gh_version_lock:
        tag = _gh_version_cache.get("tag_name")
    return {"tag_name": tag or "ophalen…"}


@app.route("/gh_update", methods=["POST"])
def gh_update():
    with _update_lock:
        if _update_status["running"]:
            return {"error": "Update al bezig"}, 409
        _update_status.update({"running": True, "log": [], "done": False, "success": None})
    threading.Thread(target=_run_update, daemon=True).start()
    return {"started": True}


@app.route("/gh_update_status")
def gh_update_status():
    with _update_lock:
        return dict(_update_status)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, threaded=True)