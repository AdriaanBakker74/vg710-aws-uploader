import json
import os
import subprocess
import tempfile
import zipfile

from flask import Flask, redirect, render_template_string, request, send_file, url_for
from werkzeug.utils import secure_filename

app = Flask(__name__)

BASE_DIR = "/data/vgapp"
CERT_DIR = os.path.join(BASE_DIR, "certs")
os.makedirs(BASE_DIR, exist_ok=True)
os.makedirs(CERT_DIR, exist_ok=True)

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
        <p>Configuratie, certificaten, CAN upload rates, AWS-status en container shell in één overzicht.</p>
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
      </section>
    </div>

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
  </div>

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
          awsStatus.innerHTML = '<strong>AWS status</strong><div class="muted">' + data.aws_status_text + '</div>';
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
    return render_template_string(
        HTML,
        config=exists(f"{BASE_DIR}/config.json"),
        s3cfg=exists(f"{BASE_DIR}/s3.json"),
        crt=exists(f"{CERT_DIR}/device.pem.crt"),
        key=exists(f"{CERT_DIR}/private.pem.key"),
        ca=exists(f"{CERT_DIR}/AmazonRootCA1.pem"),
        can_ids=current_can_ids(),
        rate_rows=build_rate_rows(),
        aws_status_text=aws_status_text(),
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


@app.route("/shell", methods=["POST"])
def shell():
    preset = request.form.get("preset", "").strip()
    command = request.form.get("command", "").strip()

    if preset:
        command = shell_presets().get(preset, command)

    if not command:
        return render_page(shell_command="", shell_output="No command provided.")

    output = run_shell_command(command)
    return render_page(shell_command=command, shell_output=output)


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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)