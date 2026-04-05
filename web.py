import json
import os
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

  <h2>Backup</h2>
  <a href="/download_config">
    <button>Download Config + Certs</button>
  </a>

  <h2>Status</h2>
  <ul>
  <li>config.json: {{ config }}</li>
  <li>s3.json: {{ s3cfg }}</li>
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
        s3cfg=exists(f"{BASE_DIR}/s3.json"),
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
        "s3cfg": exists(f"{BASE_DIR}/s3.json"),
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
    if filename == "s3.json":
        uploaded.save(f"{BASE_DIR}/s3.json")
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


@app.route("/download_config")
def download_config():
    zip_path = "/tmp/vg710_config_backup.zip"

    try:
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

        return send_file(zip_path, as_attachment=True)
    except Exception as e:
        return f"Error creating backup: {e}", 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)