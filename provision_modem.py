#!/usr/bin/env python3
"""
VG710 modem provisioning-tool (lokale Flask-app).

Voer een VG710-serienummer (device_id) + asset_id in en kies een
output-directory. De tool maakt in AWS IoT Core (eu-north-1) een thing aan,
genereert een actief X.509-certificaat, koppelt de bestaande `vg710-policy`
en het thing aan dat certificaat, en schrijft alle benodigde bestanden
(certs, root CA, config.json, cert_arn.txt) naar de gekozen directory.

Gebruik:
    .venv/bin/python provision_modem.py
    -> open http://127.0.0.1:5005 in de browser

Vereist: geldige AWS-credentials (env-vars of ~/.aws) met IoT-rechten op
account 203918854595.
"""

import json
import os

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from flask import Flask, redirect, request, url_for

# ---------------------------------------------------------------------------
# Vaste waarden (gedeeld door alle modems — zie modem 1/2/3)
# ---------------------------------------------------------------------------
REGION = "eu-north-1"
ACCOUNT_ID = "203918854595"
IOT_POLICY = "vg710-policy"

# Gedeelde config-template. device_id en asset_id worden per modem ingevuld.
CONFIG_TEMPLATE = {
    "device_id": None,
    "asset_id": None,
    "aws_endpoint": "a1c1nlllair6d5-ats.iot.eu-north-1.amazonaws.com",
    "mqtt_port": 8883,
    "mqtt_topic_prefix": "vg710",
    "heartbeat_interval_sec": 10,
    "can_channel": "can0",
    "s3_bucket": "bmc-vg710-raw-eun1",
    "s3_prefix": "vg710-raw",
    "s3_region": "eu-north-1",
    "s3_flush_interval_sec": 30,
    "s3_batch_size": 100,
    "ntrip": {
        "enabled": True,
        "host": "ntrip.lnrnet.nl",
        "port": 2101,
        "mountpoint": "",
        "username": "bmc-demo01",
        "password": "680391",
    },
    "septentrio": {
        "ip": "192.168.127.250",
        "port": 2101,
        "nmea_sources": [
            {"name": "nmea_tcp_5017", "host": "192.168.127.250", "port": 5017}
        ],
    },
}

# Amazon Root CA 1 (publiek, https://www.amazontrust.com/repository/AmazonRootCA1.pem)
AMAZON_ROOT_CA1 = """-----BEGIN CERTIFICATE-----
MIIDQTCCAimgAwIBAgITBmyfz5m/jAo54vB4ikPmljZbyjANBgkqhkiG9w0BAQsF
ADA5MQswCQYDVQQGEwJVUzEPMA0GA1UEChMGQW1hem9uMRkwFwYDVQQDExBBbWF6
b24gUm9vdCBDQSAxMB4XDTE1MDUyNjAwMDAwMFoXDTM4MDExNzAwMDAwMFowOTEL
MAkGA1UEBhMCVVMxDzANBgNVBAoTBkFtYXpvbjEZMBcGA1UEAxMQQW1hem9uIFJv
b3QgQ0EgMTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALJ4gHHKeNXj
ca9HgFB0fW7Y14h29Jlo91ghYPl0hAEvrAIthtOgQ3pOsqTQNroBvo3bSMgHFzZM
9O6II8c+6zf1tRn4SWiw3te5djgdYZ6k/oI2peVKVuRF4fn9tBb6dNqcmzU5L/qw
IFAGbHrQgLKm+a/sRxmPUDgH3KKHOVj4utWp+UhnMJbulHheb4mjUcAwhmahRWa6
VOujw5H5SNz/0egwLX0tdHA114gk957EWW67c4cX8jJGKLhD+rcdqsq08p8kDi1L
93FcXmn/6pUCyziKrlA4b9v7LWIbxcceVOF34GfID5yHI9Y/QCB/IIDEgEw+OyQm
jgSubJrIqg0CAwEAAaNCMEAwDwYDVR0TAQH/BAUwAwEB/zAOBgNVHQ8BAf8EBAMC
AYYwHQYDVR0OBBYEFIQYzIU07LwMlJQuCFmcx7IQTgoIMA0GCSqGSIb3DQEBCwUA
A4IBAQCY8jdaQZChGsV2USggNiMOruYou6r4lK5IpDB/G/wkjUu0yKGX9rbxenDI
U5PMCCjjmCXPI6T53iHTfIUJrU6adTrCC2qJeHZERxhlbI1Bjjt/msv0tadQ1wUs
N+gDS63pYaACbvXy8MWy7Vu33PqUXHeeE6V/Uq2V8viTO96LXFvKWlJbYK8U90vv
o/ufQJVtMVT8QtPHRh8jrdkPSHCa2XV4cdFyQzR1bldZwgJcJmApzyMZFo6IQ6XU
5MsI+yMRQ+hDKXJioaldXgjUkK642M4UwtBV8ob2xJNDd2ZhwLnoQdeXeGADbkpy
rqXRfboQnoZsG4q5WTP468SQvvG5
-----END CERTIFICATE-----
"""

app = Flask(__name__)

PAGE = """<!DOCTYPE html>
<html lang="nl"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>VG710 modem provisioning</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{ font-family: -apple-system, Segoe UI, Roboto, sans-serif; margin: 0;
         background: #0f172a; color: #e2e8f0; }}
  .wrap {{ max-width: 820px; margin: 0 auto; padding: 32px 20px 80px; }}
  h1 {{ font-size: 22px; margin: 0 0 4px; }}
  .sub {{ color: #94a3b8; margin: 0 0 28px; font-size: 14px; }}
  .card {{ background: #1e293b; border: 1px solid #334155; border-radius: 12px;
          padding: 22px; margin-bottom: 20px; }}
  label {{ display: block; font-weight: 600; margin: 14px 0 6px; font-size: 14px; }}
  .hint {{ font-weight: 400; color: #94a3b8; font-size: 12px; }}
  input[type=text] {{ width: 100%; padding: 10px 12px; border-radius: 8px;
         border: 1px solid #475569; background: #0f172a; color: #e2e8f0;
         font-size: 14px; font-family: ui-monospace, monospace; }}
  button {{ margin-top: 20px; background: #2563eb; color: #fff; border: 0;
           padding: 12px 22px; border-radius: 8px; font-size: 15px; font-weight: 600;
           cursor: pointer; }}
  button:hover {{ background: #1d4ed8; }}
  .ok {{ border-color: #16a34a; }}
  .err {{ border-color: #dc2626; }}
  .badge {{ display: inline-block; padding: 2px 10px; border-radius: 999px;
           font-size: 12px; font-weight: 700; }}
  .badge.ok {{ background: #14532d; color: #86efac; }}
  .badge.err {{ background: #7f1d1d; color: #fca5a5; }}
  table {{ width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 13px; }}
  td {{ padding: 7px 8px; border-bottom: 1px solid #334155; vertical-align: top; }}
  td.k {{ color: #94a3b8; white-space: nowrap; width: 1%; }}
  code {{ font-family: ui-monospace, monospace; background: #0f172a; padding: 1px 5px;
         border-radius: 4px; font-size: 12px; word-break: break-all; }}
  pre {{ background: #0f172a; border: 1px solid #334155; border-radius: 8px;
        padding: 14px; overflow-x: auto; font-size: 12.5px; line-height: 1.5; }}
  h2 {{ font-size: 16px; margin: 26px 0 8px; }}
  h3 {{ font-size: 14px; margin: 18px 0 6px; color: #cbd5e1; }}
  ol, ul {{ font-size: 13.5px; line-height: 1.7; padding-left: 22px; }}
  .step {{ color: #94a3b8; font-size: 12px; }}
  a {{ color: #60a5fa; }}
  .files li {{ font-family: ui-monospace, monospace; font-size: 12.5px; }}
</style></head>
<body><div class="wrap">
<h1>VG710 modem provisioning</h1>
<p class="sub">AWS IoT Core &middot; account {account} &middot; region {region}</p>
{body}
</div></body></html>
"""

FORM = """
<form method="post" action="{action}">
<div class="card">
  <label>VG710 serienummer <span class="hint">(device_id &mdash; tevens MQTT client-ID, moet uniek zijn)</span></label>
  <input type="text" name="device_id" placeholder="VF710..." value="{device_id}" required>

  <label>Asset ID <span class="hint">(4 letters + 4 cijfers, bv. NPDW1268)</span></label>
  <input type="text" name="asset_id" placeholder="NPDW1268" value="{asset_id}" required>

  <label>Output-directory <span class="hint">(waar certs + config.json worden weggeschreven; wordt aangemaakt indien nodig)</span></label>
  <input type="text" name="out_dir" placeholder="{default_dir}" value="{out_dir}" required>

  <button type="submit">Provision modem</button>
</div>
</form>
{error}
<div class="card">
<h2>Wat doet deze tool?</h2>
<ol>
  <li>Maakt een IoT <b>thing</b> aan met het serienummer als naam.</li>
  <li>Genereert een <b>actief X.509-certificaat</b> + private/public key.</li>
  <li>Koppelt de bestaande policy <code>{policy}</code> aan het certificaat.</li>
  <li>Koppelt het thing aan het certificaat.</li>
  <li>Schrijft <code>device.pem.crt</code>, <code>private.pem.key</code>,
      <code>public.pem.key</code>, <code>AmazonRootCA1.pem</code>,
      <code>config.json</code> en <code>cert_arn.txt</code> naar de directory.</li>
</ol>
<p class="step">Daarna toont de tool de Docker-instructies om de container op het modem aan te maken.</p>
</div>
"""


def render(body):
    return PAGE.format(account=ACCOUNT_ID, region=REGION, body=body)


def form_page(device_id="", asset_id="", out_dir="", error_html=""):
    home = os.path.expanduser("~")
    default = os.path.join(home, "vg710-modemX-certs")
    return render(
        FORM.format(
            action=url_for("provision"),
            device_id=device_id,
            asset_id=asset_id,
            out_dir=out_dir,
            default_dir=default,
            policy=IOT_POLICY,
            error=error_html,
        )
    )


@app.route("/")
def index():
    return form_page()


@app.route("/provision", methods=["POST"])
def provision():
    device_id = request.form.get("device_id", "").strip()
    asset_id = request.form.get("asset_id", "").strip()
    out_dir = os.path.expanduser(request.form.get("out_dir", "").strip())

    if not (device_id and asset_id and out_dir):
        return form_page(device_id, asset_id, out_dir,
                         _error("Vul alle velden in."))

    iot = boto3.client("iot", region_name=REGION)

    # Voorkom dubbele aanmaak: bestaat het thing al?
    try:
        iot.describe_thing(thingName=device_id)
        return form_page(device_id, asset_id, out_dir,
                         _error("Thing <code>%s</code> bestaat al in IoT Core. "
                                "Kies een ander serienummer of verwijder het bestaande thing eerst."
                                % device_id))
    except iot.exceptions.ResourceNotFoundException:
        pass
    except (ClientError, BotoCoreError) as e:
        return form_page(device_id, asset_id, out_dir,
                         _error("AWS-fout bij controle: %s" % e))

    steps = []
    cert_arn = None
    try:
        # 1. Thing
        thing = iot.create_thing(thingName=device_id)
        steps.append(("Thing aangemaakt", thing["thingArn"]))

        # 2. Cert + keys (actief)
        keys = iot.create_keys_and_certificate(setAsActive=True)
        cert_arn = keys["certificateArn"]
        steps.append(("Certificaat aangemaakt (actief)", cert_arn))

        # 3. Policy -> cert
        iot.attach_policy(policyName=IOT_POLICY, target=cert_arn)
        steps.append(("Policy gekoppeld", IOT_POLICY))

        # 4. Cert -> thing
        iot.attach_thing_principal(thingName=device_id, principal=cert_arn)
        steps.append(("Certificaat aan thing gekoppeld", device_id))

        # 5. Bestanden wegschrijven
        os.makedirs(out_dir, mode=0o700, exist_ok=True)
        written = _write_files(out_dir, device_id, asset_id, keys, cert_arn)
        steps.append(("Bestanden weggeschreven", out_dir))
    except (ClientError, BotoCoreError, OSError) as e:
        return form_page(device_id, asset_id, out_dir,
                         _error("Provisioning afgebroken: %s<br>"
                                "Reeds uitgevoerde stappen: %s"
                                % (e, ", ".join(s[0] for s in steps))))

    return render(result_page(device_id, asset_id, out_dir, cert_arn, steps, written))


def _write_files(out_dir, device_id, asset_id, keys, cert_arn):
    config = dict(CONFIG_TEMPLATE)
    config["device_id"] = device_id
    config["asset_id"] = asset_id

    files = {
        "device.pem.crt": (keys["certificatePem"], 0o600),
        "private.pem.key": (keys["keyPair"]["PrivateKey"], 0o600),
        "public.pem.key": (keys["keyPair"]["PublicKey"], 0o644),
        "AmazonRootCA1.pem": (AMAZON_ROOT_CA1, 0o644),
        "cert_arn.txt": (cert_arn + "\n", 0o644),
        "config.json": (json.dumps(config, indent=2) + "\n", 0o644),
    }
    for name, (content, mode) in files.items():
        path = os.path.join(out_dir, name)
        with open(path, "w") as f:
            f.write(content)
        os.chmod(path, mode)
    return list(files.keys())


def _error(msg):
    return '<div class="card err"><span class="badge err">FOUT</span> %s</div>' % msg


def result_page(device_id, asset_id, out_dir, cert_arn, steps, written):
    rows = "".join(
        '<tr><td class="k">%s</td><td><code>%s</code></td></tr>' % (k, v)
        for k, v in steps
    )
    files = "".join("<li>%s</li>" % f for f in written)
    cert_id = cert_arn.split("/")[-1]
    env_region = REGION

    docker = """
<div class="card">
<h2>Wat is er gedaan</h2>
<table>
  <tr><td class="k">device_id</td><td><code>{device_id}</code></td></tr>
  <tr><td class="k">asset_id</td><td><code>{asset_id}</code></td></tr>
  <tr><td class="k">cert ARN</td><td><code>{cert_arn}</code></td></tr>
  {rows}
</table>
<h3>Weggeschreven bestanden &mdash; <code>{out_dir}</code></h3>
<ul class="files">{files}</ul>
</div>

<div class="card">
<h2>Wat moet je nog doen: container op het modem</h2>
<p class="step">De productie-uitrol gebeurt via de <b>InHand Docker Manager</b> in de
modem-webinterface (poort 8080) &mdash; niet via docker-compose. Stappen:</p>

<h3>1. Image laden</h3>
<ul>
  <li>Docker Manager &rarr; <b>Images</b> &rarr; importeer de nieuwste
      <code>vg710-web-aws-vX.Y.Z.tar</code> (zelfde tar als modem 1/2).</li>
</ul>

<h3>2. Container aanmaken</h3>
<p class="step">Exact dezelfde instellingen als modem 1/2:</p>
<table>
  <tr><td class="k">Network mode</td><td><code>host</code> <span class="step">(vereist voor CAN-bus can0 + NTRIP-proxy poort 7791)</span></td></tr>
  <tr><td class="k">Privileged</td><td><code>aan</code></td></tr>
  <tr><td class="k">Restart policy</td><td><code>unless-stopped</code></td></tr>
  <tr><td class="k">Volume</td><td>named volume <code>vgdata</code> (local) &rarr; <code>/data/vgapp</code> <span class="step">(GEEN host bind mount, GEEN docker.sock)</span></td></tr>
  <tr><td class="k">Env-var</td><td><code>AWS_ACCESS_KEY_ID=&lt;vg710-uploader key&gt;</code></td></tr>
  <tr><td class="k">Env-var</td><td><code>AWS_SECRET_ACCESS_KEY=&lt;vg710-uploader secret&gt;</code></td></tr>
  <tr><td class="k">Env-var</td><td><code>AWS_DEFAULT_REGION={env_region}</code></td></tr>
</table>

<h3>3. Config + certs op het modem zetten</h3>
<p class="step">Plaats de zojuist weggeschreven bestanden in het named volume onder
<code>/data/vgapp/</code> op het modem:</p>
<ul class="files">
  <li>config.json</li>
  <li>device.pem.crt</li>
  <li>private.pem.key</li>
  <li>AmazonRootCA1.pem</li>
</ul>
<p class="step">De boot-gate (<code>entrypoint.sh</code>) start <code>app.py</code> pas
zodra <code>config.json</code> + de 3 certs aanwezig zijn. <code>web.py</code>
(poort 8080, default admin/admin) komt altijd meteen op.</p>

<h3>4. Container starten &amp; controleren</h3>
<ul>
  <li>Start de container; check in de webinterface (poort 8080) dat MQTT verbindt en
      GNSS/CAN data binnenkomt.</li>
  <li>Verifieer in AWS: thing <code>{device_id}</code> komt online; S3-objecten
      verschijnen onder <code>s3://bmc-vg710-raw-eun1/vg710-raw/</code> met dit device_id.</li>
</ul>
<p class="step">S3-bucket en IAM-user <code>vg710-uploader</code> worden gedeeld door alle
modems &mdash; daar hoef je niets aan te wijzigen. Alleen de IoT-identiteit (thing + cert)
is per modem uniek, en die is hierboven aangemaakt.</p>
</div>

<p><a href="/">&larr; Nog een modem provisionen</a></p>
""".format(
        device_id=device_id, asset_id=asset_id, cert_arn=cert_arn, rows=rows,
        out_dir=out_dir, files=files, env_region=env_region, cert_id=cert_id,
    )

    header = ('<div class="card ok"><span class="badge ok">GELUKT</span> '
              'Modem <code>%s</code> is geprovisioneerd in AWS IoT Core.</div>'
              % device_id)
    return header + docker


if __name__ == "__main__":
    print("VG710 provisioning-tool -> http://127.0.0.1:5005")
    app.run(host="127.0.0.1", port=5005, debug=False)
