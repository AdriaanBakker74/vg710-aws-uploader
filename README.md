# VG710 AWS Uploader

Docker container voor de **Robustel VG710** industriële router. Leest CAN-busdata en NMEA-positiedata uit, publiceert naar **AWS IoT Core** via MQTT en slaat ruwe data op in **Amazon S3**. Biedt tevens een NTRIP-proxy voor de aangesloten Septentrio GNSS-ontvanger en een webgebaseerd configuratiepaneel.

Ontwikkeld door **Bakker Machine Control**.

---

## Functionaliteit

| Onderdeel | Omschrijving |
|---|---|
| CAN-bus uitlezen | Leest alle frames van `can0` via SocketCAN |
| AWS IoT MQTT | Publiceert CAN-frames en heartbeats naar AWS IoT Core (TLS) |
| S3 upload | Batcht ruwe CAN- en NMEA-data als NDJSON naar S3 |
| NTRIP-proxy | Septentrio verbindt als NTRIP-client; app haalt RTCM-correcties op bij externe caster |
| NMEA-lezer | Leest GGA/GSA/GST van de Septentrio via TCP voor positie en kwaliteitsdata |
| Web UI | Configuratiepaneel op poort 8080 |

---

## Architectuur

```
┌─────────────────────────────────────────────┐
│               VG710 Docker container         │
│                                             │
│  app.py                  web.py             │
│  ├─ CAN reader           ├─ Flask UI :8080  │
│  ├─ MQTT publisher       ├─ Config upload   │
│  ├─ S3 uploader          ├─ CAN live log    │
│  ├─ NTRIP proxy :7791    ├─ GNSS status     │
│  └─ NMEA reader          └─ Systeemstatus   │
│                                             │
│  Gedeelde status via /data/vgapp/*.json     │
└──────────┬──────────────────────┬───────────┘
           │                      │
     AWS IoT Core / S3      Septentrio GNSS
     (MQTT + NDJSON)        (NMEA + NTRIP)
```

### Processen

- **`app.py`** — hoofdproces: CAN, MQTT, S3, NTRIP-proxy, NMEA
- **`web.py`** — Flask webserver op poort 8080
- Communicatie via JSON-bestanden in `/data/vgapp/`

---

## Webinterface

Bereikbaar op `http://<VG710-IP>:8080`

- **Statusindicatoren** — config, certificaten, AWS IoT verbinding
- **GNSS positie** — fix type, coördinaten, hoogte, satellieten, DOP, nauwkeurigheid (1σ)
- **AWS IoT & S3 uploads** — live teller CAN- en NMEA-uploads
- **Systeemstatus** — CPU-gebruik, geheugen, load average
- **CAN berichten** — live scrollend venster met alle inkomende frames
- **NTRIP instellingen** — proxy-server config + upstream caster met mountpoint-ophaler
- **S3 upload instellingen** — flush-interval en batchgrootte per datatype
- **CAN upload rates** — per CAN ID instelbaar publicatie-interval
- **Container shell** — diagnose-commando's vanuit de browser

---

## Configuratie

### `config.json`

```json
{
  "device_id": "VF7102446015943",
  "asset_id": "CROW5943",
  "aws_endpoint": "<endpoint>.iot.<region>.amazonaws.com",
  "mqtt_port": 8883,
  "mqtt_topic_prefix": "vg710",
  "heartbeat_interval_sec": 10,
  "can_channel": "can0",
  "s3_bucket": "mijn-bucket",
  "s3_prefix": "vg710-raw",
  "s3_region": "eu-north-1",
  "s3_flush_interval_sec": 30,
  "s3_batch_size": 100,
  "ntrip": {
    "enabled": true,
    "host": "ntrip.example.com",
    "port": 2101,
    "mountpoint": "MOUNTPOINT",
    "username": "gebruiker",
    "password": "wachtwoord"
  },
  "ntrip_proxy": {
    "host": "0.0.0.0",
    "port": 7791,
    "username": "proxyuser",
    "password": "proxypass",
    "mountpoint": "proxymountpoint"
  },
  "septentrio": {
    "ip": "192.168.127.250",
    "nmea_sources": [
      { "name": "nmea_tcp_5017", "host": "192.168.127.250", "port": 5017 }
    ]
  }
}
```

### Certificaten

Plaats in `/data/vgapp/certs/`:

| Bestand | Omschrijving |
|---|---|
| `AmazonRootCA1.pem` | Amazon Root CA |
| `device.pem.crt` | Apparaatcertificaat |
| `private.pem.key` | Privésleutel |

Upload mogelijk via de webinterface of direct in het volume.

---

## Deployment

### Docker run

```bash
docker run -d \
  --name vg710-web-aws \
  --restart unless-stopped \
  --network host \
  -v /data/vgapp:/data/vgapp \
  --device /dev/can0 \
  vg710-web-aws:v1.x.x
```

> `--network host` zorgt dat de NTRIP-proxy op poort 7791 bereikbaar is voor de Septentrio.

### Volumes

| Host pad | Container pad | Inhoud |
|---|---|---|
| `/data/vgapp` | `/data/vgapp` | config, certificaten, statusbestanden |

---

## NTRIP-proxy

De Septentrio verbindt als NTRIP-client met de **proxy** op `<VG710-IP>:7791`. De app verbindt zelf met de externe caster en brug de RTCM-correcties door.

```
Septentrio ──NTRIP──► VG710:7791 (proxy) ──NTRIP──► Externe caster
                                          ◄── RTCM ──
```

- GGA-positie wordt automatisch meegestuurd bij verbinding (vereiste voor VRS-netwerken)
- GGA wordt elke 10 seconden herhaald
- Mountpoint instellen via de webinterface → NTRIP instellingen

---

## CI/CD

GitHub Actions bouwt automatisch een `linux/arm/v7` Docker image bij elke push naar `main`:

- Versienummer wordt automatisch opgehoogd (`v1.0.0` → `v1.0.1` → ...)
- Image wordt als `.tar` artifact opgeslagen (30 dagen bewaard)
- Versie is zichtbaar in de webinterface onder de paginatitel

---

## Vereisten

- Robustel VG710 (ARMv7) of compatibele hardware
- CAN-interface (`can0`) actief op het host-systeem
- AWS IoT Core thing met bijbehorende certificaten
- Docker op het apparaat (bijv. via Portainer)
