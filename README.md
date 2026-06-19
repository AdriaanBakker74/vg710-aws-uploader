# VG710 AWS Uploader

Docker container voor de **Robuste VG710** industriële router. Leest CAN-busdata en NMEA-positiedata uit, publiceert naar **AWS IoT Core** via MQTT en slaat ruwe data op in **Amazon S3**. Biedt tevens een NTRIP-proxy voor de aangesloten Septentrio GNSS-ontvanger en een webgebaseerd configuratiepaneel.

Ontwikkeld door **Bakker Machine Control**.

---

## Functionaliteit

| Onderdeel | Omschrijving |
|---|---|
| CAN-bus uitlezen | Leest alle frames van `can0` via SocketCAN, met automatische herverbinding |
| CAN sensor groepen | Groepeert CAN IDs op bereik (bijv. 0x180–0x183) met naam en upload rate |
| AWS IoT MQTT | Publiceert CAN-frames en heartbeats naar AWS IoT Core (TLS) |
| S3 upload | Batcht ruwe CAN- en NMEA-data als NDJSON naar S3, gesamplede op een vast 1 Hz-rooster (sample-and-hold) |
| Sensor-activatie | Stuurt NMT Start zodat Völkel-sensoren data sturen; periodiek (instelbaar) of handmatig via knop |
| NTRIP-proxy | Septentrio verbindt als NTRIP-client; app haalt RTCM-correcties op bij externe caster |
| NMEA-lezer | Leest GGA/GSA/GST van de Septentrio via TCP voor positie en kwaliteitsdata |
| Web UI | Configuratiepaneel op poort 8080 |

---

## Architectuur

```
┌─────────────────────────────────────────────┐
│               VG710 Docker container        │
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

## Dataverwerking & S3-wegschrijven

Ruwe data wordt als **NDJSON** (één JSON-object per regel) in batches naar S3 geschreven. De pijplijn van bus → S3:

### 1. Sample-and-hold op een vast 1 Hz-rooster

Niet elke binnenkomende frame gaat naar S3 — dat zou de bucket vervuilen bij snelle sensoren. In plaats daarvan:

- **Per CAN-ID** houdt `app.py` één variabele bij met de **laatst ontvangen frame** (`S3_CAN_PENDING`). Elke nieuwe frame overschrijft de vorige.
- Een aparte sampler-thread (`can_s3_sampler_loop`) tikt op een **vast rooster** (default 1 Hz). Op elke tik wordt per ID de meest recente waarde gepakt, naar S3 geschreven en de buffer **gereset**.
- Komt er in een interval geen nieuwe frame? Dan wordt er voor die ID niets verstuurd (geen herhaling van oude data).

> Voorbeeld: sensor stuurt op t=0,6 / 1,2 / 1,8 → S3 krijgt op t=1 de waarde van t=0,6 en op t=2 de waarde van t=1,8. Records staan op een net, niet op de sensorfase.

Een **tragere** per-ID- of per-groep-rate (`upload_rate_sec` / `can_upload_rates`) blijft gelden en krijgt voorrang op de 1 Hz-basis.

### 2. NMEA tijd-uitgelijnd met CAN

NMEA-zinnen gebruiken dezelfde sample-and-hold: per zin-type (`GNGGA`, `GNHDT`, …) wordt de laatste zin onthouden en **op dezelfde tik** als CAN gecaptured. Daardoor zijn CAN- en NMEA-records in S3 tijd-uitgelijnd. Live-status en NTRIP verwerken nog wél elke binnenkomende zin.

### 3. Busmanagement-frames worden uitgesloten

Commando's op de CAN-bus zijn geen sensordata en gaan **niet** naar S3:

- **NMT-broadcasts** (`0x000`) — o.a. de sensor-activatie, pre-operational, reset
- **SDO-requests** (`0x600`–`0x67F`) — node-ID- en baudrate-wijzigingen

Deze blijven wel zichtbaar in de live CAN-log voor diagnose.

### 4. Batching naar S3

- Records worden gebufferd en geflusht bij `s3_batch_size` records óf na `s3_flush_interval_sec` seconden (apart instelbaar voor CAN en NMEA).
- Bij geen netwerk worden batches op schijf gequeued (`/data/vgapp/s3_queue`) en later geüpload.

### Apparaatregel (device-registry)

`app.py` schrijft één regel naar een vaste S3-key:

```
{S3_PREFIX}/devices/{device_id}.njson
```

De vorige regel wordt steeds **overschreven** (geen historie), zodat de bucket altijd één actuele regel per device bevat:

```json
{"device_id":"VF710...","asset_id":"CROW...","app_version":"v1.x.x","lat":52.1,"lon":5.2,"ts":"2026-06-19T09:41:00+00:00"}
```

Wanneer geschreven:

- **Bij opstart** — direct (lat/lon nog `null` als er nog geen fix is).
- **Bij de eerste GPS-fix** — opnieuw, nu mét `lat`/`lon`.
- **Daarna elke 5 minuten** — met de actuele positie.

### Sensor-activatie (NMT Start)

Völkel CANopen-sensoren sturen pas data in de **Operational** state. `app.py` stuurt daarom NMT Start (`000#0100`):

- **Eenmalig** bij bus-bring-up (sensoren actief bij aanschakelen).
- **Periodiek** elke `nmt_autostart_interval_sec` (default 30s) — aan/uit via de toggle `nmt_autostart_enabled` in de webinterface. Deze wordt **live** uit `config.json` gelezen, dus werkt zonder container-herstart.
- **Handmatig** via de knop "Sensoren activeren" in de webinterface.

---

## Webinterface

Bereikbaar op `http://<VG710-IP>:8080`

- **Statusindicatoren** — config, certificaten, AWS IoT verbinding
- **GNSS positie** — fix type, coördinaten, hoogte, satellieten, DOP, nauwkeurigheid (1σ)
- **AWS IoT & S3 uploads** — live teller CAN- en NMEA-uploads
- **Systeemstatus** — CPU-gebruik, geheugen, load average
- **CAN interface beheer** — baudrate wijzigen, interface aan/uitzetten en status opvragen vanuit de browser
- **CAN sensor groepen** — groepen op ID-bereik met naam (bijv. Temperatuursensor) en upload rate; groepnaam zichtbaar bij detected IDs
- **Völkel sensoren** — detecteren, node-ID en baudrate wijzigen (CANopen SDO), handmatig **Sensoren activeren** (NMT Start) en toggle voor periodieke auto-activatie
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
  "nmt_autostart_enabled": true,
  "nmt_autostart_interval_sec": 30,
  "can_sensor_groups": [
    {
      "name": "Temperatuursensor",
      "id_start": "0x180",
      "id_end": "0x183",
      "upload_rate_sec": 10
    },
    {
      "name": "Afstandsensor",
      "id_start": "",
      "id_end": "",
      "upload_rate_sec": 10
    }
  ],
  "s3_bucket": "mijn-bucket",
  "s3_prefix": "vg710-raw",
  "s3_region": "eu-north-1",
  "s3_flush_interval_sec": 30,
  "s3_batch_size": 100,
  "s3_can_min_interval_sec": 1.0,
  "s3_nmea_min_interval_sec": 1.0,
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
