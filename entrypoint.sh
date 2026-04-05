#!/bin/sh

mkdir -p /data/vgapp/certs

echo "Starting Web UI..."
python /app/web.py &

echo "Waiting for config + certs..."

while true; do
  if [ -f /data/vgapp/config.json ] && \
     [ -f /data/vgapp/certs/device.pem.crt ] && \
     [ -f /data/vgapp/certs/private.pem.key ] && \
     [ -f /data/vgapp/certs/AmazonRootCA1.pem ]; then

    echo "All files present. Starting CAN → AWS app..."
    exec python /app/app.py
  fi

  sleep 3
done