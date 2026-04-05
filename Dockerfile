FROM python:3.10-slim

ARG VERSION=dev
ENV APP_VERSION=$VERSION

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      iproute2 \
      can-utils \
      && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    flask \
    python-can \
    paho-mqtt \
    boto3

WORKDIR /app

COPY app.py /app/app.py
COPY web.py /app/web.py
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

EXPOSE 8080

CMD ["/entrypoint.sh"]