"""Reads alarms from Kafka and stores them in PostgreSQL."""
import json
import psycopg
from confluent_kafka import Consumer

KAFKA = "localhost:9092"
TOPIC = "ran.alarms"
DB = "postgresql://ranmon:ranmon@localhost:5432/ranmon"

SCHEMA = """
CREATE TABLE IF NOT EXISTS alarms (
    id          BIGSERIAL PRIMARY KEY,
    node        TEXT        NOT NULL,
    alarm       TEXT        NOT NULL,
    severity    TEXT        NOT NULL,
    raised_at   TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS alarms_node_time ON alarms (node, raised_at);
"""

INSERT = "INSERT INTO alarms (node, alarm, severity, raised_at) VALUES (%s, %s, %s, %s)"

consumer = Consumer({
    "bootstrap.servers": KAFKA,
    "group.id": "collector",
    "auto.offset.reset": "earliest",   # on first run, start from the oldest alarm
    "enable.auto.commit": False,       # only mark alarms as read once they're saved
})
consumer.subscribe([TOPIC])

with psycopg.connect(DB) as conn:
    conn.execute(SCHEMA)
    conn.commit()
    print("Connected. Waiting for alarms...")
    stored = 0
    try:
        while True:
            messages = consumer.consume(num_messages=100, timeout=1.0)
            rows = []
            for msg in messages:
                if msg.error():
                    print("Kafka error:", msg.error())
                    continue
                e = json.loads(msg.value())
                rows.append((e["node"], e["alarm"], e["severity"], e["ts"]))
            if rows:
                with conn.cursor() as cur:
                    cur.executemany(INSERT, rows)
                conn.commit()
                consumer.commit(asynchronous=False)
                stored += len(rows)
                print(f"{stored} alarms stored")
    except KeyboardInterrupt:
        print(f"Stopped after {stored} alarms")
    finally:
        consumer.close()