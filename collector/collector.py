"""Reads alarms from Kafka and stores them in PostgreSQL."""
import json
import psycopg
import os
from confluent_kafka import Consumer

KAFKA = os.getenv("KAFKA", "localhost:9092")
TOPIC = "ran.alarms"
DB = os.getenv("DB", "postgresql://ranmon:ranmon@localhost:5432/ranmon")

SCHEMA = """
CREATE TABLE IF NOT EXISTS alarms (
    id          BIGSERIAL PRIMARY KEY,
    node        TEXT        NOT NULL,
    region      TEXT        NOT NULL,
    hub         TEXT        NOT NULL,
    alarm       TEXT        NOT NULL,
    severity    TEXT        NOT NULL,
    event       TEXT        NOT NULL CHECK (event IN ('raise', 'clear')),
    ts          TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS alarms_node_time ON alarms (node, ts);
"""

INSERT = """INSERT INTO alarms (node, region, hub, alarm, severity, event, ts)
            VALUES (%s, %s, %s, %s, %s, %s, %s)"""
            
consumer = Consumer({
    "bootstrap.servers": KAFKA,
    "group.id": "collector",
    "auto.offset.reset": "earliest",   # on first run, start from the oldest alarm
    "enable.auto.commit": False,       # only mark alarms as read once they're saved
    "allow.auto.create.topics": True,
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
                rows.append((e["node"], e["region"], e["hub"], e["alarm"], e["severity"], e["event"], e["ts"]))
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