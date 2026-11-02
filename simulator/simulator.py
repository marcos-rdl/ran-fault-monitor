import json, random, time
from datetime import datetime, timezone
from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:9092"})
NODES = [f"gNB-{i:04d}" for i in range(500)]
ALARMS = ["NODE_DOWN", "LINK_FLAP", "POWER_FAILURE", "HIGH_TEMPERATURE"]

sent = 0
try:
    while True:
        node = random.choice(NODES)
        event = {
            "node": node,
            "alarm": random.choice(ALARMS),
            "severity": random.choice(["minor", "major", "critical"]),
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        producer.produce("ran.alarms", key=node, value=json.dumps(event))
        producer.poll(0)
        sent += 1
        if sent % 50 == 0:
            print(f"{sent} alarms sent")
        time.sleep(random.uniform(0.05, 0.5))
except KeyboardInterrupt:
    producer.flush()
    print(f"Stopped after {sent} alarms")