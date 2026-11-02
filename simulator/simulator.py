"""Simulates alarms from 500 base stations (gNBs) and sends them to Kafka.

The network: 5 regions, each with 4 transport hubs, each hub serving 25 sites.
Faults: single-site outages, power failures (sites run on battery, then may go down),
flapping links, high temperature, and hub failures that take down all 25 sites
behind them at once (an alarm storm). Every fault is cleared when it recovers.
"""
import heapq, itertools, json, random, time
from datetime import datetime, timezone
from confluent_kafka import Producer

TOPIC = "ran.alarms"
REGIONS = ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"]
HUBS_PER_REGION = 4
SITES_PER_HUB = 25                  # 5 x 4 x 25 = 500 sites

# Average seconds between new faults, across the whole network
EVERY = {"outage": 8, "power": 30, "flap": 15, "temperature": 20, "hub": 240}
FIRST_HUB_FAILURE = 60              # seconds after start, so you see a storm early
FLAKY_SITES = 5                     # a few bad sites that fail far more often than the rest
FLAKY_FACTOR = 15

producer = Producer({"bootstrap.servers": "localhost:9092"})

# ---------- the network ----------
sites = []
for region in REGIONS:
    for h in range(1, HUBS_PER_REGION + 1):
        hub = f"HUB-{region[0]}{h}"
        for _ in range(SITES_PER_HUB):
            sites.append({"node": f"gNB-{len(sites):04d}", "region": region, "hub": hub, "busy": False})
hubs = {s["hub"]: s["region"] for s in sites}
hubs_down = set()
flaky = set(random.sample(range(len(sites)), FLAKY_SITES))

# ---------- scheduling things for later ----------
queue, order = [], itertools.count()

def later(delay, fn, *args):
    heapq.heappush(queue, (time.monotonic() + delay, next(order), fn, args))

# ---------- sending alarms ----------
def send(node, region, hub, alarm, severity, event="raise"):
    msg = {
        "node": node, "region": region, "hub": hub,
        "alarm": alarm, "severity": severity, "event": event,
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    producer.produce(TOPIC, key=node, value=json.dumps(msg))
    producer.poll(0)
    print(f"{msg['ts'][11:19]}  {event:5}  {severity:8}  {alarm:19}  {node} ({hub})")

def alarm(site, name, severity, event="raise"):
    send(site["node"], site["region"], site["hub"], name, severity, event)

def pick_site():
    """A random site that has no fault right now. Flaky sites are picked more often."""
    free = [i for i, s in enumerate(sites) if not s["busy"]]
    if not free:
        return None
    weights = [FLAKY_FACTOR if i in flaky else 1 for i in free]
    site = sites[random.choices(free, weights)[0]]
    site["busy"] = True
    return site

def recover(site, name, severity):
    alarm(site, name, severity, "clear")
    site["busy"] = False

# ---------- the faults ----------
def outage():
    if s := pick_site():
        alarm(s, "NODE_DOWN", "critical")
        later(random.uniform(20, 180), recover, s, "NODE_DOWN", "critical")

def power():
    if s := pick_site():
        alarm(s, "POWER_FAILURE", "major")
        battery, restored = random.uniform(15, 60), random.uniform(30, 150)
        went_down = battery < restored       # the battery ran out before power came back
        if went_down:
            later(battery, alarm, s, "NODE_DOWN", "critical")
        later(restored, power_back, s, went_down)

def power_back(s, went_down):
    if went_down:
        alarm(s, "NODE_DOWN", "critical", "clear")
    recover(s, "POWER_FAILURE", "major")

def flap():
    if s := pick_site():
        t = 0
        for _ in range(random.randint(3, 8)):
            later(t, alarm, s, "LINK_FLAP", "minor")
            later(t + random.uniform(1, 3), alarm, s, "LINK_FLAP", "minor", "clear")
            t += random.uniform(4, 10)
        later(t, lambda: s.update(busy=False))

def temperature():
    if s := pick_site():
        alarm(s, "HIGH_TEMPERATURE", "major")
        later(random.uniform(60, 300), recover, s, "HIGH_TEMPERATURE", "major")

def hub_failure():
    up = [h for h in hubs if h not in hubs_down]
    if not up:
        return
    hub = random.choice(up)
    hubs_down.add(hub)
    send(hub, hubs[hub], hub, "TRANSPORT_LINK_DOWN", "critical")
    behind = [s for s in sites if s["hub"] == hub and not s["busy"]]
    for s in behind:
        s["busy"] = True
        later(random.uniform(0, 3), alarm, s, "NODE_DOWN", "critical")
    later(random.uniform(60, 180), hub_back, hub, behind)

def hub_back(hub, behind):
    send(hub, hubs[hub], hub, "TRANSPORT_LINK_DOWN", "critical", "clear")
    for s in behind:
        later(random.uniform(0, 5), recover, s, "NODE_DOWN", "critical")
    hubs_down.discard(hub)

FAULTS = {"outage": outage, "power": power, "flap": flap, "temperature": temperature, "hub": hub_failure}

# ---------- main loop ----------
TICK = 0.1
print(f"Simulating {len(sites)} sites behind {len(hubs)} hubs. Flaky sites: "
      + ", ".join(sites[i]["node"] for i in sorted(flaky)))
later(FIRST_HUB_FAILURE, hub_failure)
try:
    while True:
        for name, fault in FAULTS.items():
            if random.random() < TICK / EVERY[name]:
                fault()
        while queue and queue[0][0] <= time.monotonic():
            _, _, fn, args = heapq.heappop(queue)
            fn(*args)
        producer.poll(0)
        time.sleep(TICK)
except KeyboardInterrupt:
    producer.flush()
    print("Simulator stopped.")