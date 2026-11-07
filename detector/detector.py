"""Flags sites that fail far more often than the rest of the network.

Every 30 s it counts each site's faults in the last hour and computes a z-score:
how many standard deviations that site is above the network average.
A site is flagged when its z-score is at least Z_THRESHOLD and it had at least
MIN_FAULTS faults.

What counts as one fault:
  - each NODE_DOWN, POWER_FAILURE or HIGH_TEMPERATURE raised at the site
  - each flapping episode (repeated LINK_FLAPs a few seconds apart count once)
Not counted: outages caused by a hub failure (the correlator put them in the hub's
incident) and the NODE_DOWN that follows a POWER_FAILURE (same fault).
Faults are counted instead of incidents because the correlator keeps a site that
fails again right after recovering in the same incident.
"""
import time
import os
from datetime import datetime
import psycopg

DB = os.getenv("DB", "postgresql://ranmon:ranmon@localhost:5432/ranmon")
SITES = 500                  # every site counts, including the ones with no faults
WINDOW = "60 minutes"
Z_THRESHOLD = 3.5
MIN_FAULTS = 5               # avoids false alarms in the first minutes, when counts are tiny
CHECK_EVERY = 30             # seconds

SCHEMA = """
DROP TABLE IF EXISTS anomalies;     -- recalculated from scratch at every start
CREATE TABLE anomalies (
    site        TEXT         PRIMARY KEY,
    region      TEXT         NOT NULL,
    hub         TEXT         NOT NULL,
    faults      INT          NOT NULL,
    z_score     NUMERIC(5,1) NOT NULL,
    first_seen  TIMESTAMPTZ  NOT NULL,
    last_seen   TIMESTAMPTZ  NOT NULL,
    active      BOOLEAN      NOT NULL
);
"""

FIND = """
WITH raised AS (
    SELECT a.node, a.region, a.hub, a.alarm, a.ts,
           lag(a.ts) OVER (PARTITION BY a.node, a.alarm ORDER BY a.id) AS previous
    FROM alarms a JOIN incidents i ON i.id = a.incident_id
    WHERE a.event = 'raise' AND a.node LIKE 'gNB-%%'
      AND a.ts > now() - %(window)s::interval
      AND i.kind <> 'TRANSPORT_LINK_DOWN'                              -- hub's fault
      AND NOT (a.alarm = 'NODE_DOWN' AND i.kind = 'POWER_FAILURE')),   -- same fault
counts AS (
    SELECT node AS site, min(region) AS region, min(hub) AS hub,
           count(*) FILTER (WHERE alarm <> 'LINK_FLAP' OR previous IS NULL
                                  OR ts - previous > interval '30 seconds') AS n
    FROM raised
    GROUP BY node),
stats AS (
    SELECT sum(n)::float / %(sites)s AS mean,
           sqrt(greatest(sum(n * n)::float / %(sites)s - power(sum(n)::float / %(sites)s, 2), 0)) AS sd
    FROM counts)
SELECT c.site, c.region, c.hub, c.n, round(((c.n - s.mean) / s.sd)::numeric, 1) AS z, s.mean
FROM counts c, stats s
WHERE s.sd > 0 AND c.n >= %(min)s AND (c.n - s.mean) / s.sd >= %(z)s
ORDER BY z DESC
"""

SAVE = """
INSERT INTO anomalies (site, region, hub, faults, z_score, first_seen, last_seen, active)
VALUES (%s, %s, %s, %s, %s, now(), now(), true)
ON CONFLICT (site) DO UPDATE SET
    faults     = EXCLUDED.faults,
    z_score    = EXCLUDED.z_score,
    last_seen  = now(),
    first_seen = CASE WHEN anomalies.active THEN anomalies.first_seen ELSE now() END,
    active     = true
"""

params = {"window": WINDOW, "sites": SITES, "min": MIN_FAULTS, "z": Z_THRESHOLD}

with psycopg.connect(DB) as conn:
    conn.execute(SCHEMA)
    conn.commit()
    print(f"Detector running: flags sites with z >= {Z_THRESHOLD} over the last {WINDOW}. Ctrl+C to stop.")
    try:
        while True:
            was_active = {r[0] for r in conn.execute("SELECT site FROM anomalies WHERE active").fetchall()}
            rows = conn.execute(FIND, params).fetchall()
            for site, region, hub, n, z, mean in rows:
                conn.execute(SAVE, (site, region, hub, n, z))
                if site not in was_active:
                    print(f"ANOMALY   {site} ({hub}): {n} faults in the last hour, "
                          f"network average {mean:.1f}, z = {z}")
            flagged = [r[0] for r in rows]
            for (site,) in conn.execute("""UPDATE anomalies SET active = false
                                           WHERE active AND NOT (site = ANY(%s)) RETURNING site""",
                                        (flagged,)).fetchall():
                print(f"NORMAL    {site} is back to a normal failure rate")
            conn.commit()
            print(f"{datetime.now():%H:%M:%S}  checked, {len(flagged)} site(s) flagged")
            time.sleep(CHECK_EVERY)
    except KeyboardInterrupt:
        print("Detector stopped.")