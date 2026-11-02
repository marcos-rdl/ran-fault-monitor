"""Groups related alarms into incidents.

Rules:
  1. A TRANSPORT_LINK_DOWN on a hub opens one hub incident. Every NODE_DOWN from a
     site behind that hub joins it, so a storm of 25 alarms becomes 1 incident.
     Site outages on that hub that started just before the hub alarm are merged in.
  2. Any other alarm joins the open incident of its site, if there is one
     (a NODE_DOWN after a POWER_FAILURE is the same problem), otherwise it opens one.
  3. A clear joins the incident of the alarm it clears.
  4. An incident closes when all its alarms are cleared and nothing new has
     arrived for HOLD_DOWN seconds (so a flapping link stays one incident).
"""
import time
from datetime import timedelta
import psycopg

DB = "postgresql://ranmon:ranmon@localhost:5432/ranmon"
POLL_SECONDS = 2
MERGE_WINDOW = timedelta(seconds=30)   # site outages this close before a hub failure belong to it
HOLD_DOWN = timedelta(seconds=60)

SCHEMA = """
CREATE TABLE IF NOT EXISTS incidents (
    id          BIGSERIAL PRIMARY KEY,
    kind        TEXT        NOT NULL,          -- alarm that opened it, e.g. NODE_DOWN
    root        TEXT        NOT NULL,          -- the site or hub at the origin
    region      TEXT        NOT NULL,
    hub         TEXT        NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'open',   -- open, closed or merged
    opened_at   TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL,
    closed_at   TIMESTAMPTZ,
    merged_into BIGINT REFERENCES incidents(id),
    alarm_count INT         NOT NULL DEFAULT 0,
    site_count  INT         NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS incidents_open ON incidents (status, root);
ALTER TABLE alarms ADD COLUMN IF NOT EXISTS incident_id BIGINT REFERENCES incidents(id);
CREATE TABLE IF NOT EXISTS correlator_state (last_alarm_id BIGINT NOT NULL);
INSERT INTO correlator_state SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM correlator_state);
"""

def one(cur, sql, *args):
    row = cur.execute(sql, args).fetchone()
    return row[0] if row else None

def open_incident(cur, a):
    inc = one(cur, """INSERT INTO incidents (kind, root, region, hub, opened_at, updated_at)
                      VALUES (%s, %s, %s, %s, %s, %s) RETURNING id""",
              a["alarm"], a["node"], a["region"], a["hub"], a["ts"], a["ts"])
    print(f"INC-{inc} opened   {a['alarm']} at {a['node']} ({a['hub']})")
    return inc

def attach(cur, inc, a):
    cur.execute("UPDATE alarms SET incident_id = %s WHERE id = %s", (inc, a["id"]))
    refresh(cur, inc, a["ts"])

def refresh(cur, inc, ts):
    cur.execute("""UPDATE incidents SET
                     updated_at  = GREATEST(updated_at, %s),
                     alarm_count = (SELECT count(*) FROM alarms WHERE incident_id = %s),
                     site_count  = (SELECT count(DISTINCT node) FROM alarms
                                    WHERE incident_id = %s AND node LIKE 'gNB-%%')
                   WHERE id = %s""", (ts, inc, inc, inc))

def handle(cur, a):
    if a["event"] == "clear":
        inc = one(cur, """SELECT incident_id FROM alarms
                          WHERE node = %s AND alarm = %s AND event = 'raise' AND incident_id IS NOT NULL
                          ORDER BY id DESC LIMIT 1""", a["node"], a["alarm"])
        if inc:
            attach(cur, inc, a)
        return

    if a["alarm"] == "TRANSPORT_LINK_DOWN":
        inc = open_incident(cur, a)
        attach(cur, inc, a)
        # site outages on this hub that started just before belong to this incident too
        for (child,) in cur.execute("""SELECT id FROM incidents
                                       WHERE status = 'open' AND hub = %s AND kind = 'NODE_DOWN'
                                         AND opened_at >= %s""",
                                    (a["hub"], a["ts"] - MERGE_WINDOW)).fetchall():
            cur.execute("UPDATE alarms SET incident_id = %s WHERE incident_id = %s", (inc, child))
            cur.execute("UPDATE incidents SET status = 'merged', merged_into = %s WHERE id = %s", (inc, child))
            print(f"INC-{child} merged into INC-{inc}")
        refresh(cur, inc, a["ts"])
        return

    if a["alarm"] == "NODE_DOWN":
        inc = one(cur, """SELECT id FROM incidents
                          WHERE status = 'open' AND kind = 'TRANSPORT_LINK_DOWN' AND hub = %s""", a["hub"])
        if inc:
            attach(cur, inc, a)
            return

    inc = one(cur, "SELECT id FROM incidents WHERE status = 'open' AND root = %s", a["node"])
    attach(cur, inc or open_incident(cur, a), a)

def close_finished(cur):
    for inc, opened in cur.execute("""
            SELECT i.id, i.opened_at FROM incidents i
            WHERE i.status = 'open' AND i.updated_at < now() - %s
              AND NOT EXISTS (                       -- no alarm still raised
                SELECT 1 FROM (SELECT DISTINCT ON (node, alarm) event FROM alarms
                               WHERE incident_id = i.id ORDER BY node, alarm, id DESC) last
                WHERE last.event = 'raise')""", (HOLD_DOWN,)).fetchall():
        cur.execute("UPDATE incidents SET status = 'closed', closed_at = updated_at WHERE id = %s", (inc,))
        print(f"INC-{inc} closed")

with psycopg.connect(DB, row_factory=psycopg.rows.dict_row) as conn:
    conn.execute(SCHEMA)
    conn.commit()
    print("Correlator running. Ctrl+C to stop.")
    try:
        while True:
            with conn.cursor() as cur:
                last = cur.execute("SELECT last_alarm_id FROM correlator_state").fetchone()["last_alarm_id"]
                new = cur.execute("""SELECT id, node, region, hub, alarm, event, ts FROM alarms
                                     WHERE id > %s ORDER BY id LIMIT 1000""", (last,)).fetchall()
                plain = conn.cursor(row_factory=psycopg.rows.tuple_row)
                for a in new:
                    handle(plain, a)
                if new:
                    plain.execute("UPDATE correlator_state SET last_alarm_id = %s", (new[-1]["id"],))
                close_finished(plain)
            conn.commit()
            time.sleep(POLL_SECONDS)
    except KeyboardInterrupt:
        print("Correlator stopped.")