"""Serves the live city map and pushes the network state to it.
 
One background task reads PostgreSQL every 2 seconds. Every open browser tab
gets that same snapshot over a WebSocket, so 1 tab or 50 tabs cost the
database the same.
 
  GET /                 the map page (static/index.html)
  GET /api/state        the latest snapshot as JSON
  GET /api/site/{node}  the last alarms of one site, for the side card
  WS  /ws               the snapshot, pushed every 2 seconds
"""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
 
import psycopg
from fastapi import FastAPI, WebSocket
from fastapi.responses import FileResponse, HTMLResponse
from psycopg.rows import dict_row
 
DB = "postgresql://ranmon:ranmon@localhost:5432/ranmon"
PUSH_SECONDS = 2
PAGE = Path(__file__).parent / "static" / "index.html"
 
# The last event of every (node, alarm) pair. If it is a raise, that alarm is still active.
ACTIVE = """
SELECT node, alarm, severity, incident_id FROM (
    SELECT DISTINCT ON (node, alarm) node, alarm, severity, event, incident_id
    FROM alarms ORDER BY node, alarm, id DESC
) last
WHERE event = 'raise'
"""
OPEN_INCIDENTS = """
SELECT id, kind, root, region, hub, alarm_count, site_count,
       EXTRACT(EPOCH FROM now() - opened_at)::int AS age
FROM incidents WHERE status = 'open'
ORDER BY site_count DESC, opened_at
"""
FEED = "SELECT node, alarm, severity, event, ts FROM alarms ORDER BY id DESC LIMIT 18"
LAST_HOUR = """
SELECT (SELECT count(*) FROM alarms WHERE event = 'raise' AND ts > now() - interval '1 hour') AS alarms,
       (SELECT count(*) FROM incidents WHERE status <> 'merged' AND opened_at > now() - interval '1 hour') AS incidents
"""
 
snapshot = {"ok": False, "error": "starting up"}
 
 
async def read_snapshot(conn):
    async with conn.cursor() as cur:
        active = await (await cur.execute(ACTIVE)).fetchall()
        incidents = await (await cur.execute(OPEN_INCIDENTS)).fetchall()
        feed = await (await cur.execute(FEED)).fetchall()
        last_hour = await (await cur.execute(LAST_HOUR)).fetchone()
 
    sites, hubs_down = {}, []
    for a in active:
        if a["alarm"] == "TRANSPORT_LINK_DOWN":       # raised by the hub itself
            hubs_down.append(a["node"])
            continue
        site = sites.setdefault(a["node"], {"state": "degraded", "alarms": [], "incident": None})
        site["alarms"].append(a["alarm"])
        if a["alarm"] == "NODE_DOWN":
            site["state"] = "down"
        if a["incident_id"]:
            site["incident"] = a["incident_id"]
 
    return {
        "ok": True,
        "time": datetime.now(timezone.utc).isoformat(),
        "sites": sites,                                # only sites with an active alarm; the rest are up
        "hubs_down": hubs_down,
        "incidents": incidents,
        "feed": [{**f, "ts": f["ts"].isoformat()} for f in feed],
        "last_hour": last_hour,
    }
 
 
async def poll_database():
    global snapshot
    while True:
        try:
            async with await psycopg.AsyncConnection.connect(DB, autocommit=True, row_factory=dict_row) as conn:
                while True:
                    snapshot = await read_snapshot(conn)
                    await asyncio.sleep(PUSH_SECONDS)
        except psycopg.Error as e:
            print(f"Database problem, retrying in 5 s: {e}")
            snapshot = {"ok": False, "error": str(e).strip()}
            await asyncio.sleep(5)
 
 
@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(poll_database())
    yield
    task.cancel()
 
 
app = FastAPI(title="RAN Fault Monitor map", lifespan=lifespan)
 
 
@app.get("/")
async def page():
    if not PAGE.exists():
        return HTMLResponse("The map page is not there yet: add mapserver/static/index.html. "
                            "Meanwhile, look at <a href='/api/state'>/api/state</a>.")
    return FileResponse(PAGE)
 
 
@app.get("/api/state")
async def state():
    return snapshot
 
 
@app.get("/api/site/{node}")
async def site(node: str):
    async with await psycopg.AsyncConnection.connect(DB, row_factory=dict_row) as conn:
        rows = await (await conn.execute(
            "SELECT alarm, severity, event, ts, incident_id FROM alarms WHERE node = %s ORDER BY id DESC LIMIT 6",
            (node,))).fetchall()
    return [{**r, "ts": r["ts"].isoformat()} for r in rows]
 
 
@app.websocket("/ws")
async def push(socket: WebSocket):
    await socket.accept()
    try:
        while True:
            await socket.send_json(snapshot)
            await asyncio.sleep(PUSH_SECONDS)
    except Exception:          # the browser tab was closed or reloaded
        pass
 