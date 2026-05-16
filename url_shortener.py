import hashlib, logging, string, time, os, json
from typing import List, Dict, Optional, Tuple
from flask import Flask, request, redirect, jsonify, render_template

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Database Layer — PostgreSQL on Render, in-memory locally
# ──────────────────────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL", "")
USE_DB = bool(DATABASE_URL)

if USE_DB:
    import psycopg2
    from psycopg2.extras import RealDictCursor

# In-memory fallback stores
_mem_urls = []
_mem_events = []
_mem_stats = {"cdn_hits": 0, "cdn_misses": 0,
              "backend1_requests": 0, "backend2_requests": 0, "backend3_requests": 0}

def get_db():
    url = DATABASE_URL
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url, cursor_factory=RealDictCursor)

def init_db():
    conn = get_db(); cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS urls (
            id SERIAL PRIMARY KEY, short_code VARCHAR(10) UNIQUE NOT NULL,
            target_url TEXT NOT NULL, primary_node VARCHAR(50) NOT NULL,
            replicas TEXT NOT NULL DEFAULT '[]', backend_server VARCHAR(50),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW());
        CREATE TABLE IF NOT EXISTS event_logs (
            id SERIAL PRIMARY KEY, message TEXT NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW());
        CREATE TABLE IF NOT EXISTS system_stats (
            key VARCHAR(50) PRIMARY KEY, value INTEGER NOT NULL DEFAULT 0);
        INSERT INTO system_stats (key, value) VALUES
            ('cdn_hits',0),('cdn_misses',0),('backend1_requests',0),
            ('backend2_requests',0),('backend3_requests',0)
        ON CONFLICT (key) DO NOTHING;
    """)
    conn.commit(); cur.close(); conn.close()
    logger.info("[DB] PostgreSQL tables initialized")

def db_save_url(short_code, target_url, primary_node, replicas, backend):
    if USE_DB:
        conn = get_db(); cur = conn.cursor()
        cur.execute("INSERT INTO urls (short_code,target_url,primary_node,replicas,backend_server) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (short_code) DO NOTHING",
                    (short_code, target_url, primary_node, json.dumps(replicas), backend))
        conn.commit(); cur.close(); conn.close()
    _mem_urls.insert(0, {"short": short_code, "long": target_url, "primary": primary_node, "replicas": replicas, "backend": backend})

def db_get_url(short_code):
    if USE_DB:
        conn = get_db(); cur = conn.cursor()
        cur.execute("SELECT target_url FROM urls WHERE short_code = %s", (short_code,))
        row = cur.fetchone(); cur.close(); conn.close()
        if row: return row["target_url"]
    for u in _mem_urls:
        if u["short"] == short_code: return u["long"]
    return None

def db_get_all_urls():
    if USE_DB:
        conn = get_db(); cur = conn.cursor()
        cur.execute("SELECT short_code,target_url,primary_node,replicas,backend_server FROM urls ORDER BY created_at DESC LIMIT 100")
        rows = cur.fetchall(); cur.close(); conn.close()
        return [{"short": r["short_code"], "long": r["target_url"], "primary": r["primary_node"],
                 "replicas": json.loads(r["replicas"]), "backend": r["backend_server"]} for r in rows]
    return _mem_urls[:100]

def db_log_event(msg):
    ts = time.strftime('%H:%M:%S')
    entry = f"[{ts}] {msg}"
    if USE_DB:
        conn = get_db(); cur = conn.cursor()
        cur.execute("INSERT INTO event_logs (message) VALUES (%s)", (msg,))
        conn.commit(); cur.close(); conn.close()
    _mem_events.append(entry)
    if len(_mem_events) > 200: _mem_events.pop(0)

def db_get_events(limit=80):
    if USE_DB:
        conn = get_db(); cur = conn.cursor()
        cur.execute("SELECT message, created_at FROM event_logs ORDER BY id DESC LIMIT %s", (limit,))
        rows = cur.fetchall(); cur.close(); conn.close()
        return [f"[{r['created_at'].strftime('%H:%M:%S')}] {r['message']}" for r in reversed(rows)]
    return _mem_events[-limit:]

def db_increment_stat(key, amount=1):
    _mem_stats[key] = _mem_stats.get(key, 0) + amount
    if USE_DB:
        conn = get_db(); cur = conn.cursor()
        cur.execute("UPDATE system_stats SET value = value + %s WHERE key = %s", (amount, key))
        conn.commit(); cur.close(); conn.close()

def db_get_stats():
    if USE_DB:
        conn = get_db(); cur = conn.cursor()
        cur.execute("SELECT key, value FROM system_stats")
        rows = cur.fetchall(); cur.close(); conn.close()
        return {r["key"]: r["value"] for r in rows}
    return dict(_mem_stats)

# ──────────────────────────────────────────────
# Consistent Hash Ring
# ──────────────────────────────────────────────
class ConsistentHashRing:
    def __init__(self, virtual_nodes=3):
        self.virtual_nodes = virtual_nodes
        self.keys: List[int] = []
        self.ring: Dict[int, "StorageNode"] = {}

    @staticmethod
    def _hash(key: str) -> int:
        return int(hashlib.sha256(key.encode()).hexdigest(), 16)

    def add_node(self, node):
        for i in range(self.virtual_nodes):
            h = self._hash(f"{node.name}:{i}")
            self.keys.append(h); self.ring[h] = node
        self.keys.sort()

    def get_nodes(self, key: str, count: int):
        if not self.keys: return []
        h = self._hash(key)
        idx = 0
        for i, k in enumerate(self.keys):
            if h <= k: idx = i; break
        selected, seen, start = [], set(), idx
        while len(selected) < count:
            node = self.ring[self.keys[idx]]
            if node.name not in seen:
                selected.append(node); seen.add(node.name)
            idx = (idx + 1) % len(self.keys)
            if idx == start: break
        return selected

# ──────────────────────────────────────────────
# Storage Node
# ──────────────────────────────────────────────
class StorageNode:
    def __init__(self, name):
        self.name = name; self.data: Dict[str, str] = {}; self.is_active = True

    def store(self, short, long, replicas):
        if not self.is_active: raise RuntimeError(f"Node {self.name} is down!")
        self.data[short] = long
        for r in replicas:
            if r.name != self.name:
                try: r.replicate(short, long)
                except: pass

    def replicate(self, short, long):
        if not self.is_active: raise RuntimeError(f"Node {self.name} is down!")
        self.data[short] = long

    def get(self, short):
        if not self.is_active: raise RuntimeError(f"Node {self.name} is down!")
        return self.data.get(short)

    def fail(self): self.is_active = False
    def recover(self): self.is_active = True

# ──────────────────────────────────────────────
# Backend Server
# ──────────────────────────────────────────────
ALPHABET = string.ascii_lowercase + string.digits

def generate_short(long_url):
    h = hashlib.sha256(long_url.encode()).hexdigest()
    code, num = "", int(h[:12], 16)
    while num and len(code) < 7:
        num, rem = divmod(num, len(ALPHABET)); code += ALPHABET[rem]
    return code or h[:7]

class BackendServer:
    def __init__(self, name, ring, rf=3):
        self.name = name; self.ring = ring; self.rf = rf; self.requests_handled = 0
        self._stat_key = name.lower().replace("-", "") + "_requests"

    def shorten(self, long_url):
        self.requests_handled += 1; db_increment_stat(self._stat_key)
        short = generate_short(long_url)
        nodes = self.ring.get_nodes(short, self.rf)
        if not nodes: raise RuntimeError("No storage nodes available")
        primary, replicas = nodes[0], nodes[1:]
        primary.store(short, long_url, replicas)
        db_save_url(short, long_url, primary.name, [r.name for r in replicas], self.name)
        return {"short": short, "primary": primary.name, "replicas": [r.name for r in replicas],
                "backend": self.name, "storage_node": primary.name}

    def resolve(self, short):
        self.requests_handled += 1; db_increment_stat(self._stat_key)
        nodes = self.ring.get_nodes(short, self.rf)
        for n in nodes:
            try:
                result = n.get(short)
                if result: return result, n.name
            except: pass
        url = db_get_url(short)
        if url:
            for n in nodes:
                if n.is_active: n.data[short] = url
            return url, nodes[0].name if nodes else "DB-Direct"
        return None, None

# ──────────────────────────────────────────────
# Load Balancer
# ──────────────────────────────────────────────
class LoadBalancer:
    def __init__(self, backends):
        self.backends = backends; self._idx = 0

    def _next(self):
        b = self.backends[self._idx]
        self._idx = (self._idx + 1) % len(self.backends); return b

    def shorten(self, long_url): return self._next().shorten(long_url)

    def resolve(self, short):
        backend = self._next()
        result, storage_node = backend.resolve(short)
        return result, storage_node, backend.name

# ──────────────────────────────────────────────
# CDN Cache
# ──────────────────────────────────────────────
class CDNCache:
    def __init__(self, load_balancer, ttl=60):
        self.lb = load_balancer; self.ttl = ttl
        self.cache: Dict[str, Dict] = {}

    def get(self, short):
        now = time.time(); trace = []
        if short in self.cache and (now - self.cache[short]['time']) < self.ttl:
            db_increment_stat("cdn_hits")
            trace.append({"step": "CDN", "status": "HIT", "color": "green"})
            return self.cache[short]['url'], {"trace": trace}
        db_increment_stat("cdn_misses")
        trace.append({"step": "CDN", "status": "MISS", "color": "orange"})
        long_url, storage_node, backend_name = self.lb.resolve(short)
        trace.append({"step": "LB", "status": f"→ {backend_name}", "color": "purple"})
        if long_url:
            trace.append({"step": "Backend", "status": f"→ {storage_node}", "color": "cyan"})
            self.cache[short] = {'url': long_url, 'time': now}
        else:
            trace.append({"step": "Backend", "status": "NOT FOUND", "color": "red"})
        return long_url, {"trace": trace}

    def purge(self): self.cache.clear()

    def get_stats(self):
        now = time.time()
        active = {k: v for k, v in self.cache.items() if (now - v['time']) < self.ttl}
        stats = db_get_stats()
        return {"hits": stats.get("cdn_hits", 0), "misses": stats.get("cdn_misses", 0),
                "items": len(active),
                "entries": [{"short": k, "long": v['url'], "expires_in": int(self.ttl - (now - v['time']))} for k, v in active.items()]}

# ──────────────────────────────────────────────
# System Bootstrap
# ──────────────────────────────────────────────
storage_nodes = [StorageNode("Storage-A"), StorageNode("Storage-B"), StorageNode("Storage-C")]
ring = ConsistentHashRing(virtual_nodes=3)
for sn in storage_nodes: ring.add_node(sn)

backends = [BackendServer("Backend-1", ring), BackendServer("Backend-2", ring), BackendServer("Backend-3", ring)]
lb = LoadBalancer(backends)
cdn = CDNCache(lb, ttl=60)
trace_log: List[dict] = []

def log_event(msg): db_log_event(msg)

def add_trace(url, trace, is_shorten=False):
    trace_log.insert(0, {"time": time.strftime('%H:%M:%S'), "url": url,
                         "type": "SHORTEN" if is_shorten else "RESOLVE", "path": trace})
    if len(trace_log) > 10: trace_log.pop()

# ──────────────────────────────────────────────
# Flask Application
# ──────────────────────────────────────────────
app = Flask(__name__)

if USE_DB:
    with app.app_context():
        init_db()
        for row in db_get_all_urls():
            nodes = ring.get_nodes(row["short"], 3)
            for n in nodes: n.data[row["short"]] = row["long"]
        stats = db_get_stats()
        for b in backends: b.requests_handled = stats.get(b._stat_key, 0)
        logger.info("[BOOT] Reloaded state from PostgreSQL")
else:
    logger.info("[BOOT] Running in LOCAL MODE (no PostgreSQL)")

@app.route("/")
def index(): return render_template("index.html")

@app.route("/api/shorten", methods=["POST"])
def api_shorten():
    data = request.get_json(force=True)
    long_url = data.get("url", "").strip()
    if not long_url: return jsonify({"error": "URL is required"}), 400
    if not long_url.startswith(("http://", "https://")): long_url = "https://" + long_url
    try:
        info = lb.shorten(long_url)
        log_event(f"SHORTEN  {info['short']} → {long_url}  | primary={info['primary']} replicas={info['replicas']}")
        trace = [{"step": "CDN", "status": "BYPASS (Write)", "color": "orange"},
                 {"step": "LB", "status": f"→ {info['backend']}", "color": "purple"},
                 {"step": "Backend", "status": f"→ {info['storage_node']} (Primary)", "color": "cyan"}]
        add_trace(info["short"], trace, is_shorten=True)
        info['trace'] = trace
        return jsonify(info)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/<short_code>")
def resolve(short_code):
    if short_code in ["favicon.ico", "robots.txt", "api"]: return "Not found", 404
    long_url, info = cdn.get(short_code)
    add_trace(short_code, info["trace"], is_shorten=False)
    if long_url:
        log_event(f"REDIRECT {short_code} → {long_url}"); return redirect(long_url)
    log_event(f"NOT_FOUND {short_code}"); return "URL not found", 404

@app.route("/api/urls")
def api_urls(): return jsonify(db_get_all_urls())

@app.route("/api/nodes")
def api_nodes():
    stats = db_get_stats()
    return jsonify({
        "storage": [{"name": n.name, "active": n.is_active, "records": len(n.data)} for n in storage_nodes],
        "backends": [{"name": b.name, "requests": stats.get(b._stat_key, 0)} for b in backends]})

@app.route("/api/logs")
def api_logs(): return jsonify(db_get_events(80))

@app.route("/api/cdn")
def api_cdn(): return jsonify(cdn.get_stats())

@app.route("/api/trace")
def api_trace(): return jsonify(trace_log)

@app.route("/api/cdn/purge", methods=["POST"])
def api_cdn_purge():
    cdn.purge(); log_event("CDN cache purged"); return jsonify({"ok": True})

@app.route("/api/node/<name>/<action>", methods=["POST"])
def api_toggle_node(name, action):
    node = next((n for n in storage_nodes if n.name == name), None)
    if not node: return jsonify({"error": "Node not found"}), 404
    if action == "fail": node.fail(); log_event(f"NODE_DOWN {name}")
    elif action == "recover": node.recover(); log_event(f"NODE_UP   {name}")
    return jsonify({"ok": True})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"\n>>> Starting on http://localhost:{port} <<<\n")
    app.run(host="0.0.0.0", debug=False, port=port)
