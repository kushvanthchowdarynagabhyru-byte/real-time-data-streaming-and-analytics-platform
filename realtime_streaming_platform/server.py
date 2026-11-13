"""Live web dashboard + JSON API (standard library only).

  GET /              dashboard (auto-refreshes every second)
  GET /api/snapshot  current metrics as JSON
  GET /health
"""
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PAGE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Streaming Analytics</title>
<style>
:root{--bg:#f6f7f9;--card:#fff;--fg:#1c2330;--muted:#6b7585;--line:#e3e6ec;--accent:#2563eb;--bad:#dc2626}
@media (prefers-color-scheme:dark){:root{--bg:#10141b;--card:#181e28;--fg:#e6e9ef;--muted:#8a94a6;--line:#273041;--accent:#60a5fa;--bad:#f87171}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.45 system-ui,sans-serif}
header{padding:16px 20px;display:flex;justify-content:space-between;align-items:center}
h1{font-size:18px;margin:0}.live{color:var(--muted)}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:#16a34a;margin-right:6px}
main{padding:0 20px 24px;display:grid;gap:14px;max-width:1100px;margin:auto}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px 14px}
.card .v{font-size:22px;font-weight:600}.card .l{color:var(--muted);font-size:12px}
section{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px 14px;overflow-x:auto}
h2{font-size:14px;margin:0 0 8px}
table{width:100%;border-collapse:collapse}th,td{text-align:left;padding:5px 8px;border-bottom:1px solid var(--line);white-space:nowrap}
th{color:var(--muted);font-weight:500;font-size:12px}
.bad{color:var(--bad)}svg{display:block}
@media (max-width:640px){main{padding:0 12px 20px}}
</style></head><body>
<header><h1>Real-Time Streaming Analytics</h1><div class="live"><span class="dot"></span><span id="up">connecting...</span></div></header>
<main>
<div class="cards" id="cards"></div>
<section><h2>Sensors (live temperature, last 60 readings)</h2><table id="sensors"></table></section>
<section><h2>Revenue by category (from closed windows)</h2><table id="revenue"></table></section>
<section><h2>Recent anomaly alerts</h2><table id="alerts"></table></section>
<section><h2>Recent closed windows</h2><table id="windows"></table></section>
</main>
<script>
const esc=s=>String(s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const fmtT=t=>new Date(t*1000).toLocaleTimeString();
function spark(a){if(a.length<2)return'';const w=140,h=28,mn=Math.min(...a),mx=Math.max(...a),r=(mx-mn)||1;
const pts=a.map((v,i)=>(i/(a.length-1)*w).toFixed(1)+','+(h-2-(v-mn)/r*(h-4)).toFixed(1)).join(' ');
return '<svg width="'+w+'" height="'+h+'"><polyline fill="none" stroke="var(--accent)" stroke-width="1.5" points="'+pts+'"/></svg>';}
function rows(head,data){return '<tr>'+head.map(h=>'<th>'+h+'</th>').join('')+'</tr>'+(data.length?data.join(''):'<tr><td colspan="'+head.length+'">no data yet</td></tr>');}
function card(l,v){return '<div class="card"><div class="v">'+esc(v)+'</div><div class="l">'+esc(l)+'</div></div>';}
async function tick(){
 try{const s=await (await fetch('/api/snapshot')).json();
 document.getElementById('up').textContent='live - uptime '+s.uptime_s+'s';
 const r=s.rates,t=s.totals,l=s.lag;
 document.getElementById('cards').innerHTML=[
  card('sensor events/sec',r['sensor-readings']||0),card('order events/sec',r['orders']||0),
  card('events processed',Object.values(t.events).reduce((a,b)=>a+b,0).toLocaleString()),
  card('windows closed',t.windows),card('alerts',t.alerts),card('late events dropped',t.late_dropped),
  card('consumer lag (sensors)',l['sensor-readings']),card('consumer lag (orders)',l['orders'])].join('');
 document.getElementById('sensors').innerHTML=rows(['Sensor','Latest','Trend'],s.sensors.map(x=>'<tr><td>'+esc(x.id)+'</td><td>'+esc(x.latest)+' &deg;C</td><td>'+spark(x.series)+'</td></tr>'));
 document.getElementById('revenue').innerHTML=rows(['Category','Orders','Revenue'],s.revenue.map(x=>'<tr><td>'+esc(x.category)+'</td><td>'+esc(x.orders)+'</td><td>'+esc(x.revenue.toLocaleString())+'</td></tr>'));
 document.getElementById('alerts').innerHTML=rows(['Time','Sensor','Type','Value','Detail'],s.recent_alerts.map(a=>'<tr><td>'+fmtT(a.ts)+'</td><td>'+esc(a.key)+'</td><td class="bad">'+esc(a.kind)+'</td><td>'+esc(a.value)+'</td><td>'+esc(a.detail)+'</td></tr>'));
 document.getElementById('windows').innerHTML=rows(['Stream','Key','Window','Count','Avg','Min','Max'],s.recent_windows.map(w=>'<tr><td>'+esc(w.stream)+'</td><td>'+esc(w.key)+'</td><td>'+fmtT(w.start)+' - '+fmtT(w.end)+'</td><td>'+w.count+'</td><td>'+w.avg+'</td><td>'+w.minimum.toFixed(2)+'</td><td>'+w.maximum.toFixed(2)+'</td></tr>'));
 }catch(e){document.getElementById('up').textContent='disconnected';}
}
tick();setInterval(tick,1000);
</script></body></html>"""


def make_handler(engine):
    class Handler(BaseHTTPRequestHandler):
        def _send(self, code, body: bytes, ctype: str):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path in ("/", "/index.html"):
                self._send(200, PAGE.encode(), "text/html; charset=utf-8")
            elif self.path == "/api/snapshot":
                self._send(200, json.dumps(engine.snapshot()).encode(), "application/json")
            elif self.path == "/health":
                self._send(200, b'{"status":"ok"}', "application/json")
            else:
                self._send(404, b'{"error":"not found"}', "application/json")

        def log_message(self, *args):
            pass

    return Handler


def serve(engine, host="127.0.0.1", port=8080):
    server = ThreadingHTTPServer((host, port), make_handler(engine))
    print(f"Dashboard: http://{host}:{port}   (Ctrl+C to stop)")
    try:
        server.serve_forever()
    finally:
        server.server_close()
