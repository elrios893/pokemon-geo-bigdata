/* Interfaz del sistema: consume la API de este mismo origen. Mapa: Leaflet (BSD-2), calor: Leaflet.heat (BSD-2),
   dibujo: Leaflet.draw (MIT), mosaicos: OpenStreetMap (ODbL). */
"use strict";

const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const fmt = (n) => Number(n).toLocaleString("es-CO");

const TYPE_COLOR = {
  normal: "#a8a29e", fire: "#ea580c", water: "#2563eb", electric: "#eab308", grass: "#16a34a", ice: "#38bdf8",
  fighting: "#b91c1c", poison: "#9333ea", ground: "#a16207", flying: "#818cf8", psychic: "#db2777", bug: "#84cc16",
  rock: "#78716c", ghost: "#6d28d9", dragon: "#4338ca", fairy: "#f472b6",
};
const colorOf = (types) => TYPE_COLOR[(types || [])[0]] || "#78716c";
const DOW = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"];

/* ---------- mapa ---------- */
const map = L.map("map", { zoomControl: true, preferCanvas: true }).setView([40.758, -73.9855], 13);
const tiles = L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
  maxZoom: 19, attribution: "&copy; <a href='https://www.openstreetmap.org/copyright'>OpenStreetMap</a>",
}).addTo(map);
let tileErrors = 0;
tiles.on("tileerror", () => { if (++tileErrors > 3) $("notice").hidden = false; });
tiles.on("tileload", () => { tileErrors = 0; $("notice").hidden = true; });

const results = L.layerGroup().addTo(map);   // puntos de /near, /geonear, /within
const overlay = L.layerGroup().addTo(map);   // circulo, hotspots
const drawn = new L.FeatureGroup().addTo(map);
let heat = null;

/* ---------- llamada a la API + panel de JSON crudo ---------- */
let lastUrl = "";
const inflight = {};
async function api(path, { method = "GET", body, key } = {}) {
  const url = location.origin + path;
  const t0 = performance.now();
  lastUrl = url;
  $("raw-url").textContent = (method === "POST" ? "POST " : "GET ") + path;
  $("raw-url").title = $("raw-url").textContent;
  $("raw-open").hidden = method !== "GET";
  if (method === "GET") $("raw-open").href = url;
  $("raw-meta").textContent = "consultando…";
  let signal;
  if (key) { inflight[key]?.abort(); const ac = (inflight[key] = new AbortController()); signal = ac.signal; }   // descarta la consulta anterior de la misma clase
  const res = await fetch(url, { method, signal, headers: body ? { "Content-Type": "application/json" } : {}, body: body ? JSON.stringify(body) : undefined });
  const text = await res.text();
  const ms = Math.round(performance.now() - t0);
  let data = null;
  try { data = JSON.parse(text); } catch { /* respuesta no JSON */ }
  let meta = `HTTP ${res.status} · ${ms} ms · ${fmt(text.length)} bytes`;
  if (method === "POST") meta += `\ncurl -X POST -H "Content-Type: application/json" -d '${JSON.stringify(body)}' "${url}"`;
  $("raw-meta").textContent = meta;
  const pretty = data ? JSON.stringify(data, null, 2) : text;
  const LIM = 40000;
  $("raw-body").textContent = pretty.length > LIM ? pretty.slice(0, LIM) + `\n… (${fmt(pretty.length - LIM)} caracteres más; usa "Abrir" para verlo completo)` : pretty;
  if (!res.ok) throw new Error((data && data.error) || `HTTP ${res.status}`);
  return data;
}

$("raw-copy").onclick = () => {
  const txt = $("raw-url").textContent.startsWith("POST") ? $("raw-meta").textContent.split("\n")[1] || "" : lastUrl;
  navigator.clipboard?.writeText(txt);
  $("raw-copy").textContent = "Copiado";
  setTimeout(() => ($("raw-copy").textContent = "Copiar URL"), 1200);
};

function fail(err) {
  if (err.name === "AbortError") return;
  $("summary").innerHTML = `<span class="warn">Error: ${esc(err.message)}</span>`;
  $("list").innerHTML = "";
}

/* ---------- pestañas ---------- */
let tab = "near";
function setTab(name) {
  tab = name; focused = null; cellBox = null;
  document.querySelectorAll(".tabs button").forEach((b) => b.classList.toggle("on", b.dataset.tab === name));
  document.querySelectorAll(".panel").forEach((p) => (p.hidden = p.id !== "tab-" + name));
  results.clearLayers(); overlay.clearLayers(); drawn.clearLayers();
  if (heat) { map.removeLayer(heat); heat = null; }
  $("summary").innerHTML = ""; $("list").innerHTML = "";
  map.off("click", onMapClick);
  drawCtl && (name === "within" ? map.addControl(drawCtl) : map.removeControl(drawCtl));
  if (name === "near") { map.on("click", onMapClick); drawCenter(); runNear(); }
  if (name === "time") loadTime();
}
document.querySelectorAll(".tabs button").forEach((b) => (b.onclick = () => setTab(b.dataset.tab)));

/* ---------- especies (selector común) ---------- */
let species = [];
async function loadSpecies() {
  const d = await api("/stats/species?limit=151");
  species = d.results;
  const opts = '<option value="">Todas</option>' +
    [...species].sort((a, b) => a.name.localeCompare(b.name)).map((s) => `<option value="${s.pokemonId}">${esc(s.name)} (#${s.pokemonId})</option>`).join("");
  document.querySelectorAll("select.species").forEach((s) => (s.innerHTML = opts));
  const types = [...new Set(species.flatMap((s) => s.types || []))].sort();
  const topts = '<option value="">Todos</option>' + types.map((t) => `<option value="${t}">${t}</option>`).join("");
  document.querySelectorAll("select.types").forEach((s) => (s.innerHTML = topts));
}

/* ---------- lista y marcadores de avistamientos ---------- */
function plot(items, fitTo) {
  results.clearLayers();
  const list = $("list"); list.innerHTML = "";
  items.forEach((s, i) => {
    const [lng, lat] = s.location.coordinates;
    const c = colorOf(s.types);
    const m = L.circleMarker([lat, lng], { radius: 6, color: "#fff", weight: 1.5, fillColor: c, fillOpacity: 0.95 })
      .bindPopup(`<b>${esc(s.name)}</b> <span class="m">#${s.pokemonId}</span><br>${esc((s.types || []).join(" / "))}<br>` +
        `<span class="m">${esc(s.local_date)} · ${String(s.local_hour).padStart(2, "0")}h local · ${esc(s.source)}</span>` +
        (s.distance_m != null ? `<br><span class="m">a ${fmt(s.distance_m)} m</span>` : "") +
        `<br><span class="m">[${lng.toFixed(5)}, ${lat.toFixed(5)}]</span>`);
    m.addTo(results);
    if (i < 60) {
      const li = document.createElement("li");
      li.innerHTML = `<span class="dot" style="background:${c}"></span><span class="nm">${esc(s.name)}</span><span class="mt">${s.distance_m != null ? fmt(s.distance_m) + " m" : String(s.local_hour).padStart(2, "0") + "h"}</span>`;
      li.onclick = () => { map.panTo([lat, lng]); m.openPopup(); };
      list.appendChild(li);
    }
  });
  if (fitTo && items.length) map.fitBounds(fitTo, { padding: [40, 40], maxZoom: 17 });
}

/* ---------- pestaña: cerca ---------- */
let center = { lat: 40.758, lng: -73.9855 }, centerMarker = null, circle = null;
function drawCenter() {
  overlay.clearLayers();
  $("n-center").value = `${center.lat.toFixed(5)}, ${center.lng.toFixed(5)}`;
  centerMarker = L.marker([center.lat, center.lng], { draggable: true }).addTo(overlay);
  centerMarker.on("dragend", () => { const p = centerMarker.getLatLng(); center = { lat: p.lat, lng: p.lng }; drawCenter(); runNear(); });
  circle = L.circle([center.lat, center.lng], { radius: +$("n-radius").value, color: "#c2410c", weight: 1.5, fillOpacity: 0.06, interactive: false }).addTo(overlay);
}
function onMapClick(e) { center = { lat: e.latlng.lat, lng: e.latlng.lng }; drawCenter(); runNear(); }
$("n-radius").oninput = () => {
  const v = +$("n-radius").value;
  $("n-radius-o").textContent = v >= 1000 ? (v / 1000).toFixed(2).replace(/\.?0+$/, "") + " km" : v + " m";
  circle && circle.setRadius(v);
};
$("n-radius").onchange = () => runNear();
$("n-species").onchange = () => runNear();
$("n-mode").onchange = () => runNear();
$("n-type").onchange = () => runNear();
$("n-limit").onchange = () => runNear();
$("n-go").onclick = () => runNear();

async function runNear() {
  const q = new URLSearchParams({ lat: center.lat.toFixed(6), lng: center.lng.toFixed(6), radius: $("n-radius").value, limit: $("n-limit").value });
  if ($("n-species").value) q.set("pokemonId", $("n-species").value);
  if ($("n-type").value) q.set("type", $("n-type").value);
  try {
    const d = await api(`/${$("n-mode").value}?${q}`, { key: "spawns" });
    plot(d.results);
    const far = d.results.length ? d.results[d.results.length - 1].distance_m : null;
    $("summary").innerHTML = `<b>${fmt(d.count)}</b> avistamientos a ≤ ${fmt(d.query.radius_m)} m` +
      (d.count >= +$("n-limit").value ? ` <span class="warn">· tope de ${$("n-limit").value} alcanzado</span>` : "") +
      (far != null ? ` · el más lejano a ${fmt(far)} m` : "");
    if (!d.count) $("summary").innerHTML = "Sin avistamientos en ese radio. Prueba con un radio mayor o con otra zona (Manhattan tiene la mayor densidad).";
  } catch (e) { fail(e); }
}

/* ---------- pestaña: zona ---------- */
const drawCtl = new L.Control.Draw({
  draw: { polygon: { allowIntersection: false, shapeOptions: { color: "#0f766e" } }, rectangle: { shapeOptions: { color: "#0f766e" } },
          polyline: false, circle: false, marker: false, circlemarker: false },
  edit: { featureGroup: drawn, edit: false },
});
let lastPoly = null;
map.on(L.Draw.Event.CREATED, (e) => {
  if (tab !== "within") return;
  drawn.clearLayers(); drawn.addLayer(e.layer);
  const ring = e.layer.getLatLngs()[0].map((p) => [+p.lng.toFixed(6), +p.lat.toFixed(6)]);
  ring.push(ring[0]);
  lastPoly = { type: "Polygon", coordinates: [ring] };
  runWithin();
});
map.on(L.Draw.Event.DELETED, () => { lastPoly = null; results.clearLayers(); $("summary").innerHTML = ""; $("list").innerHTML = ""; });
$("w-species").onchange = () => lastPoly && runWithin();
$("w-type").onchange = () => lastPoly && runWithin();
$("w-limit").onchange = () => lastPoly && runWithin();
$("w-clear").onclick = () => { drawn.clearLayers(); results.clearLayers(); lastPoly = null; $("summary").innerHTML = ""; $("list").innerHTML = ""; };

async function runWithin() {
  const q = new URLSearchParams({ limit: $("w-limit").value, count: "true" });
  if ($("w-species").value) q.set("pokemonId", $("w-species").value);
  if ($("w-type").value) q.set("type", $("w-type").value);
  try {
    const d = await api(`/within?${q}`, { method: "POST", body: lastPoly, key: "spawns" });
    plot(d.results);
    $("summary").innerHTML = `<b>${fmt(d.total)}</b> avistamientos dentro de la zona` +
      (d.truncated ? ` <span class="warn">· se dibujan ${fmt(d.count)} (tope del límite)</span>` : "");
  } catch (e) { fail(e); }
}

/* ---------- pestaña: hotspots + calor ---------- */
$("h-go").onclick = loadHot;
$("h-heat").onchange = () => { if ($("h-heat").checked) loadHeat(); else if (heat) { map.removeLayer(heat); heat = null; } };
map.on("moveend", () => { if (tab === "hot" && $("h-heat").checked && !focused) debounce(loadHeat, 400); });
$("h-type").onchange = () => focused && showHotspot(focused);
let focused = null, cellBox = null;

/* Avistamientos de un hotspot: POST /within con el cuadrado de la celda (0,01 grados) -> $geoWithin. */
async function showHotspot(h) {
  focused = h;
  if (heat) { map.removeLayer(heat); heat = null; }
  const [lng, lat] = h.location.coordinates, d = 0.005;
  const ring = [[lng - d, lat - d], [lng + d, lat - d], [lng + d, lat + d], [lng - d, lat + d], [lng - d, lat - d]].map((p) => p.map((v) => +v.toFixed(6)));
  cellBox && overlay.removeLayer(cellBox);
  cellBox = L.rectangle([[lat - d, lng - d], [lat + d, lng + d]], { color: "#c2410c", weight: 2, fill: false, dashArray: "4 4", interactive: false }).addTo(overlay);
  map.fitBounds(cellBox.getBounds(), { padding: [30, 30], maxZoom: 18 });
  const q = new URLSearchParams({ limit: 1000, count: "true" });
  if ($("h-type").value) q.set("type", $("h-type").value);
  try {
    const r = await api(`/within?${q}`, { method: "POST", body: { type: "Polygon", coordinates: [ring] }, key: "spawns" });
    plot(r.results);
    $("summary").innerHTML = `<b>${fmt(r.total)}</b> avistamientos en el hotspot #${h.rank}` +
      ($("h-type").value ? ` (tipo ${esc($("h-type").value)})` : ` en ${fmt(h.puntos_distintos)} puntos distintos`) +
      (r.truncated ? ` <span class="warn">· se dibujan ${fmt(r.count)}</span>` : "");
  } catch (e) { fail(e); }
}
let timer; const debounce = (f, ms) => { clearTimeout(timer); timer = setTimeout(f, ms); };

async function loadHot() {
  focused = null; cellBox = null; results.clearLayers();
  try {
    const d = await api(`/stats/hotspots?limit=${$("h-limit").value}`);
    overlay.clearLayers(); const list = $("list"); list.innerHTML = "";
    const pts = [];
    d.results.forEach((h) => {
      const [lng, lat] = h.location.coordinates; pts.push([lat, lng]);
      const tops = h.top_species.map((s) => `${esc(s.name)} (${fmt(s.count)})`).join(", ");
      const pop = `<b>Hotspot #${h.rank}</b><br>${fmt(h.count)} avistamientos en ${fmt(h.puntos_distintos)} puntos distintos<br>` +
        `<span class="m">${h.species} especies · celda 0,01° · ${esc(h.cell_id)}</span><br>${tops}`;
      L.marker([lat, lng], { icon: L.divIcon({ className: "", html: `<div class="hs">${h.rank}</div>`, iconSize: [26, 26] }) })
        .bindPopup(pop).on("click", () => showHotspot(h)).addTo(overlay);
      const li = document.createElement("li");
      li.innerHTML = `<span class="dot" style="background:var(--accent)"></span><span class="nm">#${h.rank} · ${esc(h.top_species[0]?.name)}</span><span class="mt">${fmt(h.count)}</span>`;
      li.onclick = () => showHotspot(h);
      list.appendChild(li);
    });
    $("summary").innerHTML = `<b>${fmt(d.count)}</b> celdas más densas (clic en un número o en la lista para ver sus avistamientos)`;
    if (pts.length) map.fitBounds(pts, { padding: [50, 50], maxZoom: 13 });
    if ($("h-heat").checked) loadHeat();
  } catch (e) { fail(e); }
}

async function loadHeat() {
  const b = map.getBounds();
  const q = new URLSearchParams({ limit: 500, min_lat: b.getSouth(), max_lat: b.getNorth(), min_lng: b.getWest(), max_lng: b.getEast() });
  try {
    const d = await api(`/stats/grid?${q}`, { key: "heat" });
    const max = Math.max(1, ...d.results.map((c) => c.count));
    const pts = d.results.map((c) => [c.lat_c, c.lng_c, Math.sqrt(c.count / max)]);
    if (heat) map.removeLayer(heat);
    heat = L.heatLayer(pts, { radius: 28, blur: 22, maxZoom: 14, minOpacity: 0.25,
      gradient: { 0.2: "#fde68a", 0.5: "#fb923c", 0.8: "#dc2626", 1: "#7f1d1d" } }).addTo(map);
    heat.bringToBack?.();
  } catch (e) { fail(e); }
}

/* ---------- pestaña: tiempo y especies ---------- */
let gran = "hour", picked = null;
document.querySelectorAll("#t-gran button").forEach((b) => (b.onclick = () => {
  gran = b.dataset.g; picked = null;
  document.querySelectorAll("#t-gran button").forEach((x) => x.classList.toggle("on", x === b));
  loadTime();
}));

function bars(items, label, title) {
  const W = 360, H = 150, pad = 18, max = Math.max(1, ...items.map((i) => i.v));
  const bw = (W - pad) / items.length;
  const rects = items.map((it, i) => {
    const h = Math.max(1, (it.v / max) * (H - 28));
    return `<rect class="bar" x="${(pad + i * bw + 0.5).toFixed(1)}" y="${(H - 16 - h).toFixed(1)}" width="${Math.max(1, bw - 1.5).toFixed(1)}" height="${h.toFixed(1)}" rx="1.5"><title>${esc(it.k)}: ${fmt(it.v)}</title></rect>`;
  }).join("");
  const step = Math.ceil(items.length / 8);
  const ticks = items.map((it, i) => (i % step ? "" : `<text x="${(pad + i * bw + bw / 2).toFixed(1)}" y="${H - 3}" text-anchor="middle">${esc(label(it.k))}</text>`)).join("");
  return `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(title)}"><text x="0" y="9">${fmt(max)}</text>${rects}${ticks}</svg>`;
}

async function loadTime() {
  try {
    let items, label, title;
    if (picked) {
      const d = await api(`/stats/species/${picked.pokemonId}/hours`);
      items = d.results.map((r) => ({ k: r.local_hour, v: r.count })); label = (k) => k + "h"; title = `Horas de ${d.name}`;
      $("summary").innerHTML = `<b>${esc(d.name)}</b> por hora local · pico a las ${items.reduce((a, b) => (b.v > a.v ? b : a)).k}h`;
    } else {
      const d = await api(`/stats/time?granularity=${gran}`);
      items = d.results.map((r) => ({ k: r.key, v: r.count }));
      label = gran === "dow" ? (k) => DOW[k] : gran === "day" ? (k) => String(k).slice(5) : (k) => k + "h";
      title = "Avistamientos por " + gran;
      const top = items.reduce((a, b) => (b.v > a.v ? b : a));
      $("summary").innerHTML = `<b>${fmt(items.reduce((a, b) => a + b.v, 0))}</b> avistamientos · máximo en ${esc(label(top.k))}`;
    }
    $("t-chart").innerHTML = bars(items, label, title);
    await loadRank();
  } catch (e) { fail(e); }
}

async function loadRank() {
  const d = await api("/stats/species?limit=15");
  const max = d.results[0].count;
  $("t-species").innerHTML = d.results.map((s) =>
    `<div class="r${picked && picked.pokemonId === s.pokemonId ? " on" : ""}" data-id="${s.pokemonId}">
       <span class="nm"><span>${esc(s.name)}</span><i>${fmt(s.count)}</i></span><span class="bar" style="width:${(100 * s.count / max).toFixed(0)}%"></span></div>`).join("");
  $("t-species").querySelectorAll(".r").forEach((r) => (r.onclick = () => {
    const id = +r.dataset.id;
    picked = picked && picked.pokemonId === id ? null : { pokemonId: id };
    loadTime();
  }));
}

/* ---------- inicio ---------- */
(async function init() {
  try {
    const h = await fetch("/health").then((r) => r.json());
    $("health").textContent = h.status === "ok" ? "API y Mongo activos" : "Mongo caído";
    $("health").className = "health " + (h.status === "ok" ? "ok" : "bad");
  } catch { $("health").textContent = "API sin respuesta"; $("health").className = "health bad"; }
  setTab("near");                                  // no espera al catalogo de especies
  try { await loadSpecies(); } catch (e) { fail(e); }
})();
