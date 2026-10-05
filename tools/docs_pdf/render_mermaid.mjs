#!/usr/bin/env node
/**
 * render_mermaid.mjs — Renderiza un archivo .mmd (Mermaid) a PNG con el mismo stack que generate.mjs
 * (mermaid.js dentro de Chrome/Edge headless via puppeteer-core).
 *
 * Uso: node render_mermaid.mjs --input ../../docs/img/arquitectura.mmd --output ../../docs/img/arquitectura.png [--scale 2]
 * Variable opcional: PUPPETEER_EXECUTABLE_PATH.
 */
import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import puppeteer from "puppeteer-core";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const argv = process.argv.slice(2);
const arg = (name, def) => { const i = argv.indexOf(name); return i >= 0 ? argv[i + 1] : def; };
const input = path.resolve(arg("--input", ""));
const output = path.resolve(arg("--output", input.replace(/\.mmd$/i, ".png")));
const scale = Number(arg("--scale", "2"));
if (!existsSync(input)) { console.error(`No existe: ${input}`); process.exit(1); }

function findChrome() {
  if (process.env.PUPPETEER_EXECUTABLE_PATH) return process.env.PUPPETEER_EXECUTABLE_PATH;
  const pf = process.env["PROGRAMFILES"] || "C:\Program Files";
  const pf86 = process.env["PROGRAMFILES(X86)"] || "C:\Program Files (x86)";
  const c = [
    path.join(pf, "Google", "Chrome", "Application", "chrome.exe"),
    path.join(pf86, "Microsoft", "Edge", "Application", "msedge.exe"),
    "/usr/bin/google-chrome", "/usr/bin/chromium", "/usr/bin/chromium-browser",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
  ];
  return c.find((p) => existsSync(p));
}

const exe = findChrome();
if (!exe) { console.error("No se encontro Chrome/Edge: define PUPPETEER_EXECUTABLE_PATH"); process.exit(1); }
const source = readFileSync(input, "utf-8");
const mermaidJs = readFileSync(path.resolve(__dirname, "node_modules/mermaid/dist/mermaid.min.js"), "utf-8");

const browser = await puppeteer.launch({ executablePath: exe, headless: true });
try {
  const page = await browser.newPage();
  await page.setViewport({ width: 1600, height: 1000, deviceScaleFactor: scale });
  await page.setContent('<html><body style="margin:0;background:#fff"><div id="out" style="display:inline-block;padding:16px"></div></body></html>');
  await page.addScriptTag({ content: mermaidJs });
  const error = await page.evaluate(async (src) => {
    mermaid.initialize({ startOnLoad: false, theme: "default", securityLevel: "loose",
      fontFamily: "Segoe UI, Arial, sans-serif", flowchart: { htmlLabels: true, useMaxWidth: false } });
    try { const { svg } = await mermaid.render("d1", src); document.getElementById("out").innerHTML = svg; return null; }
    catch (e) { return String(e && e.message ? e.message : e); }
  }, source);
  if (error) { console.error("Error de Mermaid:", error); process.exit(1); }
  const el = await page.$("#out");
  await el.screenshot({ path: output, omitBackground: false });
  console.log(`PNG generado: ${output}`);
} finally { await browser.close(); }
