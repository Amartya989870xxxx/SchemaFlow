// script.js (frontend)
const API_BASE = "http://localhost:3000"; // make sure backend is running on :3000

async function safeFetch(url) {
  try {
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } catch (err) {
    console.warn("fetch failed:", url, err);
    return null;
  }
}

// Fetch dashboard stats (used on landing if you want small widgets)
async function loadStats() {
  const data =
    (await safeFetch(`${API_BASE}/stats`)) ||
    (await safeFetch(`${API_BASE}/api/stats`)) ||
    null;

  if (!data) return;

  const el = document.getElementById("rawRecords");
  if (el) el.innerText = data.raw ?? "--";

  const nEl = document.getElementById("normalizedRecords");
  if (nEl) nEl.innerText = data.normalized ?? "--";

  const sEl = document.getElementById("schemaVersions");
  if (sEl) sEl.innerText = data.schemaVersions ?? "--";

  const fEl = document.getElementById("fileTypes");
  if (fEl) fEl.innerText = data.fileTypes ?? "--";
}

// You can call loadStats() from the landing page if you added small widgets there.
// Uncomment if you want them on index.html:
// loadStats();