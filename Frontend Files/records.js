// records.js (updated)
// Keeps UI the same, uses POST /api/query, modal preview, pagination, schema datalist

const API = "http://localhost:3000";

document.addEventListener("DOMContentLoaded", () => {
  // Element refs
  const queryInput = document.getElementById("query");
  const schemaInput = document.getElementById("schemaFilter");
  const fileInput = document.getElementById("fileFilter");
  const searchBtn = document.getElementById("searchBtn");
  const tbody = document.querySelector("#recordsTable tbody");
  const prevBtn = document.getElementById("prevPage");
  const nextBtn = document.getElementById("nextPage");

  const modalBg = document.getElementById("modalBg");
  const modalContent = document.getElementById("modalContent");

  // state
  let page = 1;
  const limit = 10;
  let lastResults = []; // hold current page results

  // attach listeners
  searchBtn.addEventListener("click", () => { page = 1; loadRecords(); });
  prevBtn.addEventListener("click", () => { if (page > 1) { page--; loadRecords(); }});
  nextBtn.addEventListener("click", () => { page++; loadRecords(); });

  // close modal when clicking background or pressing Esc
  modalBg.addEventListener("click", (e) => {
    if (e.target === modalBg) closeModal();
  });
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeModal();
  });

  // build schema datalist for suggestions (doesn't change page markup)
  async function populateSchemaDatalist() {
    try {
      const r = await fetch(`${API}/api/schema/history`);
      if (!r.ok) return;
      const history = await r.json();
      // create datalist element if not present
      let list = document.getElementById("schemaVersionsDatalist");
      if (!list) {
        list = document.createElement("datalist");
        list.id = "schemaVersionsDatalist";
        document.body.appendChild(list);
        // attach to schema input
        if (schemaInput) schemaInput.setAttribute("list", list.id);
      }
      list.innerHTML = history.map(h => `<option value="${h.version}">`).join("");
    } catch (err) {
      // ignore
      console.warn("Failed to fetch schema history for datalist", err);
    }
  }

  // Format preview string (safe)
  function formatPreview(obj) {
    try {
      if (!obj) return "";
      const s = JSON.stringify(obj);
      const preview = s.length > 120 ? s.slice(0, 120) + "..." : s;
      return preview;
    } catch (e) {
      return String(obj).slice(0, 120) + (String(obj).length > 120 ? "..." : "");
    }
  }

  // render rows from an array of normalized records
  function renderRows(rows) {
    lastResults = rows || [];
    if (!rows || !rows.length) {
      tbody.innerHTML = `<tr><td colspan="5" style="color:#f77">No records found</td></tr>`;
      return;
    }

    tbody.innerHTML = rows.map((r, idx) => {
      const id = r._id || (r.rawId ? String(r.rawId) : "-");
      const schema = (r.schemaVersion !== undefined && r.schemaVersion !== null) ? r.schemaVersion : "-";
      const time = r.normalizedAt ? new Date(r.normalizedAt).toLocaleString() : (r.ingestedAt ? new Date(r.ingestedAt).toLocaleString() : "-");
      // Show canonical if present, otherwise fall back to payload / entire record
      const previewObj = r.canonical ?? r.payload ?? r;
      const preview = escapeHtml(formatPreview(previewObj));
      return `
        <tr>
          <td class="mono">${escapeHtml(id)}</td>
          <td>${escapeHtml(String(schema))}</td>
          <td><code class="preview-snippet">${preview}</code></td>
          <td>${escapeHtml(time)}</td>
          <td><button class="btn-link view-btn" data-idx="${idx}">View</button></td>
        </tr>
      `;
    }).join("");

    // attach view handlers
    document.querySelectorAll(".view-btn").forEach(btn => {
      btn.addEventListener("click", (e) => {
        const idx = Number(btn.dataset.idx);
        if (!Number.isNaN(idx) && lastResults[idx]) showRecord(lastResults[idx]);
      });
    });
  }

  // show record in overlay modal
  function showRecord(record) {
    modalContent.textContent = JSON.stringify(record, null, 2);
    modalBg.style.display = "flex";
    // scroll modal content to top
    modalContent.scrollTop = 0;
  }

  function closeModal() {
    modalBg.style.display = "none";
    modalContent.textContent = "";
  }

  // utility: escape HTML for safe insertion
  function escapeHtml(str) {
    return String(str)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  // main loader: uses POST /api/query
  async function loadRecords() {
    tbody.innerHTML = `<tr><td colspan="5" style="color:var(--muted)">Loading...</td></tr>`;

    const q = (queryInput && queryInput.value) ? queryInput.value.trim() : "";
    const schema = (schemaInput && schemaInput.value) ? schemaInput.value.trim() : "";
    const fileType = (fileInput && fileInput.value) ? fileInput.value.trim() : "";

    const body = {
      q: q || undefined,
      schemaVersion: schema ? Number(schema) : undefined,
      fileType: fileType || undefined,
      limit,
      page
    };

    try {
      const res = await fetch(`${API}/api/query`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
      });

      // backend returns { ok, total, results } — handle that
      const json = await res.json();

      if (!res.ok) {
        const msg = (json && json.error) ? json.error : `Server returned ${res.status}`;
        tbody.innerHTML = `<tr><td colspan="5" style="color:#f77">Failed to load records: ${escapeHtml(msg)}</td></tr>`;
        return;
      }

      // Support two shapes:
      // 1) { ok: true, total: N, results: [...] }
      // 2) direct array of normalized records (legacy)
      if (Array.isArray(json)) {
        renderRows(json);
      } else if (json && Array.isArray(json.results)) {
        renderRows(json.results);
        // optionally you can update pagination UI if you show total
        // e.g. disable next if results < limit
        if (json.results.length < limit) {
          // likely last page
          nextBtn.disabled = true;
        } else {
          nextBtn.disabled = false;
        }
        prevBtn.disabled = page <= 1;
      } else {
        // unknown response
        renderRows([]);
      }

    } catch (err) {
      console.error("Records error:", err);
      tbody.innerHTML = `<tr><td colspan="5" style="color:#f77">Failed to load records</td></tr>`;
    }
  }

  // kick off
  populateSchemaDatalist().finally(() => loadRecords());
});