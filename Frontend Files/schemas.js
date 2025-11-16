// schemas.js
const API = "http://localhost:3000";

document.addEventListener("DOMContentLoaded", loadSchemaList);

async function loadSchemaList() {
  const tbody = document.querySelector("#schemaTable tbody");

  try {
    const res = await fetch(`${API}/api/schemas`);
    const list = await res.json();

    tbody.innerHTML = list
      .map(
        s => `
        <tr>
          <td>${s.version}</td>
          <td>${Object.keys(s.fields).length}</td>
          <td>${new Date(s.createdAt).toLocaleString()}</td>
          <td><a class="btn-link" onclick="loadDetails(${s.version})">Details</a></td>
        </tr>`
      )
      .join("");
  } catch {
    tbody.innerHTML =
      `<tr><td colspan="4" style="color:#f77">Failed to load schemas</td></tr>`;
  }
}

async function loadDetails(v) {
  const box = document.getElementById("schemaDetails");
  box.innerHTML = `<p>Loading version ${v}...</p>`;

  try {
    const res = await fetch(`${API}/api/schemas/view/${v}`);
    const data = await res.json();

    box.innerHTML = `
      <h3>Schema Version ${v}</h3>
      <pre>${JSON.stringify(data.fields, null, 2)}</pre>
    `;
  } catch {
    box.innerHTML = `<p style="color:#f77">Failed to load schema.</p>`;
  }
}