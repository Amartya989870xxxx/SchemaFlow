// dashboard.js
const API = "http://localhost:3000";

document.addEventListener("DOMContentLoaded", loadDashboard);

async function loadDashboard() {
  try {
    // 1) Load stats
    const statsRes = await fetch(`${API}/api/stats`);
    const stats = await statsRes.json();

    document.getElementById("rawCount").innerText = stats.rawRecords;
    document.getElementById("normCount").innerText = stats.normalizedRecords;
    document.getElementById("schemaCount").innerText = stats.schemaVersions;
    document.getElementById("fileCount").innerText = stats.fileTypes;

    // 2) Load schema versions
    const schemaRes = await fetch(`${API}/api/schemas`);
    const schemaList = await schemaRes.json();

    const schemaTable = document.getElementById("schemaList");
    schemaTable.innerHTML = schemaList
      .map(
        s => `
        <tr>
          <td>${s.version}</td>
          <td>${s.totalSamples}</td>
          <td>${new Date(s.createdAt).toLocaleString()}</td>
          <td><a class="btn-link" href="schemas.html?version=${s.version}">View</a></td>
        </tr>
        `
      )
      .join("");

    // 3) Load recent uploads
    const uploadRes = await fetch(`${API}/api/uploads/recent`);
    const uploads = await uploadRes.json();

    const uploadTable = document.getElementById("uploadList");
    uploadTable.innerHTML = uploads
      .map(
        u => `
        <tr>
          <td>${u.fileMetadata?.filename || "-"}</td>
          <td>${u.fileMetadata?.mimetype || "-"}</td>
          <td>${new Date(u.ingestedAt).toLocaleString()}</td>
        </tr>`
      )
      .join("");
  } catch (err) {
    console.error("Dashboard failed:", err);
  }
}