const API = "http://localhost:3000";

document.addEventListener("DOMContentLoaded", loadHistory);

async function loadHistory() {
  const tbody = document.getElementById("historyBody");

  try {
    const res = await fetch(`${API}/api/schema/history`);
    const list = await res.json();

    tbody.innerHTML = list
      .map(
        (s, i) => `
        <tr>
          <td>${s.version}</td>
          <td>${new Date(s.createdAt).toLocaleString()}</td>
          <td>${s.fieldsCount}</td>
          <td>${s.totalSamples}</td>
          <td>
            ${
              i < list.length - 1
                ? `<button class="btn-link" onclick="viewDiff(${s.version}, ${
                    list[i + 1].version
                  })">Diff with v${list[i + 1].version}</button>`
                : `<span style="color:var(--muted)">—</span>`
            }
          </td>
        </tr>`
      )
      .join("");
  } catch (err) {
    tbody.innerHTML = `<tr><td colspan="5" style="color:#f77">Failed to load schema history</td></tr>`;
  }
}

async function viewDiff(a, b) {
  const panel = document.getElementById("diffPanel");
  const title = document.getElementById("diffTitle");
  const added = document.getElementById("addedFields");
  const removed = document.getElementById("removedFields");
  const changed = document.getElementById("changedFields");

  panel.style.display = "block";
  title.innerText = `Comparing Schema v${a} → v${b}`;

  added.innerText = "Loading...";
  removed.innerText = "Loading...";
  changed.innerText = "Loading...";

  try {
    const res = await fetch(`${API}/api/schema/diff/${a}/${b}`);
    const data = await res.json();

    added.innerText = JSON.stringify(data.diff.added, null, 2);
    removed.innerText = JSON.stringify(data.diff.removed, null, 2);
    changed.innerText = JSON.stringify(data.diff.changed, null, 2);
  } catch (err) {
    added.innerText = removed.innerText = changed.innerText = "Error loading diff";
  }
}