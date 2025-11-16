// transform.js
const { RawRecord, NormalizedRecord, SchemaVersion } = require("./db");

function normalizePayload(payload) {
  const out = {};
  for (const k of Object.keys(payload)) {
    let v = payload[k];
    if (Array.isArray(v)) {
      v = v.map(x => (typeof x === "object" ? JSON.stringify(x) : String(x))).join(",");
    } else if (v && typeof v === "object") {
      v = JSON.stringify(v);
    }
    if (typeof v === "string" && /^\d+$/.test(v) && (k.toLowerCase().includes("age") || k.toLowerCase().includes("price") || k.toLowerCase().includes("count"))) {
      v = parseInt(v, 10);
    }
    out[k] = v;
  }
  return out;
}

async function normalizePending() {
  const latest = await SchemaVersion.findOne().sort({ version: -1 });
  const schemaVer = latest ? latest.version : null;

  const raws = await RawRecord.find().sort({ ingestedAt: -1 }).limit(1000);
  for (const r of raws) {
    const exists = await NormalizedRecord.findOne({ rawId: r._id });
    if (exists) continue;
    const canonical = normalizePayload(r.payload);
    await NormalizedRecord.create({
      canonical,
      rawId: r._id,
      schemaVersion: schemaVer
    });
    console.log("Normalized raw record:", r._id.toString());
  }
}

module.exports = { normalizePending };
