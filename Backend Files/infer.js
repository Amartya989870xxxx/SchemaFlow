const { RawRecord, SchemaVersion } = require("./db");

function typeName(v) {
  if (v === null || v === undefined) return "null";
  if (Array.isArray(v)) return "array";
  if (typeof v === "object") return "object";
  if (Number.isInteger(v)) return "integer";
  return typeof v;
}

function collect(obj, stats, prefix="") {
  if (obj === null || obj === undefined) return;

  if (typeof obj !== "object" || Array.isArray(obj)) {
    const key = prefix || "value";
    stats[key] = stats[key] || { count:0, types:new Set() };
    stats[key].count++;
    stats[key].types.add(typeName(obj));
    return;
  }

  for (const k of Object.keys(obj)) {
    const v = obj[k];
    const path = prefix ? `${prefix}.${k}` : k;

    stats[path] = stats[path] || { count:0, types:new Set() };
    stats[path].count++;
    stats[path].types.add(typeName(v));

    if (typeof v === "object") {
      collect(v, stats, path);
    }
    if (Array.isArray(v)) {
      for (const item of v.slice(0,3)) {
        collect(item, stats, `${path}[]`);
      }
    }
  }
}

function buildSchema(stats, total) {
  const output = {};
  for (const [k, s] of Object.entries(stats)) {
    output[k] = {
      present: s.count,
      optional: s.count < total,
      types: Array.from(s.types)
    };
  }
  return output;
}

function deepDifferent(a, b) {
  return JSON.stringify(a) !== JSON.stringify(b);
}

async function inferAndMaybeCreateVersion(sampleSize=30) {
  const samples = await RawRecord.find().sort({ ingestedAt:-1 }).limit(sampleSize);
  if (samples.length === 0) return null;

  const stats = {};
  for (const s of samples) collect(s.payload, stats);

  const newSchema = buildSchema(stats, samples.length);
  const latest = await SchemaVersion.findOne().sort({ version:-1 });

  if (!latest || deepDifferent(latest.fields, newSchema)) {
    const newVer = latest ? latest.version + 1 : 1;
    return await SchemaVersion.create({
      version: newVer,
      fields: newSchema,
      totalSamples: samples.length,
      notes: "auto"
    });
  }

  return null;
}

module.exports = { inferAndMaybeCreateVersion };