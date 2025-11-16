// routes.js – FINAL CLEAN + FIXED VERSION
const express = require("express");
const multer = require("multer");
const os = require("os");
const path = require("path");
const fs = require("fs");

const { RawRecord, SchemaVersion, NormalizedRecord } = require("./db");
const { extractFile } = require("./extractor");
const { inferAndMaybeCreateVersion } = require("./infer");
const { normalizePending } = require("./transform");

const router = express.Router();

// multer temp storage
const upload = multer({ dest: path.join(os.tmpdir(), "uploads") });

/* ============================================================
   SINGLE FILE UPLOAD (THIS IS WHAT FRONTEND USES)
   ============================================================ */
router.post("/upload", upload.single("file"), async (req, res) => {
  const f = req.file;
  const source = req.query.source || "upload";

  if (!f) return res.status(400).json({ error: "No file uploaded (field name: file)" });

  const tempPath = f.path;

  try {
    const extracted = await extractFile(tempPath, f.originalname, f.mimetype);

    const meta = {
      filename: f.originalname,
      mimetype: f.mimetype,
      size: f.size,
      uploadedAt: new Date()
    };

    const payloadToStore =
      extracted.type === "json"
        ? extracted.payload
        : { _extractedText: extracted.payload };

    const doc = await RawRecord.create({
      payload: payloadToStore,
      source,
      fileMetadata: meta,
      ingestedAt: new Date(),
    });

    // background tasks (async)
    inferAndMaybeCreateVersion().catch(() => {});
    normalizePending().catch(() => {});

    res.json({ ok: true, id: doc._id });

  } catch (err) {
    console.error("UPLOAD ERROR:", err);
    res.status(500).json({ error: err.message });
  } finally {
    try { fs.unlinkSync(tempPath); } catch {}
  }
});

/* ============================================================
   JSON INGEST (direct POST JSON)
   ============================================================ */
router.post("/ingest", async (req, res) => {
  try {
    const payload = req.body;
    const source = req.query.source || req.headers["x-source"] || "ingest";

    const doc = await RawRecord.create({ payload, source });

    res.status(201).json({ id: doc._id, msg: "ingested" });

    // background jobs
    inferAndMaybeCreateVersion().catch(() => {});
    normalizePending().catch(() => {});

  } catch (err) {
    console.error("INGEST ERROR:", err);
    res.status(500).json({ error: err.message });
  }
});

/* ============================================================
   LIST RAW DOCS
   ============================================================ */
router.get("/raw", async (req, res) => {
  try {
    const docs = await RawRecord.find()
      .sort({ ingestedAt: -1 })
      .limit(200);

    res.json(docs);
  } catch (err) {
    console.error("RAW FETCH ERROR:", err);
    res.status(500).json({ error: err.message });
  }
});

/* ============================================================
   SINGLE RAW VIEW
   ============================================================ */
router.get("/raw/:id", async (req, res) => {
  try {
    const doc = await RawRecord.findById(req.params.id);
    if (!doc) return res.status(404).json({ error: "not found" });

    res.json(doc);
  } catch (err) {
    console.error("RAW FETCH ERROR:", err);
    res.status(500).json({ error: err.message });
  }
});

/* ============================================================
   SCHEMA ROUTES
   ============================================================ */
router.get("/schema/latest", async (req, res) => {
  try {
    const latest = await SchemaVersion.findOne().sort({ version: -1 });
    if (!latest) return res.status(404).json({ error: "no schema yet" });

    res.json(latest);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get("/schemas", async (req, res) => {
  try {
    const docs = await SchemaVersion.find().sort({ version: -1 });
    res.json(docs);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get("/schemas/view/:v", async (req, res) => {
  try {
    const v = Number(req.params.v);
    if (isNaN(v)) return res.status(400).json({ error: "invalid version" });

    const doc = await SchemaVersion.findOne({ version: v });
    if (!doc) return res.status(404).json({ error: "schema not found" });

    res.json(doc);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ============================================================
   FORCE SCHEMA + NORMALIZATION
   ============================================================ */
router.post("/admin/run", async (req, res) => {
  try {
    const s = await inferAndMaybeCreateVersion();
    await normalizePending();

    res.json({ ok: true, createdSchema: s ? s.version : null });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ============================================================
   RECORDS LIST + SEARCH
   ============================================================ */
router.get("/records", async (req, res) => {
  try {
    const { q, schemaVersion, fileType } = req.query;

    let filter = {};
    if (schemaVersion) filter.schemaVersion = Number(schemaVersion);
    if (fileType) filter["fileMetadata.mimetype"] = fileType;

    let records = await NormalizedRecord.find(filter)
      .sort({ normalizedAt: -1 })
      .limit(200);

    if (q) {
      const lower = q.toLowerCase();
      records = records.filter(r =>
        JSON.stringify(r.canonical).toLowerCase().includes(lower)
      );
    }

    res.json(records);
  } catch (err) {
    res.status(500).json({ error: "Failed to fetch records" });
  }
});

/* ============================================================
   DASHBOARD STATS
   ============================================================ */
router.get("/stats", async (req, res) => {
  try {
    const raw = await RawRecord.countDocuments();
    const norm = await NormalizedRecord.countDocuments();
    const schemas = await SchemaVersion.countDocuments();
    const fileTypes = await RawRecord.distinct("fileMetadata.mimetype");

    res.json({
      rawRecords: raw,
      normalizedRecords: norm,
      schemaVersions: schemas,
      fileTypes: fileTypes.length
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ============================================================
   RECENT UPLOADS
   ============================================================ */
router.get("/uploads/recent", async (req, res) => {
  try {
    const list = await RawRecord.find()
      .sort({ ingestedAt: -1 })
      .limit(10);

    res.json(list);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});


// ------- Phase A: schema history, diff, and simple query endpoints -------
// Paste this into routes.js (before module.exports = router;)

const escapeHtml = (s) => (typeof s === 'string' ? s.replace(/[&<>"'`]/g, (m) => ({
  '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;', '`':'&#96;'
})[m]) : s);

// Utility: field-diff between two schema 'fields' objects
function diffSchemas(fieldsA = {}, fieldsB = {}) {
  const keysA = Object.keys(fieldsA);
  const keysB = Object.keys(fieldsB);
  const added = keysB.filter(k => !keysA.includes(k));
  const removed = keysA.filter(k => !keysB.includes(k));
  const possibleChanged = keysA.filter(k => keysB.includes(k));
  const changed = possibleChanged.filter(k => {
    // compare present / types arrays simply (stringify)
    const a = fieldsA[k] || {};
    const b = fieldsB[k] || {};
    return JSON.stringify(a) !== JSON.stringify(b);
  });
  return { added, removed, changed };
}

/**
 * GET /api/schema/history
 * Returns an array of schema versions (lightweight)
 */
router.get('/schema/history', async (req, res) => {
  try {
    const docs = await SchemaVersion.find().sort({ version: -1 }).limit(200).lean();
    // normalize shape for frontend
    const out = docs.map(d => ({
      version: d.version,
      createdAt: d.createdAt,
      totalSamples: d.totalSamples ?? d.totalSamples,
      notes: d.notes ?? '',
      fieldsCount: d.fields ? Object.keys(d.fields).length : 0
    }));
    res.json(out);
  } catch (err) {
    console.error('schema/history error:', err);
    res.status(500).json({ error: err.message || 'failed to fetch schema history' });
  }
});

/**
 * GET /api/schema/diff/:a/:b
 * Return a simple diff (added / removed / changed fields)
 */
router.get('/schema/diff/:a/:b', async (req, res) => {
  try {
    const a = Number(req.params.a);
    const b = Number(req.params.b);
    if (Number.isNaN(a) || Number.isNaN(b)) return res.status(400).json({ error: 'invalid versions' });

    const docA = await SchemaVersion.findOne({ version: a }).lean();
    const docB = await SchemaVersion.findOne({ version: b }).lean();

    if (!docA || !docB) return res.status(404).json({ error: 'one or both schema versions not found' });

    const diff = diffSchemas(docA.fields || {}, docB.fields || {});
    res.json({
      from: a,
      to: b,
      diff
    });
  } catch (err) {
    console.error('schema/diff error:', err);
    res.status(500).json({ error: err.message || 'schema diff failed' });
  }
});

/**
 * POST /api/query
 * Simple, safe query endpoint (first-step)
 * Body: { q: "search text", schemaVersion: 12, limit: 50 }
 * This does a case-insensitive JSON-string match against normalized.canonical
 */
router.post('/query', async (req, res) => {
  try {
    const { q, schemaVersion, limit = 100 } = req.body || {};
    const filter = {};

    if (schemaVersion) filter.schemaVersion = Number(schemaVersion);

    // fetch candidate documents (we'll filter by q in memory)
    let docs = await NormalizedRecord.find(filter).sort({ normalizedAt: -1 }).limit(Number(limit)).lean();

    if (q && typeof q === 'string' && q.trim()) {
      const qLower = q.toLowerCase();
      docs = docs.filter(d => {
        const canonical = d.canonical || {};
        return JSON.stringify(canonical).toLowerCase().includes(qLower);
      });
    }

    // respond with safe lightweight preview
    const out = docs.map(d => ({
      _id: d._id,
      schemaVersion: d.schemaVersion,
      normalizedAt: d.normalizedAt,
      preview: JSON.stringify(d.canonical || {}).slice(0, 400),
      canonical: d.canonical // include full canonical; front can choose to omit or show
    }));

    res.json({ ok: true, total: out.length, results: out });
  } catch (err) {
    console.error('query error:', err);
    res.status(500).json({ error: err.message || 'query failed' });
  }
});

module.exports = router;