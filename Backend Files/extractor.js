const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const pdfParse = require('pdf-parse');
const mammoth = require('mammoth');
const cheerio = require('cheerio');
const xml2js = require('xml2js');
const Tesseract = require('tesseract.js');
const FileType = require('file-type');

// Utility: clean text
function clean(text) {
    return text.replace(/\s+/g, ' ').trim();
}

async function readText(filePath) {
    return fs.promises.readFile(filePath, "utf8");
}

async function parseCSV(filePath) {
    return new Promise((resolve, reject) => {
        const rows = [];
        fs.createReadStream(filePath)
          .pipe(csv())
          .on("data", row => rows.push(row))
          .on("end", () => resolve(rows))
          .on("error", err => reject(err));
    });
}

async function parsePDF(filePath) {
    const data = await fs.promises.readFile(filePath);
    const parsed = await pdfParse(data);
    return clean(parsed.text);
}

async function parseDOCX(filePath) {
    const result = await mammoth.extractRawText({ path: filePath });
    return clean(result.value);
}

async function parseHTML(filePath) {
    const html = await readText(filePath);
    const $ = cheerio.load(html);
    const title = $("h1").first().text() || "";
    const desc = $("p").first().text() || "";
    const text = clean($("body").text());
    return {
        title: title || null,
        description: desc || null,
        text,
        wordCount: text.split(/\s+/).length
    };
}

async function parseXML(filePath) {
    const xml = await readText(filePath);
    return xml2js.parseStringPromise(xml, { explicitArray: false });
}

async function parseImage(filePath) {
    const worker = Tesseract.createWorker();
    await worker.load();
    await worker.loadLanguage("eng");
    await worker.initialize("eng");

    const { data } = await worker.recognize(filePath);
    await worker.terminate();

    let text = clean(data.text);
    return {
        text,
        wordCount: text.split(/\s+/).length
    };
}

async function extractFile(filePath, originalName, providedMime) {
    let mime = providedMime;
    if (!mime) {
        const ft = await FileType.fromFile(filePath);
        mime = ft ? ft.mime : null;
    }

    const ext = (path.extname(originalName) || "").toLowerCase();

    try {
        // ---- JSON ----
        if (mime === "application/json" || ext === ".json") {
            const txt = await readText(filePath);
            return { type: "json", payload: JSON.parse(txt) };
        }

        // ---- CSV ----
        if (mime === "text/csv" || ext === ".csv") {
            const rows = await parseCSV(filePath);
            return { type: "json", payload: rows[0] || {} };
        }

        // ---- PDF ----
        if (mime === "application/pdf" || ext === ".pdf") {
            const text = await parsePDF(filePath);
            return { type: "json", payload: { text, wordCount: text.split(/\s+/).length } };
        }

        // ---- DOCX ----
        if (ext === ".docx") {
            const text = await parseDOCX(filePath);
            return { type: "json", payload: { text, wordCount: text.split(/\s+/).length } };
        }

        // ---- HTML ----
        if (ext === ".html" || ext === ".htm") {
            return { type: "json", payload: await parseHTML(filePath) };
        }

        // ---- XML ----
        if (ext === ".xml" || mime === "application/xml") {
            return { type: "json", payload: await parseXML(filePath) };
        }

        // ---- IMAGE (OCR) ----
        if (mime && mime.startsWith("image/")) {
            return { type: "json", payload: await parseImage(filePath) };
        }

        // ============================================================
        // === PLAIN TEXT (WITH MIXED JSON DETECTION) — UPDATED PART ===
        // ============================================================

        const txt = await readText(filePath);
        const cleaned = clean(txt);

        // 1. Try to detect JSON block between ===JSON=== ... ===UNSTRUCTURED===
        let jsonPayload = null;
        const jsonMatch = txt.match(/===JSON===([\s\S]*?)===UNSTRUCTURED===/i);

        if (jsonMatch) {
            const rawJson = jsonMatch[1]
                .replace(/'/g, '"')       // convert single → double quotes
                .trim();

            try {
                jsonPayload = JSON.parse(rawJson);
                console.log("Extracted structured JSON from mixed TXT file");
            } catch (e) {
                console.log("Failed JSON parse in mixed TXT:", e.message);
            }
        }

        // 2. If JSON extracted → return that for schema inference
        if (jsonPayload) {
            return { type: "json", payload: jsonPayload };
        }

        // 3. Otherwise fallback to plain text metadata
        return {
            type: "json",
            payload: {
                text: cleaned,
                length: cleaned.length,
                wordCount: cleaned.split(/\s+/).length
            }
        };

    } catch (err) {
        return { type: "error", payload: { error: err.message } };
    }
}

module.exports = { extractFile };