# Dynamic ETL Backend (Node.js + Express + MongoDB)
---

## 📌 Overview

The backend is designed to handle messy, unpredictable, and evolving data by:

* Accepting **any file format** (PDF, CSV, DOCX, TXT, HTML, JSON, images, mixed-format files)
* Extracting meaningful data intelligently
* Generating schemas automatically
* Creating **new schema versions** on structural changes
* Storing both **raw and normalized data**
* Tracking schema evolution internally

It forms the **core ETL engine**, handling all extraction, transformation, and storage operations.

---

## 🏗️ Backend Architecture

```
        ┌──────────────────┐
        │  File Ingestion  │
        └────────┬─────────┘
                 ↓
        ┌────────────────────────┐
        │  Content Classifier    │
        └────────┬──────────────┘
                 ↓
  ┌─────────────────────────────────┐
  │   Multi-Format Extraction Layer │
  └───────┬───────────────┬────────┘
          ↓               ↓
   PDF Extractor      CSV Extractor    DOCX Extractor   Image OCR   HTML/Text Parser
          ↓               ↓                    ↓             ↓            ↓
  ────────────────────────────── Extracted Unified Data ────────────────────────────────

                 ↓
        ┌────────────────────┐
        │ Dynamic Schema Gen │
        └────────┬───────────┘
                 ↓
        ┌────────────────────┐
        │ Schema Drift Check │
        └────────┬───────────┘
                 ↓
        ┌────────────────────┐
        │ Schema Versioning  │
        └────────┬───────────┘
                 ↓
     ┌────────────────────────────┐
     │ Raw + Normalized Storage   │
     └────────────────────────────┘
```

---

## 🎯 Key Backend Features

### 1️⃣ Accepts Any File Format

* Uses **content-based detection**, NOT extension-based
* Supported formats:

  * PDFs, CSVs, DOCX, TXT, HTML, JSON, Images
  * Mixed-format files (e.g., HTML + images + JSON)
* Detects format via MIME type, magic bytes, and content patterns

---

### 2️⃣ Multi-Layer Extraction

* Specialized extractors per format:

  * **PDF:** text + metadata
  * **CSV:** rows + headers
  * **DOCX:** paragraphs + tables
  * **HTML:** cleaned text + tags
  * **Images:** OCR text
* Mixed files are processed segment-by-segment
* Extracted data **merged into a unified JSON object**

---

### 3️⃣ Dynamic Schema Inference

* Scans extracted JSON-like data
* Detects fields, data types, optional vs required, nested structures
* Handles inconsistent fields and type variations
* Example inferred schema:

```json
{
  "title": "string",
  "amount": "float",
  "timestamp": "datetime",
  "images_text": "array"
}
```

---

### 4️⃣ Schema Drift Detection

* Compares new schema to the latest stored version
* Detects:

  * Added or removed fields
  * Data type changes
  * Nested structure changes
* Creates **new schema version** automatically when changes occur

---

### 5️⃣ Schema Version Control

* Stores every schema version with:

  * Version number
  * Schema structure
  * Timestamp
  * Diff from previous version
* Ensures **safe storage**, but **does not provide full backward query compatibility** yet
* Example version history:

```
v1 → name, email  
v2 → + html_text  
v3 → + ocr_results  
v4 → data type change in "amount"
```

---

### 6️⃣ Raw + Normalized Storage

* **Raw Storage:** exact uploaded content + extraction outputs
* **Normalized Storage:** data cleaned and transformed according to inferred schema
* **Schema Metadata:** current version details
* **Schema History:** tracks schema versions internally

---

### 7️⃣ Error Handling & Fault Tolerance

* Failed files logged for retries
* Raw content stored for debugging
* Extraction errors do **not block ingestion**

---

## 🚀 Installation

```bash
git clone https://github.com/yourrepo/dynamic-etl.git
cd backend
npm install
node app.js
```

---

## 📌 API Endpoints

* **POST /upload** → Upload a file
* **GET /schema/latest** → Get latest schema version
* **GET /schema/versions** → Get internal schema history
* **GET /records** → Fetch normalized stored records
* **GET /stats** → Pipeline statistics

---

## 🧩 Tech Stack (Backend Only)

* **Node.js + Express** → backend server
* **Multer** → file uploads
* **pdf-parse** → PDF extraction
* **PapaParse** → CSV parsing
* **Tesseract.js** → OCR for images
* **Mammoth** → DOCX extraction
* **Cheerio** → HTML parsing
* **MongoDB** → dynamic storage + versioning

---

## ⚡ Backend Limitations (Current)

* ❌ No support for `.md` markdown
* ❌ No fragment-level counts, offsets, or key-value metadata
* ❌ Normalization is basic; mixed-type handling is limited
* ❌ No DB compatibility metadata or suggested indexes
* ❌ Schema migration / backward query support is partial
* ❌ No LLM / natural language query interface
* ❌ Minimal logging & security
* ❌ No stress/performance tests for large/concurrent uploads

---

## 🏆 Why the Backend Stands Out

* Handles any data format, including mixed files
* Fully automated **dynamic schema generation**
* Maintains **internal schema version history**
* Stores **raw + normalized data** safely
* Acts as the **core ETL engine** for evolving, unstructured datasets

---

