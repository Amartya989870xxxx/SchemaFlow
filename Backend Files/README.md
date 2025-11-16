# Dynamic ETL Backend (Node.js + Express + MongoDB)

This project accepts many file types (JSON, CSV, PDF, DOCX, TXT, HTML, XML, image via OCR), extracts text/JSON, stores raw payloads, dynamically infers schemas, normalizes records, and exposes simple APIs.

## Requirements
- Node.js (v16+ recommended)
- npm
- MongoDB (local or Atlas)
- (Optional but recommended) Tesseract OCR binary for better OCR performance:
  - macOS: `brew install tesseract`
  - Ubuntu/Debian: `sudo apt-get install tesseract-ocr`
  - Windows: install from the Tesseract project releases

## Setup
1. Copy this folder to your machine.
2. Install packages:
   ```bash
   npm install
