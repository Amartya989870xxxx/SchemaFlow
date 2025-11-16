
---


# 🎨 Dynamic ETL Frontend

### AuraVerse Hackathon Project  
**User Dashboard for File Ingestion, Extraction & Schema Evolution**

---

## 📌 Overview

The **Dynamic ETL Frontend** is a lightweight, responsive dashboard that interacts with the backend ETL engine. It provides a simple UI for uploading files, inspecting extracted content, and tracking schema evolution in real time.

The frontend allows users to:

- ✔ Upload ANY type of file (PDF, CSV, DOCX, TXT, images, JSON, HTML)  
- ✔ View real-time extraction results  
- ✔ Monitor schema versions & differences  
- ✔ Track schema evolution over time  
- ✔ Inspect raw & normalized records  
- ✔ View analytics about processed data  

Built entirely with **HTML, CSS, JavaScript**.

---

# 🖥️ Features

## 🌐 1️⃣ File Upload Interface

- Drag-and-drop or click-to-upload  
- Supports all formats handled by backend  
  (PDF, CSV, DOCX, JSON, HTML, TXT, Images via OCR)  
- Shows:
  - Upload progress  
  - Extraction preview  
  - Detected content type  
  - Parsed text/rows  
  - Normalized output  
  - Schema version used  

---

## 📊 2️⃣ Dashboard Analytics

The dashboard fetches backend stats & visualizes:

- Total uploaded files  
- Raw records count  
- Normalized records count  
- Latest schema version  
- All schema versions  
- File format distribution  
- Upload success/failure count  
- Timestamp of latest ingestion  

Frontend uses minimal vanilla JS + simple charts (optional).

---

## 📑 3️⃣ Schema Viewer

A clean viewer to understand how schema evolves.

You can:

- View the **latest schema**  
- Explore **all previous versions**  
- See **exact changes** in fields/types  
- Expand nested fields  
- Inspect schema metadata (timestamp, sample size)  

---

## 🗂️ 4️⃣ Records Browser

Browse through stored data:

- **Raw** extracted data  
- **Normalized** canonical data  
- Schema version for each record  
- Pagination for large results  
- Clean UI to explore data quality  

---

## 🚦 5️⃣ Error, Status & Toast Notifications

Frontend shows:

- Upload errors & API errors  
- Extraction failures  
- Schema drift detected  
- Backend offline warnings  
- Success toasts for processed files  

UI stays clean & minimal.

---

# 🔌 API Connections

The frontend uses the following API routes from backend:

| Feature              | API Endpoint               |
|---------------------|----------------------------|
| File Upload         | `POST /api/upload`         |
| Latest Schema       | `GET /api/schema/latest`   |
| All Schema Versions | `GET /api/schema/versions` |
| Raw Records         | `GET /api/raw`             |
| Normalized Records  | `GET /api/normalized`      |
| Pipeline Stats      | `GET /api/stats`           |

Communication uses **JavaScript fetch()** with JSON.

---

# 🚀 Installation & Running the Project

## 1️⃣ Clone the repository

```sh
git clone https://github.com/yourrepo/dynamic-etl.git
cd frontend
````

## 2️⃣ Run a local static server

```sh
npx serve
```

Or simply open `index.html` in the browser.

## 3️⃣ Start the backend service

```sh
cd backend
npm install
node server.js
```

Expected logs:

```
Server listening on 3000
Connected to MongoDB
```

## 4️⃣ Open the frontend

Visit:

```
http://localhost:3000
```

(or whichever port is used)

---

# 🎯 Tech Stack

* **HTML5**
* **CSS**
* **JavaScript**
* **Fetch API**
* **Responsive Layout**

Zero external frameworks → deploy anywhere.

---

# 🏆 Why This Frontend Stands Out

* ✔ Super clean UI
* ✔ Zero dependencies
* ✔ Works instantly — no build steps
* ✔ Visualizes schema evolution clearly
* ✔ Designed specifically for the Dynamic ETL backend
* ✔ Great for demo, hackathon, data engineering showcase

---

```
