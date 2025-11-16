const hiddenInput = document.getElementById("hiddenFileInput");
const selectBtn = document.getElementById("fileSelectorBtn");
const fileName = document.getElementById("selectedFileName");
const metaBox = document.getElementById("metadataBox");
const uploadBtn = document.getElementById("uploadBtn");
const statusText = document.getElementById("uploadStatus");

// Fix double-click issue on Mac Safari/Chrome
selectBtn.addEventListener("click", () => {
  hiddenInput.value = ""; // resets file input reliably
  hiddenInput.click();
});

hiddenInput.addEventListener("change", () => {
  fileName.textContent = hiddenInput.files.length
    ? hiddenInput.files[0].name
    : "No file chosen";
});

uploadBtn.addEventListener("click", async () => {
  statusText.textContent = "Uploading...";
  statusText.className = "";

  if (!hiddenInput.files.length) {
    statusText.textContent = "Please select a file.";
    statusText.className = "upload-error";
    return;
  }

  const form = new FormData();
  form.append("file", hiddenInput.files[0]);  // MUST BE "file"

  try {
    const meta = JSON.parse(metaBox.value || "{}");
    form.append("metadata", JSON.stringify(meta));
  } catch {
    statusText.textContent = "Invalid JSON metadata!";
    statusText.className = "upload-error";
    return;
  }

  try {
    // FIXED URL (must include /api)
    const uploadRes = await fetch("http://localhost:3000/api/upload", {
      method: "POST",
      body: form
    });

    const data = await uploadRes.json();
    if (data.error) {
      statusText.textContent = data.error;
      statusText.className = "upload-error";
    } else {
      statusText.textContent = "Upload successful!";
      statusText.className = "upload-success";
    }

  } catch (err) {
    console.error(err);
    statusText.textContent = "Network error.";
    statusText.className = "upload-error";
  }
});