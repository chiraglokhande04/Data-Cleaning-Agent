import React, { useState } from "react";
import axios from "axios";

export default function UploadDataset({ onUploaded }) {
  const [file, setFile] = useState(null);
  const [loading, setLoading] = useState(false);

  const handleUpload = async () => {
    if (!file) return;

    setLoading(true);
    const form = new FormData();
    form.append("file", file);

    try {
      const res = await axios.post("http://localhost:3000/api/files/upload", form, {
        headers: { "Content-Type": "multipart/form-data" },
      });

      onUploaded(res.data.dataset); // send dataset metadata to parent

    } catch (err) {
      console.error(err);
      alert("Upload failed");
    }
    setLoading(false);
  };

  return (
    <div className="p-6 border rounded-xl bg-white shadow-md max-w-lg mx-auto">
      <h2 className="text-xl font-semibold mb-4">Upload CSV File</h2>

      <input
        type="file"
        accept=".csv"
        onChange={(e) => setFile(e.target.files[0])}
        className="mb-4 block w-full text-sm"
      />

      <button
        onClick={handleUpload}
        disabled={loading}
        className="bg-blue-600 text-white px-5 py-2 rounded-lg hover:bg-blue-700 disabled:bg-gray-500"
      >
        {loading ? "Uploading..." : "Upload"}
      </button>
    </div>
  );
}
