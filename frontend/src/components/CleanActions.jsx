import React, { useState } from "react";
import axios from "axios";

export default function CleanActions({ dataset }) {
  const [loading, setLoading] = useState(false);
  const [msg, setMsg] = useState("");

  const runCleaning = async (auto) => {
    setLoading(true);
    setMsg("Processing...");

    try {
      const res = await axios.post(
        `http://localhost:3000/api/datasets/${dataset._id}/clean?auto=${auto}`
      );

      setMsg("Cleaning Completed!");

      // Auto download cleaned CSV
      window.open(res.data.cleaned_url, "_blank");

    } catch (err) {
      console.error(err);
      setMsg("Error during cleaning");
    }
    setLoading(false);
  };

  if (!dataset) return null;

  return (
    <div className="p-6 bg-white shadow-md rounded-xl border max-w-lg mx-auto mt-6">
      <h2 className="text-xl font-semibold mb-3">
        Dataset: {dataset.filename}
      </h2>

      <div className="flex gap-4 mt-4">
        <button
          onClick={() => runCleaning(false)}
          disabled={loading}
          className="bg-yellow-500 text-white px-4 py-2 rounded-lg hover:bg-yellow-600 disabled:bg-gray-500"
        >
          Start Cleaning
        </button>

        <button
          onClick={() => runCleaning(true)}
          disabled={loading}
          className="bg-green-600 text-white px-4 py-2 rounded-lg hover:bg-green-700 disabled:bg-gray-500"
        >
          Auto Clean
        </button>
      </div>

      {loading && <p className="mt-4 text-blue-600">Running Cleaner...</p>}
      {msg && <p className="mt-2 text-gray-700">{msg}</p>}
    </div>
  );
}
