import React, { useState } from "react";
import UploadDataset from "./components/UploadDataset";
import CleanActions from "./components/CleanActions";

export default function App() {
  const [dataset, setDataset] = useState(null);

  return (
    <div className="min-h-screen bg-gray-100 p-8">
      {!dataset ? (
        <UploadDataset onUploaded={(data) => setDataset(data)} />
      ) : (
        <CleanActions dataset={dataset} />
      )}
    </div>
  );
}
