import React, { useState } from "react";

const apiUrl = import.meta.env?.VITE_API_URL || "http://localhost:5000";

export default function CleanActions({ dataset }) {
  const [loading, setLoading] = useState(false);
  const [autoClean, setAutoClean] = useState(false);
  const [cleaningStage, setCleaningStage] = useState("");
  const [isCompleted, setIsCompleted] = useState(false);
  const [cleanedUrl, setCleanedUrl] = useState("");
  const [showPreview, setShowPreview] = useState(false);
  const [showIssues, setShowIssues] = useState(false);
  const [cleanedIssues, setCleanedIssues] = useState([]);


  const runCleaning = async () => {
    setLoading(true);
    setIsCompleted(false);
    setCleaningStage("Initializing...");

    try {
      // Simulate stages
      await new Promise(resolve => setTimeout(resolve, 1000));
      setCleaningStage("Analyzing dataset...");

      await new Promise(resolve => setTimeout(resolve, 1500));
      setCleaningStage("Detecting issues...");

      await new Promise(resolve => setTimeout(resolve, 1500));
      setCleaningStage("Cleaning data...");

      const response = await fetch(
        `${apiUrl}/datasets/${dataset._id}/clean?auto=${autoClean}`,
        { method: "POST" }
      );

      if (!response.ok) throw new Error("Cleaning failed");

      const data = await response.json();
      setCleanedIssues(data.dataset.issues || []);

      setCleaningStage("Finalizing...");
      await new Promise(resolve => setTimeout(resolve, 800));

      setCleanedUrl(data.cleaned_url);
      setIsCompleted(true);
      setCleaningStage("");
    } catch (err) {
      console.error(err);
      setCleaningStage("Error during cleaning");
    } finally {
      setLoading(false);
    }
  };

  const handleDownload = () => {
    if (cleanedUrl) {
      window.open(cleanedUrl, "_blank");
    }
  };

  const generateReport = () => {
    alert("Generating comprehensive cleaning report...");
    // Implement report generation
  };

  if (!dataset) return null;

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      <div className="max-w-4xl mx-auto">
        {/* Dataset Info Card */}
        <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-6 mb-6">
          <div className="flex items-start justify-between">
            <div className="flex-1">
              <div className="flex items-center gap-3 mb-2">
                <div className="w-10 h-10 bg-blue-100 rounded-lg flex items-center justify-center">
                  <svg className="w-6 h-6 text-blue-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                  </svg>
                </div>
                <div>
                  <h2 className="text-xl font-semibold text-gray-900">{dataset.filename}</h2>
                  <p className="text-sm text-gray-500">{dataset.row_count?.toLocaleString()} rows · {Object.keys(dataset.schema || {}).length} columns</p>
                </div>
              </div>
            </div>

            <button
              onClick={() => setShowPreview(!showPreview)}
              className="px-4 py-2 text-sm font-medium text-blue-600 hover:bg-blue-50 rounded-lg transition-colors flex items-center gap-2"
            >
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z" />
              </svg>
              {showPreview ? "Hide Preview" : "View Preview"}
            </button>
          </div>

          {/* Preview Section */}
          {showPreview && dataset.preview && (
            <div className="mt-6 pt-6 border-t border-gray-200">
              <h3 className="text-sm font-semibold text-gray-900 mb-3">Data Preview</h3>
              <div className="overflow-x-auto rounded-lg border border-gray-200">
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="bg-gray-50">
                    <tr>
                      {Object.keys(dataset.preview[0] || {}).map((col, idx) => (
                        <th key={idx} className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          {col}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {dataset.preview.slice(0, 5).map((row, rowIdx) => (
                      <tr key={rowIdx} className="hover:bg-gray-50">
                        {Object.values(row).map((val, colIdx) => (
                          <td key={colIdx} className="px-4 py-3 text-sm text-gray-900 whitespace-nowrap">
                            {val || <span className="text-gray-400 italic">null</span>}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>

        {/* Cleaning Controls */}
        {!isCompleted && (
          <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-6">
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Data Cleaning Options</h3>

            {/* Auto Clean Checkbox */}
            <label className="flex items-center gap-3 p-4 border border-gray-200 rounded-xl hover:bg-gray-50 cursor-pointer mb-6">
              <input
                type="checkbox"
                checked={autoClean}
                onChange={(e) => setAutoClean(e.target.checked)}
                className="w-5 h-5 text-blue-600 border-gray-300 rounded focus:ring-2 focus:ring-blue-500"
                disabled={loading}
              />
              <div className="flex-1">
                <div className="font-medium text-gray-900">Auto Clean Mode</div>
                <div className="text-sm text-gray-500">Automatically detect and fix common data issues</div>
              </div>
              {autoClean && (
                <span className="px-3 py-1 bg-green-100 text-green-700 text-xs font-medium rounded-full">
                  Enabled
                </span>
              )}
            </label>

            {/* Start Cleaning Button */}
            <button
              onClick={runCleaning}
              disabled={loading}
              className={`w-full py-4 px-6 rounded-xl font-semibold transition-all flex items-center justify-center gap-3 ${loading
                  ? 'bg-gray-100 text-gray-400 cursor-not-allowed'
                  : 'bg-blue-600 text-white hover:bg-blue-700 active:bg-blue-800'
                }`}
            >
              {loading ? (
                <>
                  <svg className="animate-spin h-5 w-5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                  </svg>
                  {cleaningStage}
                </>
              ) : (
                <>
                  <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M14.828 14.828a4 4 0 01-5.656 0M9 10h.01M15 10h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                  Start Cleaning Process
                </>
              )}
            </button>

            {/* Progress Indicator */}
            {loading && (
              <div className="mt-6 p-4 bg-blue-50 border border-blue-200 rounded-xl">
                <div className="flex items-center gap-3 mb-3">
                  <div className="w-8 h-8 bg-blue-600 rounded-full flex items-center justify-center">
                    <svg className="w-4 h-4 text-white animate-pulse" fill="currentColor" viewBox="0 0 20 20">
                      <path fillRule="evenodd" d="M11.3 1.046A1 1 0 0112 2v5h4a1 1 0 01.82 1.573l-7 10A1 1 0 018 18v-5H4a1 1 0 01-.82-1.573l7-10a1 1 0 011.12-.38z" clipRule="evenodd" />
                    </svg>
                  </div>
                  <div>
                    <div className="text-sm font-semibold text-blue-900">Processing Your Dataset</div>
                    <div className="text-sm text-blue-700">{cleaningStage}</div>
                  </div>
                </div>
                <div className="w-full bg-blue-200 rounded-full h-2 overflow-hidden">
                  <div className="h-full bg-blue-600 rounded-full animate-pulse" style={{ width: '70%' }}></div>
                </div>
              </div>
            )}
          </div>
        )}

        {/* Completion Actions */}
        {isCompleted && (
          <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-6">
            {/* Success Message */}
            <div className="flex items-center gap-3 mb-6 p-4 bg-green-50 border border-green-200 rounded-xl">
              <div className="w-10 h-10 bg-green-500 rounded-full flex items-center justify-center">
                <svg className="w-6 h-6 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                </svg>
              </div>
              <div>
                <h3 className="text-lg font-semibold text-green-900">Cleaning Completed!</h3>
                <p className="text-sm text-green-700">Your dataset has been successfully cleaned and is ready to download</p>
              </div>
            </div>

            {/* Action Buttons */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              {/* Download Button */}
              <button
                onClick={handleDownload}
                className="flex flex-col items-center gap-3 p-6 border-2 border-gray-200 rounded-xl hover:border-blue-500 hover:bg-blue-50 transition-all group"
              >
                <div className="w-12 h-12 bg-blue-100 rounded-xl flex items-center justify-center group-hover:bg-blue-500 transition-colors">
                  <svg className="w-6 h-6 text-blue-600 group-hover:text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
                  </svg>
                </div>
                <div className="text-center">
                  <div className="font-semibold text-gray-900 mb-1">Download</div>
                  <div className="text-xs text-gray-500">Get cleaned CSV</div>
                </div>
              </button>

              {/* View Issues Button */}
              <button
                onClick={() => setShowIssues(!showIssues)}
                className="flex flex-col items-center gap-3 p-6 border-2 border-gray-200 rounded-xl hover:border-orange-500 hover:bg-orange-50 transition-all group"
              >
                <div className="w-12 h-12 bg-orange-100 rounded-xl flex items-center justify-center group-hover:bg-orange-500 transition-colors">
                  <svg className="w-6 h-6 text-orange-600 group-hover:text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                  </svg>
                </div>
                <div className="text-center">
                  <div className="font-semibold text-gray-900 mb-1">View Issues</div>
                  <div className="text-xs text-gray-500">See what was fixed</div>
                </div>
              </button>

              {/* Generate Report Button */}
              <button
                onClick={generateReport}
                className="flex flex-col items-center gap-3 p-6 border-2 border-gray-200 rounded-xl hover:border-purple-500 hover:bg-purple-50 transition-all group"
              >
                <div className="w-12 h-12 bg-purple-100 rounded-xl flex items-center justify-center group-hover:bg-purple-500 transition-colors">
                  <svg className="w-6 h-6 text-purple-600 group-hover:text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 17v-2m3 2v-4m3 4v-6m2 10H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                  </svg>
                </div>
                <div className="text-center">
                  <div className="font-semibold text-gray-900 mb-1">Generate Report</div>
                  <div className="text-xs text-gray-500">Full analysis report</div>
                </div>
              </button>
            </div>

            {/* Issues List */}
            {showIssues && (
              <div className="mt-6 pt-6 border-t border-gray-200">
                <h4 className="text-sm font-semibold text-gray-900 mb-4">Issues Found & Fixed</h4>
                {cleanedIssues && cleanedIssues.length > 0 ? (
                  <div className="space-y-3">
                    {cleanedIssues.map((issue, idx) => (
                      <div
                        key={idx}
                        className="relative flex gap-4 p-5 rounded-xl border border-gray-200 bg-white shadow-sm hover:shadow-md transition-all duration-200"
                      >

                        {/* Severity Indicator Sidebar */}
                        <div
                          className={`absolute left-0 top-0 h-full w-1 rounded-l-xl
      ${issue.severity === "high"
                              ? "bg-red-600"
                              : issue.severity === "medium"
                                ? "bg-orange-500"
                                : "bg-yellow-500"
                            }
    `}
                        />

                        {/* Icon Box */}
                        <div
                          className={`w-11 h-11 flex items-center justify-center rounded-xl border
      ${issue.severity === "high"
                              ? "bg-red-50 border-red-100 text-red-600"
                              : issue.severity === "medium"
                                ? "bg-orange-50 border-orange-100 text-orange-600"
                                : "bg-yellow-50 border-yellow-100 text-yellow-600"
                            }
    `}
                        >
                          <svg
                            className="w-5 h-5"
                            fill="none"
                            stroke="currentColor"
                            viewBox="0 0 24 24"
                          >
                            <path
                              strokeLinecap="round"
                              strokeLinejoin="round"
                              strokeWidth={2}
                              d="M12 9v2m0 4h.01M5.062 19h13.876c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.33 16c-.77 1.333.192 3 1.732 3z"
                            />
                          </svg>
                        </div>

                        {/* Content */}
                        <div className="flex-1">
                          {/* TOP ROW */}
                          <div className="flex items-center gap-3 mb-1">
                            <span className="text-sm font-semibold text-gray-900">
                              {issue.column}
                            </span>

                            <span
                              className={`px-2 py-0.5 text-[10px] font-bold rounded-full tracking-wide
          ${issue.severity === "high"
                                  ? "bg-red-100 text-red-700"
                                  : issue.severity === "medium"
                                    ? "bg-orange-100 text-orange-700"
                                    : "bg-yellow-100 text-yellow-700"
                                }
        `}
                            >
                              {issue.severity.toUpperCase()}
                            </span>
                          </div>

                          {/* DESCRIPTION */}
                          <p className="text-sm text-gray-700 mb-3">{issue.description}</p>

                          {/* KEY : VALUE GRID */}
                          <div className="grid grid-cols-2 gap-y-2 gap-x-4 text-xs">

                            {/* Issue Type */}
                            <div className="flex flex-col">
                              <span className="text-gray-500 font-medium">issue_type</span>
                              <span className="text-gray-800 font-mono text-[13px]">
                                {issue.issue_type || "-"}
                              </span>
                            </div>

                            {/* Severity */}
                            <div className="flex flex-col">
                              <span className="text-gray-500 font-medium">severity</span>
                              <span className="text-gray-800 font-mono text-[13px]">
                                {issue.severity}
                              </span>
                            </div>

                          </div>
                        </div>
                      </div>

                    ))}
                  </div>
                ) : (
                  <div className="p-8 text-center bg-gray-50 rounded-lg border border-gray-200">
                    <div className="w-16 h-16 bg-green-100 rounded-full flex items-center justify-center mx-auto mb-3">
                      <svg className="w-8 h-8 text-green-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                      </svg>
                    </div>
                    <p className="text-sm font-medium text-gray-900 mb-1">No Issues Found</p>
                    <p className="text-sm text-gray-500">Your dataset is clean and ready to use!</p>
                  </div>
                )}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}