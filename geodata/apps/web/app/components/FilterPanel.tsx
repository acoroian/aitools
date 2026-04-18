import { useState } from "react";
import type { FilterRequest } from "~/lib/api";

interface Props {
  onFilter: (req: FilterRequest) => void;
  resultCount: number | null;
}

export default function FilterPanel({ onFilter, resultCount }: Props) {
  const [status, setStatus] = useState("active");
  const [revenueMin, setRevenueMin] = useState("");
  const [violMax, setViolMax] = useState("");
  const [medicare, setMedicare] = useState(false);

  const handleApply = () => {
    const req: FilterRequest = { license_status: status || undefined };
    if (revenueMin) req.gross_revenue_min = parseInt(revenueMin, 10);
    if (violMax) req.violation_count_max = parseInt(violMax, 10);
    if (medicare) req.certified_medicare = true;
    onFilter(req);
  };

  return (
    <div style={{
      position: "absolute", bottom: 32, left: 16, zIndex: 10,
      background: "white", borderRadius: 8, padding: "12px 16px",
      boxShadow: "0 2px 8px rgba(0,0,0,0.15)", minWidth: 220,
    }}>
      <h3 style={{ margin: "0 0 10px", fontSize: 14, fontWeight: 600 }}>Filters</h3>

      <label style={{ display: "block", marginBottom: 8, fontSize: 12 }}>
        License Status
        <select value={status} onChange={(e) => setStatus(e.target.value)}
          style={{ display: "block", width: "100%", marginTop: 2, padding: "4px 6px", fontSize: 12 }}>
          <option value="">Any</option>
          <option value="active">Active</option>
          <option value="pending">Pending</option>
          <option value="expired">Expired</option>
        </select>
      </label>

      <label style={{ display: "block", marginBottom: 8, fontSize: 12 }}>
        Min Revenue ($)
        <input type="number" value={revenueMin} onChange={(e) => setRevenueMin(e.target.value)}
          placeholder="e.g. 500000"
          style={{ display: "block", width: "100%", marginTop: 2, padding: "4px 6px", fontSize: 12, boxSizing: "border-box" }} />
      </label>

      <label style={{ display: "block", marginBottom: 8, fontSize: 12 }}>
        Max Violations
        <input type="number" value={violMax} onChange={(e) => setViolMax(e.target.value)}
          placeholder="e.g. 5"
          style={{ display: "block", width: "100%", marginTop: 2, padding: "4px 6px", fontSize: 12, boxSizing: "border-box" }} />
      </label>

      <label style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 10, fontSize: 12, cursor: "pointer" }}>
        <input type="checkbox" checked={medicare} onChange={(e) => setMedicare(e.target.checked)} />
        Medicare certified only
      </label>

      <button onClick={handleApply}
        style={{ width: "100%", padding: "6px 0", background: "#3b82f6", color: "white", border: "none", borderRadius: 4, cursor: "pointer", fontSize: 13 }}>
        Apply
      </button>

      {resultCount !== null && (
        <p style={{ margin: "8px 0 0", fontSize: 11, color: "#6b7280", textAlign: "center" }}>
          {resultCount.toLocaleString()} facilities matched
        </p>
      )}
    </div>
  );
}
