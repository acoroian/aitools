import { useState } from "react";
import type { FilterRequest } from "~/lib/api";

interface Props {
  onFilter: (req: FilterRequest) => void;
  resultCount: number | null;
  onClearHighlight: () => void;
}

const FACILITY_TYPES = [
  { value: "home_health", label: "Home Health" },
  { value: "hospice", label: "Hospice" },
  { value: "snf", label: "Skilled Nursing" },
  { value: "icf", label: "Intermediate Care" },
  { value: "rcfe", label: "Residential Care (Elderly)" },
  { value: "daycare_center", label: "Daycare Center" },
  { value: "daycare_family", label: "Family Daycare" },
  { value: "clinic", label: "Clinic" },
  { value: "hospital", label: "Hospital" },
];

const COUNTIES = [
  "Alameda", "Butte", "Contra Costa", "Fresno", "Kern", "Kings",
  "Los Angeles", "Marin", "Merced", "Monterey", "Napa", "Orange",
  "Placer", "Riverside", "Sacramento", "San Bernardino", "San Diego",
  "San Francisco", "San Joaquin", "San Luis Obispo", "San Mateo",
  "Santa Barbara", "Santa Clara", "Santa Cruz", "Shasta", "Solano",
  "Sonoma", "Stanislaus", "Tulare", "Ventura", "Yolo",
];

export default function FilterPanel({ onFilter, resultCount, onClearHighlight }: Props) {
  const [expanded, setExpanded] = useState(true);
  const [selectedTypes, setSelectedTypes] = useState<string[]>([]);
  const [status, setStatus] = useState("");
  const [county, setCounty] = useState("");
  const [revenueMin, setRevenueMin] = useState("");
  const [revenueMax, setRevenueMax] = useState("");
  const [violMin, setViolMin] = useState("");
  const [violMax, setViolMax] = useState("");
  const [violRecent, setViolRecent] = useState("");
  const [severityMin, setSeverityMin] = useState("");
  const [hasIj, setHasIj] = useState(false);
  const [medicare, setMedicare] = useState(false);
  const [medicaid, setMedicaid] = useState(false);
  const [year, setYear] = useState("");

  const toggleType = (type: string) => {
    setSelectedTypes((prev) =>
      prev.includes(type) ? prev.filter((t) => t !== type) : [...prev, type]
    );
  };

  const handleApply = () => {
    const req: FilterRequest = {};
    if (selectedTypes.length > 0) req.facility_types = selectedTypes;
    if (status) req.license_status = status;
    if (county) req.county = county;
    if (revenueMin) req.gross_revenue_min = parseInt(revenueMin, 10);
    if (revenueMax) req.gross_revenue_max = parseInt(revenueMax, 10);
    if (violMin) req.violation_count_min = parseInt(violMin, 10);
    if (violMax) req.violation_count_max = parseInt(violMax, 10);
    if (violRecent) req.violation_count_12mo_min = parseInt(violRecent, 10);
    if (severityMin) req.max_severity_level_min = parseInt(severityMin, 10);
    if (hasIj) req.has_ij_12mo = true;
    if (medicare) req.certified_medicare = true;
    if (medicaid) req.certified_medicaid = true;
    if (year) req.year = parseInt(year, 10);
    onFilter(req);
  };

  const handleClear = () => {
    setSelectedTypes([]);
    setStatus("");
    setCounty("");
    setRevenueMin("");
    setRevenueMax("");
    setViolMin("");
    setViolMax("");
    setViolRecent("");
    setSeverityMin("");
    setHasIj(false);
    setMedicare(false);
    setMedicaid(false);
    setYear("");
    onClearHighlight();
  };

  return (
    <div style={{
      position: "absolute", bottom: 16, left: 16, zIndex: 10,
      background: "white", borderRadius: 8,
      boxShadow: "0 2px 8px rgba(0,0,0,0.15)",
      maxHeight: "80vh", overflowY: "auto",
      width: expanded ? 280 : "auto",
    }}>
      <div
        style={{
          padding: "10px 16px", cursor: "pointer",
          display: "flex", justifyContent: "space-between", alignItems: "center",
          borderBottom: expanded ? "1px solid #e5e7eb" : "none",
        }}
        onClick={() => setExpanded((e) => !e)}
      >
        <span style={{ fontSize: 14, fontWeight: 600 }}>Filters</span>
        <span style={{ fontSize: 12, color: "#9ca3af" }}>{expanded ? "▼" : "▶"}</span>
      </div>

      {expanded && (
        <div style={{ padding: "8px 16px 16px" }}>
          {/* Facility types */}
          <fieldset style={{ border: "none", padding: 0, margin: "0 0 10px" }}>
            <legend style={{ fontSize: 12, fontWeight: 600, marginBottom: 4 }}>Facility Type</legend>
            <div style={{ display: "flex", flexWrap: "wrap", gap: 4 }}>
              {FACILITY_TYPES.map((t) => (
                <button
                  key={t.value}
                  onClick={() => toggleType(t.value)}
                  style={{
                    fontSize: 11, padding: "2px 8px", borderRadius: 12, cursor: "pointer",
                    border: selectedTypes.includes(t.value) ? "1px solid #3b82f6" : "1px solid #d1d5db",
                    background: selectedTypes.includes(t.value) ? "#eff6ff" : "white",
                    color: selectedTypes.includes(t.value) ? "#1d4ed8" : "#374151",
                  }}
                >
                  {t.label}
                </button>
              ))}
            </div>
          </fieldset>

          {/* Status + County row */}
          <div style={{ display: "flex", gap: 8, marginBottom: 8 }}>
            <label style={{ flex: 1, fontSize: 12 }}>
              Status
              <select value={status} onChange={(e) => setStatus(e.target.value)}
                style={{ display: "block", width: "100%", marginTop: 2, padding: "4px 6px", fontSize: 12 }}>
                <option value="">Any</option>
                <option value="active">Active</option>
                <option value="pending">Pending</option>
                <option value="expired">Expired</option>
                <option value="closed">Closed</option>
              </select>
            </label>
            <label style={{ flex: 1, fontSize: 12 }}>
              County
              <select value={county} onChange={(e) => setCounty(e.target.value)}
                style={{ display: "block", width: "100%", marginTop: 2, padding: "4px 6px", fontSize: 12 }}>
                <option value="">Any</option>
                {COUNTIES.map((c) => <option key={c} value={c}>{c}</option>)}
              </select>
            </label>
          </div>

          {/* Revenue range */}
          <fieldset style={{ border: "none", padding: 0, margin: "0 0 8px" }}>
            <legend style={{ fontSize: 12, fontWeight: 600, marginBottom: 4 }}>Revenue ($)</legend>
            <div style={{ display: "flex", gap: 8 }}>
              <input type="number" value={revenueMin} onChange={(e) => setRevenueMin(e.target.value)}
                placeholder="Min"
                style={{ flex: 1, padding: "4px 6px", fontSize: 12, boxSizing: "border-box" }} />
              <input type="number" value={revenueMax} onChange={(e) => setRevenueMax(e.target.value)}
                placeholder="Max"
                style={{ flex: 1, padding: "4px 6px", fontSize: 12, boxSizing: "border-box" }} />
            </div>
          </fieldset>

          {/* Violation filters */}
          <fieldset style={{ border: "none", padding: 0, margin: "0 0 8px" }}>
            <legend style={{ fontSize: 12, fontWeight: 600, marginBottom: 4 }}>Violations</legend>
            <div style={{ display: "flex", gap: 8, marginBottom: 4 }}>
              <input type="number" value={violMin} onChange={(e) => setViolMin(e.target.value)}
                placeholder="Min total"
                style={{ flex: 1, padding: "4px 6px", fontSize: 12, boxSizing: "border-box" }} />
              <input type="number" value={violMax} onChange={(e) => setViolMax(e.target.value)}
                placeholder="Max total"
                style={{ flex: 1, padding: "4px 6px", fontSize: 12, boxSizing: "border-box" }} />
            </div>
            <div style={{ display: "flex", gap: 8, marginBottom: 4 }}>
              <input type="number" value={violRecent} onChange={(e) => setViolRecent(e.target.value)}
                placeholder="Min (last 12mo)"
                style={{ flex: 1, padding: "4px 6px", fontSize: 12, boxSizing: "border-box" }} />
              <input type="number" value={severityMin} onChange={(e) => setSeverityMin(e.target.value)}
                placeholder="Min severity (1-10)"
                style={{ flex: 1, padding: "4px 6px", fontSize: 12, boxSizing: "border-box" }} />
            </div>
            <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12, cursor: "pointer" }}>
              <input type="checkbox" checked={hasIj} onChange={(e) => setHasIj(e.target.checked)} />
              Immediate jeopardy (last 12mo)
            </label>
          </fieldset>

          {/* Certification + Year */}
          <div style={{ display: "flex", gap: 8, marginBottom: 8, flexWrap: "wrap" }}>
            <label style={{ display: "flex", alignItems: "center", gap: 4, fontSize: 12, cursor: "pointer" }}>
              <input type="checkbox" checked={medicare} onChange={(e) => setMedicare(e.target.checked)} />
              Medicare
            </label>
            <label style={{ display: "flex", alignItems: "center", gap: 4, fontSize: 12, cursor: "pointer" }}>
              <input type="checkbox" checked={medicaid} onChange={(e) => setMedicaid(e.target.checked)} />
              Medicaid
            </label>
            <label style={{ fontSize: 12, marginLeft: "auto" }}>
              Year
              <input type="number" value={year} onChange={(e) => setYear(e.target.value)}
                placeholder="e.g. 2022" min={2015} max={2030}
                style={{ width: 72, marginLeft: 4, padding: "4px 6px", fontSize: 12, boxSizing: "border-box" }} />
            </label>
          </div>

          {/* Actions */}
          <div style={{ display: "flex", gap: 8 }}>
            <button onClick={handleApply}
              style={{
                flex: 1, padding: "6px 0", background: "#3b82f6", color: "white",
                border: "none", borderRadius: 4, cursor: "pointer", fontSize: 13,
              }}>
              Apply
            </button>
            <button onClick={handleClear}
              style={{
                padding: "6px 12px", background: "white", color: "#6b7280",
                border: "1px solid #d1d5db", borderRadius: 4, cursor: "pointer", fontSize: 13,
              }}>
              Clear
            </button>
          </div>

          {resultCount !== null && (
            <p style={{ margin: "8px 0 0", fontSize: 11, color: "#6b7280", textAlign: "center" }}>
              {resultCount.toLocaleString()} facilities matched
            </p>
          )}
        </div>
      )}
    </div>
  );
}
