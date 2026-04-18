interface Facility {
  id: string;
  name: string;
  type: string;
  subtype?: string;
  address?: string;
  city?: string;
  county?: string;
  zip?: string;
  phone?: string;
  license_status?: string;
  license_number?: string;
  license_expiry?: string;
  certified_medicare: boolean;
  certified_medicaid: boolean;
  cdph_id?: string;
  cms_npi?: string;
  oshpd_id?: string;
  cdss_id?: string;
  financials: Financial[];
  violations: Violation[];
}

interface Financial {
  year: number;
  source: string;
  gross_revenue: number | null;
  net_revenue: number | null;
  total_expenses: number | null;
  medicare_revenue: number | null;
  medicaid_revenue: number | null;
  private_revenue: number | null;
  total_visits: number | null;
  total_patients: number | null;
}

interface Violation {
  survey_date: string | null;
  source: string;
  deficiency_tag: string | null;
  category: string | null;
  severity: string | null;
  scope: string | null;
  description: string | null;
  resolved: boolean;
  resolved_date: string | null;
}

interface Props {
  facility: Facility;
  onClose: () => void;
}

function formatCurrency(cents: number | null): string {
  if (cents == null) return "N/A";
  return `$${cents.toLocaleString()}`;
}

export default function FacilityDetail({ facility, onClose }: Props) {
  return (
    <div style={{
      position: "absolute", top: 16, right: 16, zIndex: 10,
      background: "white", borderRadius: 8, padding: 0,
      boxShadow: "0 2px 12px rgba(0,0,0,0.2)", width: 360,
      maxHeight: "calc(100vh - 32px)", overflowY: "auto",
    }}>
      {/* Header */}
      <div style={{
        padding: "16px 16px 12px", borderBottom: "1px solid #e5e7eb",
        position: "sticky", top: 0, background: "white", borderRadius: "8px 8px 0 0",
      }}>
        <button onClick={onClose}
          style={{
            position: "absolute", top: 12, right: 12,
            background: "none", border: "none", cursor: "pointer",
            fontSize: 20, color: "#6b7280", lineHeight: 1,
          }}>
          ×
        </button>
        <h2 style={{ margin: 0, fontSize: 16, fontWeight: 600, paddingRight: 24 }}>
          {facility.name}
        </h2>
        <p style={{ margin: "4px 0 0", fontSize: 13, color: "#6b7280", textTransform: "capitalize" }}>
          {facility.type.replace(/_/g, " ")}
          {facility.subtype && ` · ${facility.subtype.replace(/_/g, " ")}`}
        </p>
      </div>

      {/* Details */}
      <div style={{ padding: "12px 16px" }}>
        {/* Address & Contact */}
        <section style={{ marginBottom: 16 }}>
          <h3 style={sectionHeader}>Location & Contact</h3>
          <dl style={dl}>
            <Row label="Address" value={facility.address} />
            <Row label="City" value={`${facility.city ?? ""}, CA ${facility.zip ?? ""}`} />
            <Row label="County" value={facility.county} />
            {facility.phone && <Row label="Phone" value={facility.phone} />}
          </dl>
        </section>

        {/* License */}
        <section style={{ marginBottom: 16 }}>
          <h3 style={sectionHeader}>License & Certification</h3>
          <dl style={dl}>
            <Row label="Status" value={
              <span style={{
                textTransform: "capitalize",
                color: facility.license_status === "active" ? "#059669" : "#dc2626",
                fontWeight: 600,
              }}>
                {facility.license_status ?? "Unknown"}
              </span>
            } />
            {facility.license_number && <Row label="License #" value={facility.license_number} />}
            {facility.license_expiry && <Row label="Expiry" value={facility.license_expiry} />}
            <Row label="Medicare" value={facility.certified_medicare ? "Yes" : "No"} />
            <Row label="Medicaid" value={facility.certified_medicaid ? "Yes" : "No"} />
          </dl>
        </section>

        {/* IDs */}
        {(facility.cdph_id || facility.cms_npi || facility.oshpd_id || facility.cdss_id) && (
          <section style={{ marginBottom: 16 }}>
            <h3 style={sectionHeader}>Source IDs</h3>
            <dl style={dl}>
              {facility.cdph_id && <Row label="CDPH" value={facility.cdph_id} />}
              {facility.cms_npi && <Row label="NPI" value={facility.cms_npi} />}
              {facility.oshpd_id && <Row label="OSHPD" value={facility.oshpd_id} />}
              {facility.cdss_id && <Row label="CDSS" value={facility.cdss_id} />}
            </dl>
          </section>
        )}

        {/* Financials Table */}
        {facility.financials.length > 0 && (
          <section style={{ marginBottom: 16 }}>
            <h3 style={sectionHeader}>Financials</h3>
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", fontSize: 11, borderCollapse: "collapse" }}>
                <thead>
                  <tr style={{ borderBottom: "2px solid #e5e7eb" }}>
                    <th style={th}>Year</th>
                    <th style={th}>Source</th>
                    <th style={th}>Gross Rev</th>
                    <th style={th}>Net Rev</th>
                    <th style={th}>Expenses</th>
                    <th style={th}>Visits</th>
                  </tr>
                </thead>
                <tbody>
                  {facility.financials.map((f, i) => (
                    <tr key={i} style={{ borderBottom: "1px solid #f3f4f6" }}>
                      <td style={td}>{f.year}</td>
                      <td style={td}>{f.source.toUpperCase()}</td>
                      <td style={td}>{formatCurrency(f.gross_revenue)}</td>
                      <td style={td}>{formatCurrency(f.net_revenue)}</td>
                      <td style={td}>{formatCurrency(f.total_expenses)}</td>
                      <td style={td}>{f.total_visits?.toLocaleString() ?? "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        )}

        {/* Violations Timeline */}
        {facility.violations.length > 0 && (
          <section>
            <h3 style={sectionHeader}>
              Violations ({facility.violations.length})
            </h3>
            <div>
              {facility.violations.map((v, i) => (
                <div key={i} style={{
                  padding: "8px 0",
                  borderBottom: i < facility.violations.length - 1 ? "1px solid #f3f4f6" : "none",
                }}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                    <span style={{
                      fontSize: 12, fontWeight: 600,
                      color: severityColor(v.severity),
                    }}>
                      {v.severity ?? "Unknown"} {v.deficiency_tag && `(${v.deficiency_tag})`}
                    </span>
                    <span style={{ fontSize: 11, color: "#9ca3af" }}>
                      {v.survey_date ?? "No date"}
                    </span>
                  </div>
                  <div style={{ display: "flex", gap: 8, marginTop: 2, fontSize: 11 }}>
                    <span style={{ color: "#6b7280" }}>{v.source}</span>
                    {v.scope && <span style={{ color: "#6b7280" }}>{v.scope}</span>}
                    <span style={{
                      marginLeft: "auto",
                      color: v.resolved ? "#059669" : "#dc2626",
                      fontWeight: 500,
                    }}>
                      {v.resolved ? `Resolved ${v.resolved_date ?? ""}` : "Open"}
                    </span>
                  </div>
                  {v.description && (
                    <p style={{ margin: "4px 0 0", fontSize: 11, color: "#374151", lineHeight: 1.4 }}>
                      {v.description.length > 200 ? `${v.description.slice(0, 200)}...` : v.description}
                    </p>
                  )}
                </div>
              ))}
            </div>
          </section>
        )}
      </div>
    </div>
  );
}

function Row({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <>
      <dt style={{ fontSize: 11, color: "#6b7280", marginBottom: 1 }}>{label}</dt>
      <dd style={{ fontSize: 13, margin: "0 0 6px", color: "#111827" }}>{value ?? "—"}</dd>
    </>
  );
}

function severityColor(severity: string | null): string {
  if (!severity) return "#6b7280";
  const s = severity.toLowerCase();
  if (s === "serious" || s.includes("immediate") || s >= "G") return "#dc2626";
  if (s === "moderate" || s >= "D") return "#f59e0b";
  return "#6b7280";
}

const sectionHeader: React.CSSProperties = {
  fontSize: 13, fontWeight: 600, margin: "0 0 8px",
  paddingBottom: 4, borderBottom: "1px solid #e5e7eb",
};

const dl: React.CSSProperties = { margin: 0, padding: 0 };
const th: React.CSSProperties = { textAlign: "left", padding: "4px 6px", fontWeight: 600 };
const td: React.CSSProperties = { textAlign: "left", padding: "4px 6px" };
