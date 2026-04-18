import React from "react";
import {
  View, Text, ScrollView, TouchableOpacity,
  StyleSheet, Modal,
} from "react-native";

interface Facility {
  name: string;
  type: string;
  address?: string;
  city?: string;
  zip?: string;
  county?: string;
  phone?: string;
  license_status?: string;
  license_number?: string;
  certified_medicare?: boolean;
  certified_medicaid?: boolean;
  financials?: Financial[];
  violations?: Violation[];
}

interface Financial {
  year: number;
  source: string;
  gross_revenue: number | null;
  net_revenue: number | null;
  total_visits: number | null;
}

interface Violation {
  survey_date: string | null;
  source: string;
  severity: string | null;
  deficiency_tag: string | null;
  description: string | null;
  resolved: boolean;
}

interface Props {
  facility: Facility | null;
  onClose: () => void;
}

function formatCurrency(val: number | null): string {
  if (val == null) return "N/A";
  return `$${val.toLocaleString()}`;
}

export default function FacilitySheet({ facility, onClose }: Props) {
  if (!facility) return null;

  return (
    <Modal visible animationType="slide" presentationStyle="pageSheet">
      <View style={styles.container}>
        <View style={styles.header}>
          <View style={{ flex: 1 }}>
            <Text style={styles.name}>{facility.name}</Text>
            <Text style={styles.type}>
              {facility.type.replace(/_/g, " ")} · {facility.license_status ?? "—"}
            </Text>
          </View>
          <TouchableOpacity onPress={onClose} style={styles.closeBtn}>
            <Text style={styles.closeTxt}>×</Text>
          </TouchableOpacity>
        </View>

        <ScrollView style={styles.body}>
          {/* Address */}
          <View style={styles.section}>
            <Text style={styles.sectionTitle}>Location</Text>
            <Text style={styles.value}>{facility.address ?? "—"}</Text>
            <Text style={styles.value}>
              {facility.city ?? ""}, CA {facility.zip ?? ""}
            </Text>
            {facility.county && <Text style={styles.value}>{facility.county} County</Text>}
            {facility.phone && <Text style={styles.value}>{facility.phone}</Text>}
          </View>

          {/* Certification */}
          <View style={styles.section}>
            <Text style={styles.sectionTitle}>Certification</Text>
            <View style={styles.row}>
              <Text style={styles.label}>Medicare</Text>
              <Text style={styles.value}>{facility.certified_medicare ? "Yes" : "No"}</Text>
            </View>
            <View style={styles.row}>
              <Text style={styles.label}>Medicaid</Text>
              <Text style={styles.value}>{facility.certified_medicaid ? "Yes" : "No"}</Text>
            </View>
          </View>

          {/* Financials */}
          {facility.financials && facility.financials.length > 0 && (
            <View style={styles.section}>
              <Text style={styles.sectionTitle}>Financials</Text>
              {facility.financials.map((f, i) => (
                <View key={i} style={styles.finRow}>
                  <Text style={styles.finYear}>{f.year} ({f.source.toUpperCase()})</Text>
                  <Text style={styles.value}>
                    Revenue: {formatCurrency(f.gross_revenue)}
                  </Text>
                  {f.total_visits != null && (
                    <Text style={styles.value}>
                      Visits: {f.total_visits.toLocaleString()}
                    </Text>
                  )}
                </View>
              ))}
            </View>
          )}

          {/* Violations */}
          {facility.violations && facility.violations.length > 0 && (
            <View style={styles.section}>
              <Text style={styles.sectionTitle}>
                Violations ({facility.violations.length})
              </Text>
              {facility.violations.slice(0, 10).map((v, i) => (
                <View key={i} style={styles.violRow}>
                  <View style={styles.violHeader}>
                    <Text style={[
                      styles.severity,
                      { color: v.severity === "serious" ? "#dc2626" : "#f59e0b" },
                    ]}>
                      {v.severity ?? "Unknown"}
                    </Text>
                    <Text style={styles.violDate}>{v.survey_date ?? "—"}</Text>
                  </View>
                  {v.description && (
                    <Text style={styles.violDesc} numberOfLines={3}>
                      {v.description}
                    </Text>
                  )}
                  <Text style={[
                    styles.resolvedBadge,
                    { color: v.resolved ? "#059669" : "#dc2626" },
                  ]}>
                    {v.resolved ? "Resolved" : "Open"}
                  </Text>
                </View>
              ))}
            </View>
          )}
        </ScrollView>
      </View>
    </Modal>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: "#fff" },
  header: {
    flexDirection: "row", padding: 16, alignItems: "flex-start",
    borderBottomWidth: 1, borderBottomColor: "#e5e7eb",
  },
  name: { fontSize: 18, fontWeight: "700" },
  type: { fontSize: 14, color: "#6b7280", textTransform: "capitalize", marginTop: 2 },
  closeBtn: { padding: 4 },
  closeTxt: { fontSize: 24, color: "#6b7280" },
  body: { flex: 1, padding: 16 },
  section: { marginBottom: 20 },
  sectionTitle: {
    fontSize: 15, fontWeight: "600", marginBottom: 8,
    paddingBottom: 4, borderBottomWidth: 1, borderBottomColor: "#e5e7eb",
  },
  row: { flexDirection: "row", justifyContent: "space-between", marginBottom: 4 },
  label: { fontSize: 13, color: "#6b7280" },
  value: { fontSize: 13, color: "#111827", marginBottom: 2 },
  finRow: { marginBottom: 8 },
  finYear: { fontSize: 13, fontWeight: "600", marginBottom: 2 },
  violRow: {
    paddingVertical: 8, borderBottomWidth: 1, borderBottomColor: "#f3f4f6",
  },
  violHeader: { flexDirection: "row", justifyContent: "space-between" },
  severity: { fontSize: 13, fontWeight: "600" },
  violDate: { fontSize: 12, color: "#9ca3af" },
  violDesc: { fontSize: 12, color: "#374151", marginTop: 4, lineHeight: 18 },
  resolvedBadge: { fontSize: 11, fontWeight: "500", marginTop: 4 },
});
