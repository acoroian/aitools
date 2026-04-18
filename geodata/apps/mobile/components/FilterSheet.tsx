import React, { useState } from "react";
import {
  View, Text, TextInput, Switch, TouchableOpacity,
  ScrollView, StyleSheet, Modal,
} from "react-native";
import { Picker } from "@react-native-picker/picker";

interface FilterRequest {
  facility_types?: string[];
  license_status?: string;
  county?: string;
  gross_revenue_min?: number;
  violation_count_max?: number;
  certified_medicare?: boolean;
  limit?: number;
}

interface Props {
  visible: boolean;
  onClose: () => void;
  onApply: (req: FilterRequest) => void;
}

const FACILITY_TYPES = [
  { value: "home_health", label: "Home Health" },
  { value: "hospice", label: "Hospice" },
  { value: "snf", label: "Skilled Nursing" },
  { value: "daycare_center", label: "Daycare" },
  { value: "rcfe", label: "Residential Care" },
  { value: "clinic", label: "Clinic" },
];

export default function FilterSheet({ visible, onClose, onApply }: Props) {
  const [selectedTypes, setSelectedTypes] = useState<string[]>([]);
  const [status, setStatus] = useState("");
  const [revenueMin, setRevenueMin] = useState("");
  const [violMax, setViolMax] = useState("");
  const [medicare, setMedicare] = useState(false);

  const toggleType = (val: string) => {
    setSelectedTypes((prev) =>
      prev.includes(val) ? prev.filter((t) => t !== val) : [...prev, val]
    );
  };

  const handleApply = () => {
    const req: FilterRequest = {};
    if (selectedTypes.length > 0) req.facility_types = selectedTypes;
    if (status) req.license_status = status;
    if (revenueMin) req.gross_revenue_min = parseInt(revenueMin, 10);
    if (violMax) req.violation_count_max = parseInt(violMax, 10);
    if (medicare) req.certified_medicare = true;
    onApply(req);
    onClose();
  };

  const handleClear = () => {
    setSelectedTypes([]);
    setStatus("");
    setRevenueMin("");
    setViolMax("");
    setMedicare(false);
  };

  return (
    <Modal visible={visible} animationType="slide" presentationStyle="pageSheet">
      <View style={styles.container}>
        <View style={styles.header}>
          <TouchableOpacity onPress={onClose}>
            <Text style={styles.cancel}>Cancel</Text>
          </TouchableOpacity>
          <Text style={styles.title}>Filters</Text>
          <TouchableOpacity onPress={handleClear}>
            <Text style={styles.clear}>Clear</Text>
          </TouchableOpacity>
        </View>

        <ScrollView style={styles.body}>
          <Text style={styles.label}>Facility Type</Text>
          <View style={styles.chips}>
            {FACILITY_TYPES.map((t) => (
              <TouchableOpacity
                key={t.value}
                onPress={() => toggleType(t.value)}
                style={[styles.chip, selectedTypes.includes(t.value) && styles.chipActive]}
              >
                <Text style={[
                  styles.chipText,
                  selectedTypes.includes(t.value) && styles.chipTextActive,
                ]}>
                  {t.label}
                </Text>
              </TouchableOpacity>
            ))}
          </View>

          <Text style={styles.label}>License Status</Text>
          <View style={styles.pickerWrap}>
            <Picker
              selectedValue={status}
              onValueChange={setStatus}
              style={styles.picker}
            >
              <Picker.Item label="Any" value="" />
              <Picker.Item label="Active" value="active" />
              <Picker.Item label="Pending" value="pending" />
              <Picker.Item label="Expired" value="expired" />
              <Picker.Item label="Closed" value="closed" />
            </Picker>
          </View>

          <Text style={styles.label}>Min Revenue ($)</Text>
          <TextInput
            style={styles.input}
            value={revenueMin}
            onChangeText={setRevenueMin}
            placeholder="e.g. 500000"
            keyboardType="numeric"
          />

          <Text style={styles.label}>Max Violations</Text>
          <TextInput
            style={styles.input}
            value={violMax}
            onChangeText={setViolMax}
            placeholder="e.g. 5"
            keyboardType="numeric"
          />

          <View style={styles.switchRow}>
            <Text style={styles.switchLabel}>Medicare Certified</Text>
            <Switch value={medicare} onValueChange={setMedicare} trackColor={{ true: "#3b82f6" }} />
          </View>
        </ScrollView>

        <TouchableOpacity style={styles.applyBtn} onPress={handleApply}>
          <Text style={styles.applyText}>Apply Filters</Text>
        </TouchableOpacity>
      </View>
    </Modal>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: "#fff" },
  header: {
    flexDirection: "row", justifyContent: "space-between", alignItems: "center",
    padding: 16, borderBottomWidth: 1, borderBottomColor: "#e5e7eb",
  },
  title: { fontSize: 17, fontWeight: "600" },
  cancel: { fontSize: 15, color: "#6b7280" },
  clear: { fontSize: 15, color: "#dc2626" },
  body: { flex: 1, padding: 16 },
  label: { fontSize: 14, fontWeight: "600", marginTop: 16, marginBottom: 8 },
  chips: { flexDirection: "row", flexWrap: "wrap", gap: 8 },
  chip: {
    paddingHorizontal: 12, paddingVertical: 6, borderRadius: 16,
    borderWidth: 1, borderColor: "#d1d5db", backgroundColor: "#fff",
  },
  chipActive: { borderColor: "#3b82f6", backgroundColor: "#eff6ff" },
  chipText: { fontSize: 13, color: "#374151" },
  chipTextActive: { color: "#1d4ed8" },
  pickerWrap: { borderWidth: 1, borderColor: "#d1d5db", borderRadius: 8 },
  picker: { height: 44 },
  input: {
    borderWidth: 1, borderColor: "#d1d5db", borderRadius: 8,
    padding: 10, fontSize: 14,
  },
  switchRow: {
    flexDirection: "row", justifyContent: "space-between", alignItems: "center",
    marginTop: 16, paddingVertical: 8,
  },
  switchLabel: { fontSize: 14 },
  applyBtn: {
    margin: 16, padding: 14, backgroundColor: "#3b82f6",
    borderRadius: 8, alignItems: "center",
  },
  applyText: { color: "#fff", fontSize: 16, fontWeight: "600" },
});
