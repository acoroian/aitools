import React, { useEffect, useState, useCallback } from "react";
import {
  View, Text, Switch, TouchableOpacity,
  StyleSheet, SafeAreaView, ScrollView,
} from "react-native";
import MapView from "../../components/MapView";
import FilterSheet from "../../components/FilterSheet";
import FacilitySheet from "../../components/FacilitySheet";
import { fetchLayers, getFacility, filterFacilities, type Layer } from "../../lib/api";

export default function MapScreen() {
  const [layers, setLayers] = useState<Layer[]>([]);
  const [visibleLayers, setVisibleLayers] = useState<Set<string>>(new Set());
  const [showFilters, setShowFilters] = useState(false);
  const [selectedFacility, setSelectedFacility] = useState<Record<string, unknown> | null>(null);
  const [resultCount, setResultCount] = useState<number | null>(null);

  useEffect(() => {
    fetchLayers()
      .then((data) => {
        setLayers(data);
        setVisibleLayers(new Set(data.map((l) => l.slug)));
      })
      .catch(console.error);
  }, []);

  const toggle = (slug: string) =>
    setVisibleLayers((prev) => {
      const next = new Set(prev);
      next.has(slug) ? next.delete(slug) : next.add(slug);
      return next;
    });

  const handleFacilityTap = useCallback(async (id: string) => {
    try {
      const facility = await getFacility(id);
      setSelectedFacility(facility);
    } catch (err) {
      console.error("Failed to load facility:", err);
    }
  }, []);

  const handleApplyFilters = useCallback(async (req: Record<string, unknown>) => {
    try {
      const data = await filterFacilities({ ...req, limit: 1000 });
      setResultCount(data.total);
    } catch (err) {
      console.error("Filter failed:", err);
    }
  }, []);

  return (
    <SafeAreaView style={styles.container}>
      {/* Top bar: layers + filter button */}
      <View style={styles.topBar}>
        <ScrollView
          horizontal
          showsHorizontalScrollIndicator={false}
          contentContainerStyle={styles.layerScroll}
          style={{ flex: 1 }}
        >
          {layers.map((l) => (
            <View key={l.slug} style={styles.layerItem}>
              <Text style={styles.layerLabel}>{l.name}</Text>
              <Switch
                value={visibleLayers.has(l.slug)}
                onValueChange={() => toggle(l.slug)}
                trackColor={{ true: "#3b82f6" }}
              />
            </View>
          ))}
        </ScrollView>
        <TouchableOpacity style={styles.filterBtn} onPress={() => setShowFilters(true)}>
          <Text style={styles.filterBtnText}>Filters</Text>
        </TouchableOpacity>
      </View>

      {/* Result count banner */}
      {resultCount !== null && (
        <View style={styles.resultBanner}>
          <Text style={styles.resultText}>
            {resultCount.toLocaleString()} facilities matched
          </Text>
          <TouchableOpacity onPress={() => setResultCount(null)}>
            <Text style={styles.resultClear}>Clear</Text>
          </TouchableOpacity>
        </View>
      )}

      {/* Map */}
      <MapView
        layers={layers}
        visibleLayers={visibleLayers}
        onFacilityTap={handleFacilityTap}
      />

      {/* Filter sheet */}
      <FilterSheet
        visible={showFilters}
        onClose={() => setShowFilters(false)}
        onApply={handleApplyFilters}
      />

      {/* Facility detail sheet */}
      <FacilitySheet
        facility={selectedFacility as never}
        onClose={() => setSelectedFacility(null)}
      />
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: "#fff" },
  topBar: {
    flexDirection: "row", alignItems: "center",
    backgroundColor: "white", borderBottomWidth: 1, borderBottomColor: "#e5e7eb",
    paddingRight: 8,
  },
  layerScroll: { padding: 8, gap: 12 },
  layerItem: { flexDirection: "row", alignItems: "center", gap: 6 },
  layerLabel: { fontSize: 12, color: "#374151" },
  filterBtn: {
    paddingHorizontal: 12, paddingVertical: 6,
    backgroundColor: "#3b82f6", borderRadius: 6,
  },
  filterBtnText: { color: "#fff", fontSize: 13, fontWeight: "600" },
  resultBanner: {
    flexDirection: "row", justifyContent: "space-between", alignItems: "center",
    backgroundColor: "#eff6ff", paddingHorizontal: 16, paddingVertical: 6,
  },
  resultText: { fontSize: 12, color: "#1d4ed8" },
  resultClear: { fontSize: 12, color: "#dc2626", fontWeight: "600" },
});
