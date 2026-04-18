import React, { useRef, useState, useCallback } from "react";
import { StyleSheet, View, Text, TouchableOpacity, ScrollView } from "react-native";
import MapLibreGL, { Camera, MapView as MLMapView, ShapeSource, CircleLayer } from "@maplibre/maplibre-react-native";
import type { Layer } from "../lib/api";
import { pmtilesUrl, getFacility, filterFacilities } from "../lib/api";

MapLibreGL.setAccessToken(null); // MapLibre doesn't need a token

const TYPE_COLORS: Record<string, string> = {
  home_health: "#3b82f6",
  hospice: "#8b5cf6",
  daycare: "#f59e0b",
  snf: "#10b981",
};

interface Props {
  layers: Layer[];
  visibleLayers: Set<string>;
}

export default function MapView({ layers, visibleLayers }: Props) {
  const [selectedFacility, setSelectedFacility] = useState<Record<string, unknown> | null>(null);

  const handleShapePress = useCallback(async (e: { features?: Array<{ properties?: { id?: string } }> }) => {
    const id = e.features?.[0]?.properties?.id;
    if (!id) return;
    try {
      const facility = await getFacility(id);
      setSelectedFacility(facility);
    } catch {}
  }, []);

  return (
    <View style={styles.container}>
      <MLMapView
        style={styles.map}
        styleURL="https://tiles.openfreemap.org/styles/liberty"
      >
        <Camera
          defaultSettings={{ centerCoordinate: [-119.4179, 36.7783], zoomLevel: 5 }}
        />
        {layers
          .filter((l) => visibleLayers.has(l.slug))
          .map((layer) => (
            <ShapeSource
              key={layer.slug}
              id={layer.slug}
              url={pmtilesUrl(layer.slug)}
              onPress={handleShapePress}
            >
              <CircleLayer
                id={`${layer.slug}-circles`}
                style={{
                  circleRadius: 6,
                  circleColor: TYPE_COLORS[layer.facility_types?.[0] ?? ""] ?? "#6b7280",
                  circleStrokeWidth: 1,
                  circleStrokeColor: "#ffffff",
                }}
              />
            </ShapeSource>
          ))}
      </MLMapView>

      {selectedFacility && (
        <View style={styles.popup}>
          <TouchableOpacity style={styles.closeBtn} onPress={() => setSelectedFacility(null)}>
            <Text style={styles.closeTxt}>×</Text>
          </TouchableOpacity>
          <Text style={styles.popupName}>{String(selectedFacility.name)}</Text>
          <Text style={styles.popupSub}>
            {String(selectedFacility.type ?? "").replace(/_/g, " ")} · {String(selectedFacility.license_status ?? "—")}
          </Text>
          <Text style={styles.popupAddr}>
            {String(selectedFacility.address ?? "—")}, {String(selectedFacility.city ?? "—")}
          </Text>
        </View>
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  map: { flex: 1 },
  popup: {
    position: "absolute", bottom: 32, left: 16, right: 16,
    backgroundColor: "white", borderRadius: 12, padding: 16,
    shadowColor: "#000", shadowOffset: { width: 0, height: 2 },
    shadowOpacity: 0.15, shadowRadius: 6, elevation: 4,
  },
  closeBtn: { position: "absolute", top: 8, right: 12 },
  closeTxt: { fontSize: 20, color: "#6b7280" },
  popupName: { fontSize: 16, fontWeight: "600", marginBottom: 2 },
  popupSub: { fontSize: 13, color: "#6b7280", textTransform: "capitalize", marginBottom: 4 },
  popupAddr: { fontSize: 13 },
});
