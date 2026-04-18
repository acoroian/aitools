import React, { useState, useCallback } from "react";
import { StyleSheet, View } from "react-native";
import MapLibreGL, { Camera, MapView as MLMapView, ShapeSource, CircleLayer } from "@maplibre/maplibre-react-native";
import type { Layer } from "../lib/api";
import { pmtilesUrl } from "../lib/api";

MapLibreGL.setAccessToken(null); // MapLibre doesn't need a token

const TYPE_COLORS: Record<string, string> = {
  home_health: "#3b82f6",
  hospice: "#8b5cf6",
  daycare_center: "#f59e0b",
  daycare_family: "#f59e0b",
  snf: "#10b981",
  rcfe: "#14b8a6",
  clinic: "#ec4899",
  hospital: "#ef4444",
};

interface Props {
  layers: Layer[];
  visibleLayers: Set<string>;
  onFacilityTap?: (id: string) => void;
}

export default function MapView({ layers, visibleLayers, onFacilityTap }: Props) {
  const handleShapePress = useCallback(async (e: { features?: Array<{ properties?: { id?: string } }> }) => {
    const id = e.features?.[0]?.properties?.id;
    if (id && onFacilityTap) onFacilityTap(id);
  }, [onFacilityTap]);

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
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  map: { flex: 1 },
});
