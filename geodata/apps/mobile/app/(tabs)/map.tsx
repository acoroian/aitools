import React, { useEffect, useState } from "react";
import { View, Text, Switch, StyleSheet, SafeAreaView, ScrollView } from "react-native";
import MapView from "../../components/MapView";
import { fetchLayers, type Layer } from "../../lib/api";

export default function MapScreen() {
  const [layers, setLayers] = useState<Layer[]>([]);
  const [visibleLayers, setVisibleLayers] = useState<Set<string>>(new Set());

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

  return (
    <SafeAreaView style={styles.container}>
      {/* Layer toggles */}
      <View style={styles.layerBar}>
        <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={styles.layerScroll}>
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
      </View>

      {/* Map */}
      <MapView layers={layers} visibleLayers={visibleLayers} />
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: "#fff" },
  layerBar: { backgroundColor: "white", borderBottomWidth: 1, borderBottomColor: "#e5e7eb" },
  layerScroll: { padding: 8, gap: 12 },
  layerItem: { flexDirection: "row", alignItems: "center", gap: 6 },
  layerLabel: { fontSize: 12, color: "#374151" },
});
