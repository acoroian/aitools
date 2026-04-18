"use client";
import { useEffect, useRef, useCallback, forwardRef, useImperativeHandle } from "react";
import maplibregl from "maplibre-gl";
import { Protocol } from "pmtiles";
import type { Layer } from "~/lib/api";
import { pmtilesUrl } from "~/lib/api";

// Colour per facility type
const TYPE_COLORS: Record<string, string> = {
  home_health: "#3b82f6",
  hospice: "#8b5cf6",
  daycare_center: "#f59e0b",
  daycare_family: "#f59e0b",
  snf: "#10b981",
  rcfe: "#14b8a6",
  clinic: "#ec4899",
  hospital: "#ef4444",
  default: "#6b7280",
};

export interface MapHandle {
  getMap: () => maplibregl.Map | null;
}

interface Props {
  layers: Layer[];
  visibleLayers: Set<string>;
  highlightIds: Set<string>;
  onFacilityClick: (id: string) => void;
}

const MapComponent = forwardRef<MapHandle, Props>(function MapComponent(
  { layers, visibleLayers, highlightIds, onFacilityClick },
  ref
) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);

  useImperativeHandle(ref, () => ({
    getMap: () => mapRef.current,
  }));

  // Register pmtiles:// protocol once
  useEffect(() => {
    const protocol = new Protocol();
    maplibregl.addProtocol("pmtiles", protocol.tile);
    return () => maplibregl.removeProtocol("pmtiles");
  }, []);

  // Init map
  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: {
        version: 8,
        sources: {
          "osm-tiles": {
            type: "raster",
            tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
            tileSize: 256,
            attribution: "© OpenStreetMap contributors",
          },
        },
        layers: [{ id: "osm-bg", type: "raster", source: "osm-tiles" }],
      },
      center: [-119.4179, 36.7783], // California center
      zoom: 6,
    });

    map.addControl(new maplibregl.NavigationControl(), "top-right");

    map.on("load", () => {
      layers.forEach((layer) => {
        const url = `pmtiles://${pmtilesUrl(layer.slug)}`;
        map.addSource(layer.slug, { type: "vector", url });

        map.addLayer({
          id: layer.slug,
          type: "circle",
          source: layer.slug,
          "source-layer": layer.slug,
          layout: { visibility: visibleLayers.has(layer.slug) ? "visible" : "none" },
          paint: {
            "circle-radius": ["interpolate", ["linear"], ["zoom"], 6, 4, 14, 10],
            "circle-color": [
              "match",
              ["get", "type"],
              ...Object.entries(TYPE_COLORS).flatMap(([k, v]) => [k, v]),
              TYPE_COLORS.default,
            ],
            "circle-stroke-width": 1,
            "circle-stroke-color": "#fff",
            "circle-opacity": 1,
          },
        });
      });
    });

    // Click handler
    map.on("click", (e) => {
      const features = map.queryRenderedFeatures(e.point, {
        layers: layers.map((l) => l.slug),
      });
      if (features.length > 0) {
        const id = features[0].properties?.id as string | undefined;
        if (id) onFacilityClick(id);
      }
    });

    // Pointer cursor on hover
    layers.forEach((l) => {
      map.on("mouseenter", l.slug, () => {
        map.getCanvas().style.cursor = "pointer";
      });
      map.on("mouseleave", l.slug, () => {
        map.getCanvas().style.cursor = "";
      });
    });

    mapRef.current = map;
    return () => map.remove();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Toggle layer visibility — wait for style to be loaded
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;

    const applyVisibility = () => {
      layers.forEach((layer) => {
        if (map.getLayer(layer.slug)) {
          map.setLayoutProperty(
            layer.slug,
            "visibility",
            visibleLayers.has(layer.slug) ? "visible" : "none"
          );
        }
      });
    };

    if (map.isStyleLoaded()) {
      applyVisibility();
    } else {
      map.once("load", applyVisibility);
    }
  }, [layers, visibleLayers]);

  return <div ref={containerRef} style={{ width: "100%", height: "100%" }} />;
});

export default MapComponent;
