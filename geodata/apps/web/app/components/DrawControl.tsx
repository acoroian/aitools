"use client";
import { useEffect, useRef, useCallback, useState } from "react";
import maplibregl from "maplibre-gl";

interface Props {
  map: maplibregl.Map | null;
  onPolygonDrawn: (polygon: { type: "Polygon"; coordinates: number[][][] }) => void;
  onClear: () => void;
}

/**
 * Simple polygon draw tool. Click to add vertices, double-click to finish.
 * No external dependency — just listens to map click events.
 */
export default function DrawControl({ map, onPolygonDrawn, onClear }: Props) {
  const [drawing, setDrawing] = useState(false);
  const [hasPolygon, setHasPolygon] = useState(false);
  const verticesRef = useRef<[number, number][]>([]);

  const cleanup = useCallback(() => {
    if (!map) return;
    if (map.getLayer("draw-polygon-fill")) map.removeLayer("draw-polygon-fill");
    if (map.getLayer("draw-polygon-line")) map.removeLayer("draw-polygon-line");
    if (map.getLayer("draw-vertices")) map.removeLayer("draw-vertices");
    if (map.getSource("draw-polygon")) map.removeSource("draw-polygon");
    if (map.getSource("draw-vertices")) map.removeSource("draw-vertices");
  }, [map]);

  const updatePreview = useCallback(() => {
    if (!map) return;
    const verts = verticesRef.current;
    if (verts.length < 2) return;

    const ring = [...verts, verts[0]];
    const geojson: GeoJSON.FeatureCollection = {
      type: "FeatureCollection",
      features: [{
        type: "Feature",
        geometry: { type: "Polygon", coordinates: [ring] },
        properties: {},
      }],
    };

    const pointsGeojson: GeoJSON.FeatureCollection = {
      type: "FeatureCollection",
      features: verts.map((v) => ({
        type: "Feature" as const,
        geometry: { type: "Point" as const, coordinates: v },
        properties: {},
      })),
    };

    const polySource = map.getSource("draw-polygon") as maplibregl.GeoJSONSource | undefined;
    if (polySource) {
      polySource.setData(geojson);
    } else {
      map.addSource("draw-polygon", { type: "geojson", data: geojson });
      map.addLayer({
        id: "draw-polygon-fill",
        type: "fill",
        source: "draw-polygon",
        paint: { "fill-color": "#3b82f6", "fill-opacity": 0.1 },
      });
      map.addLayer({
        id: "draw-polygon-line",
        type: "line",
        source: "draw-polygon",
        paint: { "line-color": "#3b82f6", "line-width": 2, "line-dasharray": [2, 2] },
      });
    }

    const vertSource = map.getSource("draw-vertices") as maplibregl.GeoJSONSource | undefined;
    if (vertSource) {
      vertSource.setData(pointsGeojson);
    } else {
      map.addSource("draw-vertices", { type: "geojson", data: pointsGeojson });
      map.addLayer({
        id: "draw-vertices",
        type: "circle",
        source: "draw-vertices",
        paint: { "circle-radius": 5, "circle-color": "#3b82f6", "circle-stroke-width": 2, "circle-stroke-color": "#fff" },
      });
    }
  }, [map]);

  const startDrawing = useCallback(() => {
    if (!map) return;
    cleanup();
    verticesRef.current = [];
    setDrawing(true);
    setHasPolygon(false);
    map.getCanvas().style.cursor = "crosshair";
  }, [map, cleanup]);

  const finishDrawing = useCallback(() => {
    if (!map) return;
    const verts = verticesRef.current;
    setDrawing(false);
    map.getCanvas().style.cursor = "";

    if (verts.length >= 3) {
      const ring = [...verts, verts[0]];
      setHasPolygon(true);
      onPolygonDrawn({ type: "Polygon", coordinates: [ring] });
    }
  }, [map, onPolygonDrawn]);

  const clearPolygon = useCallback(() => {
    cleanup();
    verticesRef.current = [];
    setHasPolygon(false);
    setDrawing(false);
    if (map) map.getCanvas().style.cursor = "";
    onClear();
  }, [map, cleanup, onClear]);

  // Attach click / dblclick listeners while drawing
  useEffect(() => {
    if (!map || !drawing) return;

    const onClick = (e: maplibregl.MapMouseEvent) => {
      verticesRef.current.push([e.lngLat.lng, e.lngLat.lat]);
      updatePreview();
    };

    const onDblClick = (e: maplibregl.MapMouseEvent) => {
      e.preventDefault();
      finishDrawing();
    };

    map.on("click", onClick);
    map.on("dblclick", onDblClick);
    map.doubleClickZoom.disable();

    return () => {
      map.off("click", onClick);
      map.off("dblclick", onDblClick);
      map.doubleClickZoom.enable();
    };
  }, [map, drawing, updatePreview, finishDrawing]);

  return (
    <div style={{
      position: "absolute", top: 16, left: "50%", transform: "translateX(-50%)",
      zIndex: 10, display: "flex", gap: 8,
    }}>
      {!drawing && !hasPolygon && (
        <button onClick={startDrawing} style={btnStyle}>
          Draw Area Filter
        </button>
      )}
      {drawing && (
        <span style={{ ...btnStyle, background: "#fef3c7", color: "#92400e", cursor: "default" }}>
          Click to add points, double-click to finish
        </span>
      )}
      {hasPolygon && (
        <button onClick={clearPolygon} style={{ ...btnStyle, background: "#fee2e2", color: "#991b1b" }}>
          Clear Area Filter
        </button>
      )}
    </div>
  );
}

const btnStyle: React.CSSProperties = {
  padding: "6px 14px",
  background: "white",
  border: "1px solid #d1d5db",
  borderRadius: 6,
  fontSize: 13,
  cursor: "pointer",
  boxShadow: "0 1px 4px rgba(0,0,0,0.1)",
};
