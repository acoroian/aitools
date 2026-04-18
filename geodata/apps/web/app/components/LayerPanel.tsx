import type { Layer } from "~/lib/api";

interface Props {
  layers: Layer[];
  visibleLayers: Set<string>;
  onToggle: (slug: string) => void;
}

export default function LayerPanel({ layers, visibleLayers, onToggle }: Props) {
  return (
    <div style={{
      position: "absolute", top: 16, left: 16, zIndex: 10,
      background: "white", borderRadius: 8, padding: "12px 16px",
      boxShadow: "0 2px 8px rgba(0,0,0,0.15)", minWidth: 200,
    }}>
      <h3 style={{ margin: "0 0 10px", fontSize: 14, fontWeight: 600 }}>Layers</h3>
      {layers.map((layer) => (
        <label key={layer.slug} style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6, cursor: "pointer", fontSize: 13 }}>
          <input
            type="checkbox"
            checked={visibleLayers.has(layer.slug)}
            onChange={() => onToggle(layer.slug)}
          />
          <span>{layer.name}</span>
          {layer.record_count != null && (
            <span style={{ marginLeft: "auto", color: "#9ca3af", fontSize: 11 }}>
              {layer.record_count.toLocaleString()}
            </span>
          )}
        </label>
      ))}
    </div>
  );
}
