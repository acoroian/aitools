import { useState, useCallback } from "react";
import { useLoaderData } from "@remix-run/react";
import type { LoaderFunctionArgs } from "@remix-run/node";
import { ClientOnly } from "~/lib/client-only";
import type { Layer, FilterRequest } from "~/lib/api";
import { fetchLayers, filterFacilities, getFacility } from "~/lib/api";
import LayerPanel from "~/components/LayerPanel";
import FilterPanel from "~/components/FilterPanel";

export async function loader(_: LoaderFunctionArgs) {
  try {
    const layers = await fetchLayers();
    return { layers };
  } catch {
    return { layers: [] as Layer[] };
  }
}

export default function Index() {
  const { layers } = useLoaderData<typeof loader>();
  const [visibleLayers, setVisibleLayers] = useState<Set<string>>(
    new Set(layers.map((l) => l.slug))
  );
  const [highlightIds, setHighlightIds] = useState<Set<string>>(new Set());
  const [resultCount, setResultCount] = useState<number | null>(null);
  const [selectedFacility, setSelectedFacility] = useState<Record<string, unknown> | null>(null);

  const toggleLayer = useCallback((slug: string) => {
    setVisibleLayers((prev) => {
      const next = new Set(prev);
      next.has(slug) ? next.delete(slug) : next.add(slug);
      return next;
    });
  }, []);

  const handleFilter = useCallback(async (req: FilterRequest) => {
    try {
      const data = await filterFacilities({ ...req, limit: 2000 });
      const ids = new Set<string>(data.features.map((f: { properties: { id: string } }) => f.properties.id));
      setHighlightIds(ids);
      setResultCount(data.total);
    } catch (err) {
      console.error("Filter failed:", err);
    }
  }, []);

  const handleFacilityClick = useCallback(async (id: string) => {
    try {
      const facility = await getFacility(id);
      setSelectedFacility(facility);
    } catch (err) {
      console.error("Failed to load facility:", err);
    }
  }, []);

  return (
    <div style={{ position: "relative", width: "100vw", height: "100vh" }}>
      <ClientOnly fallback={<div style={{ width: "100%", height: "100%", background: "#f3f4f6" }} />}>
        {() => {
          const MapComponent = require("~/components/Map").default;
          return (
            <MapComponent
              layers={layers}
              visibleLayers={visibleLayers}
              highlightIds={highlightIds}
              onFacilityClick={handleFacilityClick}
            />
          );
        }}
      </ClientOnly>

      <LayerPanel layers={layers} visibleLayers={visibleLayers} onToggle={toggleLayer} />
      <FilterPanel onFilter={handleFilter} resultCount={resultCount} />

      {selectedFacility && (
        <div style={{
          position: "absolute", top: 16, right: 60, zIndex: 10,
          background: "white", borderRadius: 8, padding: 16,
          boxShadow: "0 2px 8px rgba(0,0,0,0.15)", maxWidth: 320, maxHeight: "80vh", overflowY: "auto",
        }}>
          <button onClick={() => setSelectedFacility(null)}
            style={{ position: "absolute", top: 8, right: 8, background: "none", border: "none", cursor: "pointer", fontSize: 18 }}>
            ×
          </button>
          <h3 style={{ margin: "0 0 4px", fontSize: 15 }}>{String(selectedFacility.name)}</h3>
          <p style={{ margin: "0 0 8px", fontSize: 12, color: "#6b7280", textTransform: "capitalize" }}>
            {String(selectedFacility.type).replace(/_/g, " ")} · {String(selectedFacility.license_status ?? "—")}
          </p>
          <p style={{ margin: "0 0 4px", fontSize: 12 }}>{String(selectedFacility.address ?? "—")}</p>
          <p style={{ margin: "0 0 12px", fontSize: 12 }}>{String(selectedFacility.city ?? "—")}, CA {String(selectedFacility.zip ?? "")}</p>

          {Array.isArray(selectedFacility.financials) && selectedFacility.financials.length > 0 && (
            <div style={{ marginBottom: 12 }}>
              <h4 style={{ margin: "0 0 4px", fontSize: 13 }}>Latest Financials</h4>
              {[selectedFacility.financials[0]].map((f: Record<string, unknown>, i: number) => (
                <div key={i} style={{ fontSize: 12 }}>
                  <p style={{ margin: "2px 0" }}>Year: {String(f.year)}</p>
                  <p style={{ margin: "2px 0" }}>
                    Revenue: {f.gross_revenue ? `$${Number(f.gross_revenue).toLocaleString()}` : "N/A"}
                  </p>
                </div>
              ))}
            </div>
          )}

          {Array.isArray(selectedFacility.violations) && selectedFacility.violations.length > 0 && (
            <div>
              <h4 style={{ margin: "0 0 4px", fontSize: 13 }}>
                Violations ({(selectedFacility.violations as unknown[]).length})
              </h4>
              {(selectedFacility.violations as Record<string, unknown>[]).slice(0, 3).map((v, i) => (
                <div key={i} style={{ fontSize: 11, padding: "4px 0", borderTop: "1px solid #f3f4f6" }}>
                  <span style={{ fontWeight: 600 }}>{String(v.severity ?? "—")}</span>
                  {" · "}{String(v.survey_date ?? "—")}
                  <p style={{ margin: "2px 0", color: "#6b7280" }}>{String(v.description ?? "").slice(0, 80)}…</p>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
