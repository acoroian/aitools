import { useState, useCallback, useRef } from "react";
import { useLoaderData } from "@remix-run/react";
import type { LoaderFunctionArgs } from "@remix-run/node";
import { ClientOnly } from "~/lib/client-only";
import type { Layer, FilterRequest } from "~/lib/api";
import { fetchLayers, filterFacilities, getFacility } from "~/lib/api";
import LayerPanel from "~/components/LayerPanel";
import FilterPanel from "~/components/FilterPanel";
import FacilityDetail from "~/components/FacilityDetail";

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
  const [spatialFilter, setSpatialFilter] = useState<{ type: "Polygon"; coordinates: number[][][] } | null>(null);
  const mapRef = useRef<{ getMap: () => unknown } | null>(null);
  const lastFilterRef = useRef<FilterRequest>({});

  const toggleLayer = useCallback((slug: string) => {
    setVisibleLayers((prev) => {
      const next = new Set(prev);
      next.has(slug) ? next.delete(slug) : next.add(slug);
      return next;
    });
  }, []);

  const handleFilter = useCallback(async (req: FilterRequest) => {
    const fullReq = { ...req, ...(spatialFilter ? { spatial: spatialFilter } : {}) };
    lastFilterRef.current = fullReq;
    try {
      const data = await filterFacilities({ ...fullReq, limit: 2000 });
      const ids = new Set<string>(data.features.map((f: { properties: { id: string } }) => f.properties.id));
      setHighlightIds(ids);
      setResultCount(data.total);
    } catch (err) {
      console.error("Filter failed:", err);
    }
  }, [spatialFilter]);

  const handlePolygonDrawn = useCallback((polygon: { type: "Polygon"; coordinates: number[][][] }) => {
    setSpatialFilter(polygon);
  }, []);

  const handleClearSpatial = useCallback(() => {
    setSpatialFilter(null);
  }, []);

  const handleClearHighlight = useCallback(() => {
    setHighlightIds(new Set());
    setResultCount(null);
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
          const DrawControl = require("~/components/DrawControl").default;
          const map = mapRef.current?.getMap?.() ?? null;
          return (
            <>
              <MapComponent
                ref={mapRef}
                layers={layers}
                visibleLayers={visibleLayers}
                highlightIds={highlightIds}
                onFacilityClick={handleFacilityClick}
              />
              <DrawControl
                map={map}
                onPolygonDrawn={handlePolygonDrawn}
                onClear={handleClearSpatial}
              />
            </>
          );
        }}
      </ClientOnly>

      <LayerPanel layers={layers} visibleLayers={visibleLayers} onToggle={toggleLayer} />
      <FilterPanel
        onFilter={handleFilter}
        resultCount={resultCount}
        onClearHighlight={handleClearHighlight}
      />

      {selectedFacility && (
        <FacilityDetail
          facility={selectedFacility as never}
          onClose={() => setSelectedFacility(null)}
        />
      )}
    </div>
  );
}
