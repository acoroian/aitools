const API_BASE = typeof window !== "undefined"
  ? (import.meta.env.VITE_API_URL ?? "http://localhost:8000")
  : "http://localhost:8000";

export interface Layer {
  id: string;
  slug: string;
  name: string;
  description: string | null;
  facility_types: string[] | null;
  min_zoom: number;
  max_zoom: number;
  last_generated: string | null;
  record_count: number | null;
  access_policy: string;
}

export interface FilterRequest {
  facility_types?: string[];
  license_status?: string;
  county?: string;
  gross_revenue_min?: number;
  gross_revenue_max?: number;
  violation_count_max?: number;
  certified_medicare?: boolean;
  certified_medicaid?: boolean;
  spatial?: { type: "Polygon"; coordinates: number[][][] };
  limit?: number;
}

export async function fetchLayers(): Promise<Layer[]> {
  const res = await fetch(`${API_BASE}/layers`);
  if (!res.ok) throw new Error("Failed to fetch layers");
  return res.json();
}

export async function filterFacilities(req: FilterRequest) {
  const res = await fetch(`${API_BASE}/facilities/filter`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(req),
  });
  if (!res.ok) throw new Error("Filter request failed");
  return res.json();
}

export async function getFacility(id: string) {
  const res = await fetch(`${API_BASE}/facilities/${id}`);
  if (!res.ok) throw new Error("Facility not found");
  return res.json();
}

export function pmtilesUrl(layerSlug: string): string {
  return `${API_BASE}/tiles/${layerSlug}.pmtiles`;
}
