const API_BASE = process.env.EXPO_PUBLIC_API_URL ?? "http://localhost:8000";

export interface Layer {
  id: string; slug: string; name: string;
  facility_types: string[] | null;
  min_zoom: number; max_zoom: number;
  record_count: number | null; access_policy: string;
  description: string | null; last_generated: string | null;
}

export async function fetchLayers(): Promise<Layer[]> {
  const res = await fetch(`${API_BASE}/layers`);
  return res.json();
}

export async function getFacility(id: string) {
  const res = await fetch(`${API_BASE}/facilities/${id}`);
  return res.json();
}

export async function filterFacilities(req: object) {
  const res = await fetch(`${API_BASE}/facilities/filter`, {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify(req),
  });
  return res.json();
}

export function pmtilesUrl(slug: string): string {
  return `${API_BASE}/tiles/${slug}.pmtiles`;
}
