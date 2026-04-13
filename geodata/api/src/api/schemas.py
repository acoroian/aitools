from typing import Any

from pydantic import BaseModel, Field


class SpatialFilter(BaseModel):
    """GeoJSON polygon drawn by the user on the map."""

    type: str = "Polygon"
    coordinates: list[list[list[float]]]


class FacilityFilterRequest(BaseModel):
    facility_types: list[str] | None = Field(None, description="e.g. ['home_health', 'hospice']")
    license_status: str | None = Field(None, description="e.g. 'active'")
    county: str | None = None
    gross_revenue_min: int | None = Field(None, ge=0)
    gross_revenue_max: int | None = Field(None, ge=0)
    # Violation filters (Phase 3 — via facility_violation_rollup)
    violation_count_min: int | None = Field(None, ge=0)
    violation_count_max: int | None = Field(None, ge=0)
    violation_count_12mo_min: int | None = Field(None, ge=0)
    max_severity_level_min: int | None = Field(None, ge=0, le=10)
    has_ij_12mo: bool | None = None
    survey_date_after: str | None = Field(None, description="ISO date YYYY-MM-DD")
    year: int | None = Field(None, description="Financial data year filter")
    certified_medicare: bool | None = None
    certified_medicaid: bool | None = None
    spatial: SpatialFilter | None = Field(None, description="Polygon to intersect with")
    limit: int = Field(500, ge=1, le=5000)
    offset: int = Field(0, ge=0)


class FacilityProperties(BaseModel):
    id: str
    name: str
    type: str
    subtype: str | None
    address: str | None
    city: str | None
    county: str | None
    zip: str | None
    license_status: str | None
    certified_medicare: bool
    certified_medicaid: bool
    gross_revenue: int | None
    revenue_year: int | None
    violation_count: int
    last_violation: str | None


class GeoJSONFeature(BaseModel):
    type: str = "Feature"
    geometry: dict[str, Any]
    properties: dict[str, Any]


class GeoJSONFeatureCollection(BaseModel):
    type: str = "FeatureCollection"
    features: list[GeoJSONFeature]
    total: int


class LayerResponse(BaseModel):
    id: str
    slug: str
    name: str
    description: str | None
    facility_types: list[str] | None
    min_zoom: int
    max_zoom: int
    last_generated: str | None
    record_count: int | None
    bbox: dict[str, Any] | None
    access_policy: str
