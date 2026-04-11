"""SQLAlchemy ORM models — reflect the schema defined in migrations/versions/001."""

import uuid
from datetime import date, datetime
from typing import Optional

from geoalchemy2 import Geometry
from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Double,
    Float,
    Integer,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Facility(Base):
    __tablename__ = "facilities"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    type: Mapped[str] = mapped_column(Text, nullable=False)
    subtype: Mapped[Optional[str]] = mapped_column(Text)
    address: Mapped[Optional[str]] = mapped_column(Text)
    city: Mapped[Optional[str]] = mapped_column(Text)
    county: Mapped[Optional[str]] = mapped_column(Text)
    state: Mapped[str] = mapped_column(String(2), default="CA")
    zip: Mapped[Optional[str]] = mapped_column(String(10))
    phone: Mapped[Optional[str]] = mapped_column(String(20))

    lat: Mapped[Optional[float]] = mapped_column(Double)
    lon: Mapped[Optional[float]] = mapped_column(Double)
    geom: Mapped[Optional[object]] = mapped_column(Geometry("POINT", srid=4326))

    # Source IDs
    cdph_id: Mapped[Optional[str]] = mapped_column(Text, unique=True)
    cms_npi: Mapped[Optional[str]] = mapped_column(Text, unique=True)
    oshpd_id: Mapped[Optional[str]] = mapped_column(Text, unique=True)
    cdss_id: Mapped[Optional[str]] = mapped_column(Text, unique=True)

    # License info
    license_status: Mapped[Optional[str]] = mapped_column(Text)
    license_number: Mapped[Optional[str]] = mapped_column(Text)
    license_expiry: Mapped[Optional[date]] = mapped_column(Date)
    certified_medicare: Mapped[bool] = mapped_column(Boolean, default=False)
    certified_medicaid: Mapped[bool] = mapped_column(Boolean, default=False)

    # Metadata
    primary_source: Mapped[str] = mapped_column(Text, nullable=False)
    geocode_source: Mapped[Optional[str]] = mapped_column(Text)
    geocode_confidence: Mapped[Optional[float]] = mapped_column(Float)
    last_verified: Mapped[Optional[date]] = mapped_column(Date)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    financials: Mapped[list["FacilityFinancial"]] = relationship(back_populates="facility")
    violations: Mapped[list["FacilityViolation"]] = relationship(back_populates="facility")


class FacilityFinancial(Base):
    __tablename__ = "facility_financials"
    __table_args__ = (UniqueConstraint("facility_id", "year", "source"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    facility_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    year: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)

    gross_revenue: Mapped[Optional[int]] = mapped_column(BigInteger)
    net_revenue: Mapped[Optional[int]] = mapped_column(BigInteger)
    total_expenses: Mapped[Optional[int]] = mapped_column(BigInteger)
    medicare_revenue: Mapped[Optional[int]] = mapped_column(BigInteger)
    medicaid_revenue: Mapped[Optional[int]] = mapped_column(BigInteger)
    private_revenue: Mapped[Optional[int]] = mapped_column(BigInteger)
    total_visits: Mapped[Optional[int]] = mapped_column(Integer)
    total_patients: Mapped[Optional[int]] = mapped_column(Integer)
    raw_report_id: Mapped[Optional[str]] = mapped_column(Text)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    facility: Mapped["Facility"] = relationship(back_populates="financials")


class FacilityViolation(Base):
    __tablename__ = "facility_violations"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    facility_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    survey_date: Mapped[Optional[date]] = mapped_column(Date)
    deficiency_tag: Mapped[Optional[str]] = mapped_column(Text)
    category: Mapped[Optional[str]] = mapped_column(Text)
    severity: Mapped[Optional[str]] = mapped_column(Text)
    scope: Mapped[Optional[str]] = mapped_column(Text)
    description: Mapped[Optional[str]] = mapped_column(Text)
    corrective_action: Mapped[Optional[str]] = mapped_column(Text)
    citation_id: Mapped[Optional[str]] = mapped_column(Text)
    resolved: Mapped[bool] = mapped_column(Boolean, default=False)
    resolved_date: Mapped[Optional[date]] = mapped_column(Date)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    facility: Mapped["Facility"] = relationship(back_populates="violations")


class Layer(Base):
    __tablename__ = "layers"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    slug: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    facility_types: Mapped[Optional[list[str]]] = mapped_column(ARRAY(Text))
    pmtiles_path: Mapped[Optional[str]] = mapped_column(Text)
    min_zoom: Mapped[int] = mapped_column(SmallInteger, default=4)
    max_zoom: Mapped[int] = mapped_column(SmallInteger, default=14)
    last_generated: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    record_count: Mapped[Optional[int]] = mapped_column(Integer)
    bbox: Mapped[Optional[dict]] = mapped_column(JSONB)
    attribute_schema: Mapped[Optional[dict]] = mapped_column(JSONB)
    access_policy: Mapped[str] = mapped_column(Text, default="public")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
