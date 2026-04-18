"""SQLAlchemy ORM models — reflect the schema defined in migrations/versions/001."""

import uuid
from datetime import date, datetime

from geoalchemy2 import Geometry
from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Double,
    Float,
    ForeignKey,
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
    subtype: Mapped[str | None] = mapped_column(Text)
    address: Mapped[str | None] = mapped_column(Text)
    city: Mapped[str | None] = mapped_column(Text)
    county: Mapped[str | None] = mapped_column(Text)
    state: Mapped[str] = mapped_column(String(2), default="CA")
    zip: Mapped[str | None] = mapped_column(String(10))
    phone: Mapped[str | None] = mapped_column(String(20))

    lat: Mapped[float | None] = mapped_column(Double)
    lon: Mapped[float | None] = mapped_column(Double)
    geom: Mapped[object | None] = mapped_column(Geometry("POINT", srid=4326))

    # Source IDs — only cdph_id is unique; NPI/OSHPD/CDSS/CCN can appear on
    # multiple CDPH records (e.g. satellite locations sharing an NPI)
    cdph_id: Mapped[str | None] = mapped_column(Text, unique=True)
    cms_npi: Mapped[str | None] = mapped_column(Text)
    ccn: Mapped[str | None] = mapped_column(Text)  # CMS Certification Number
    oshpd_id: Mapped[str | None] = mapped_column(Text)
    cdss_id: Mapped[str | None] = mapped_column(Text)

    # License info
    license_status: Mapped[str | None] = mapped_column(Text)
    license_number: Mapped[str | None] = mapped_column(Text)
    license_expiry: Mapped[date | None] = mapped_column(Date)
    certified_medicare: Mapped[bool] = mapped_column(Boolean, default=False)
    certified_medicaid: Mapped[bool] = mapped_column(Boolean, default=False)

    # Metadata
    primary_source: Mapped[str] = mapped_column(Text, nullable=False)
    geocode_source: Mapped[str | None] = mapped_column(Text)
    geocode_confidence: Mapped[float | None] = mapped_column(Float)
    last_verified: Mapped[date | None] = mapped_column(Date)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    financials: Mapped[list["FacilityFinancial"]] = relationship(back_populates="facility")
    violations: Mapped[list["FacilityViolation"]] = relationship(back_populates="facility")


class FacilityFinancial(Base):
    __tablename__ = "facility_financials"
    __table_args__ = (UniqueConstraint("facility_id", "year", "source"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    facility_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("facilities.id"),
        nullable=False,
    )
    year: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)

    gross_revenue: Mapped[int | None] = mapped_column(BigInteger)
    net_revenue: Mapped[int | None] = mapped_column(BigInteger)
    total_expenses: Mapped[int | None] = mapped_column(BigInteger)
    medicare_revenue: Mapped[int | None] = mapped_column(BigInteger)
    medicaid_revenue: Mapped[int | None] = mapped_column(BigInteger)
    private_revenue: Mapped[int | None] = mapped_column(BigInteger)
    total_visits: Mapped[int | None] = mapped_column(Integer)
    total_patients: Mapped[int | None] = mapped_column(Integer)
    raw_report_id: Mapped[str | None] = mapped_column(Text)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    facility: Mapped["Facility"] = relationship(back_populates="financials")


class FacilityViolation(Base):
    __tablename__ = "facility_violations"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    facility_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("facilities.id"),
        nullable=False,
    )
    source: Mapped[str] = mapped_column(Text, nullable=False)
    survey_date: Mapped[date | None] = mapped_column(Date)
    deficiency_tag: Mapped[str | None] = mapped_column(Text)
    category: Mapped[str | None] = mapped_column(Text)
    severity: Mapped[str | None] = mapped_column(Text)
    scope: Mapped[str | None] = mapped_column(Text)
    description: Mapped[str | None] = mapped_column(Text)
    corrective_action: Mapped[str | None] = mapped_column(Text)
    citation_id: Mapped[str | None] = mapped_column(Text)
    resolved: Mapped[bool] = mapped_column(Boolean, default=False)
    resolved_date: Mapped[date | None] = mapped_column(Date)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    facility: Mapped["Facility"] = relationship(back_populates="violations")


class Layer(Base):
    __tablename__ = "layers"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    slug: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    facility_types: Mapped[list[str] | None] = mapped_column(ARRAY(Text))
    pmtiles_path: Mapped[str | None] = mapped_column(Text)
    min_zoom: Mapped[int] = mapped_column(SmallInteger, default=4)
    max_zoom: Mapped[int] = mapped_column(SmallInteger, default=14)
    last_generated: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    record_count: Mapped[int | None] = mapped_column(Integer)
    bbox: Mapped[dict | None] = mapped_column(JSONB)
    attribute_schema: Mapped[dict | None] = mapped_column(JSONB)
    access_policy: Mapped[str] = mapped_column(Text, default="public")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
