# webapp/models.py
import os
from datetime import datetime

from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    DateTime,
    Date,
    JSON,
    Float,
    UniqueConstraint,
    ForeignKey,
    func
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://rewuser:rewpass@db:5432/real_estate",
)

engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
Base = declarative_base()
class RewListingUrl(Base):
    __tablename__ = "rew_listing_urls"

    id = Column(Integer, primary_key=True)
    url = Column(Text, unique=True, nullable=False)
    discovered_at = Column(DateTime(timezone=True), server_default=func.now())
    status = Column(Text, nullable=False, server_default="pending")
    last_attempt_at = Column(DateTime(timezone=True))
    attempts = Column(Integer, nullable=False, server_default="0")
    last_error = Column(Text)


class Property(Base):
    __tablename__ = "properties"

    id = Column(Integer, primary_key=True)
    street_address = Column(Text, nullable=False)
    city = Column(String(255), nullable=False)
    province = Column(String(32), nullable=False)
    postal_code = Column(String(32))
    lat = Column(Float)
    lng = Column(Float)
    canonical_address = Column(Text, unique=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(),
                        onupdate=func.now())


class Sale(Base):
    __tablename__ = "sales"
    id = Column(Integer, primary_key=True)
    property_id = Column(Integer, ForeignKey("properties.id"), nullable=False)
    sale_date = Column(Date, nullable=False)
    sale_price_cad = Column(Integer, nullable=False)
    list_price_cad = Column(Integer)
    mls_number = Column(String(32))
    source = Column(String(32), nullable=False)  # 'redfin', 'mls', etc.
    beds = Column(Float)
    baths = Column(Float)
    sqft = Column(Integer)
    lot_sqft = Column(Integer)
    raw_blob = Column(JSON)
    __table_args__ = (
        UniqueConstraint("property_id", "sale_date", "sale_price_cad", "source"),
    )


class Assessment(Base):
    __tablename__ = "assessments"
    id = Column(Integer, primary_key=True)
    property_id = Column(Integer, ForeignKey("properties.id"), nullable=False)
    assessment_year = Column(Integer, nullable=False)
    total_assessed_cad = Column(Integer, nullable=False)
    land_value = Column(Integer)
    building_value = Column(Integer)
    source = Column(String(32), nullable=False)  # 'bc_assessment'
    raw_blob = Column(JSON)
    __table_args__ = (
        UniqueConstraint("property_id", "assessment_year", "source"),
    )


class PropertyCharacteristics(Base):
    __tablename__ = "property_characteristics"
    id = Column(Integer, primary_key=True)
    property_id = Column(Integer, ForeignKey("properties.id"), nullable=False)
    as_of_date = Column(Date, nullable=False)
    source = Column(String(32), nullable=False)  # 'rew', 'redfin', 'bc_assessment'
    beds = Column(Float)
    baths = Column(Float)
    sqft_finished = Column(Integer)
    sqft_unfinished = Column(Integer)
    lot_sqft = Column(Integer)
    year_built = Column(Integer)
    raw_blob = Column(JSON)
    __table_args__ = (
        UniqueConstraint("property_id", "as_of_date", "source"),
    )


class RawScrape(Base):
    __tablename__ = "raw_scrapes"
    id = Column(Integer, primary_key=True)
    source = Column(String(32), nullable=False)  # 'rew', 'redfin', 'bc_assessment'
    url = Column(Text)
    scraped_at = Column(DateTime(timezone=True), server_default=func.now())
    http_status = Column(Integer)
    payload_type = Column(String(16))  # 'html', 'json'
    payload = Column(Text)


class RewListing(Base):
    __tablename__ = "rew_listings"

    id = Column(Integer, primary_key=True)

    # Link listing -> canonical property
    property_id = Column(Integer, ForeignKey("properties.id"))

    # identifiers
    rew_url = Column(Text, nullable=False)
    rew_slug = Column(String(255))
    rew_listing_id = Column(String(32))
    mls_number = Column(String(32))

    # address / location
    street_address = Column(Text)
    city = Column(String(255))
    neighbourhood = Column(String(255))
    subcity = Column(String(255))
    province = Column(String(32))
    postal_code = Column(String(32))
    lat = Column(Float)
    lng = Column(Float)

    # canonical address for de-duplication
    canonical_address = Column(Text)  # normalized string
    # (later: add a UNIQUE constraint/index in migrations if you want)

    # property info
    property_type = Column(String(64))
    property_type_human = Column(String(64))
    building_name = Column(Text)
    beds = Column(Integer)
    baths = Column(Integer)
    sqft = Column(Integer)

    # pricing / listing meta
    price_cad = Column(Integer)
    currency = Column(String(8))
    days_on_rew = Column(Integer)
    views = Column(Integer)
    board = Column(String(255))
    source = Column(String(255))
    section = Column(String(64))  # Buy/Rent/etc.
    office_name = Column(Text)

    scraped_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    raw_blob = Column(JSON)

    __table_args__ = (
        UniqueConstraint("rew_url", name="uq_rew_url"),
    )

