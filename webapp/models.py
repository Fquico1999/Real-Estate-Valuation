# webapp/models.py
import os
from datetime import datetime

from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    DateTime,
    JSON,
    Float,
    UniqueConstraint,
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

class RewListing(Base):
    __tablename__ = "rew_listings"

    id = Column(Integer, primary_key=True)
    rew_url = Column(Text, nullable=False)
    rew_slug = Column(String(255))
    rew_listing_id = Column(String(32))
    mls_number = Column(String(32))

    street_address = Column(Text)
    city = Column(String(255))
    neighbourhood = Column(String(255))
    subcity = Column(String(255))
    province = Column(String(32))
    postal_code = Column(String(32))
    lat = Column(Float)
    lng = Column(Float)
    canonical_address = Column(Text)

    property_type = Column(String(64))
    property_type_human = Column(String(64))
    building_name = Column(Text)
    beds = Column(Integer)
    baths = Column(Integer)
    sqft = Column(Integer)

    price_cad = Column(Integer)
    currency = Column(String(8))
    days_on_rew = Column(Integer)
    views = Column(Integer)
    board = Column(String(255))
    source = Column(String(255))
    section = Column(String(64))
    office_name = Column(Text)

    scraped_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    raw_blob = Column(JSON)

    __table_args__ = (
        UniqueConstraint("rew_url", name="uq_rew_url"),
    )
