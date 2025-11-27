from typing import Optional, Dict

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import Property
from address_canonicalizer import canonical_address_from_parts
from geocoding import BBox 

def format_full_address(
    street_address: str,
    city: str,
    province: str,
    postal_code: Optional[str] = None,
) -> str:
    """
    Build a reasonably robust single-line address string for geocoding.
    """
    street_address = (street_address or "").strip()
    city = (city or "").strip()
    province = (province or "").strip()
    postal_code = (postal_code or "").strip() if postal_code else ""

    parts = [street_address]

    locality_parts = [p for p in [city, province, postal_code] if p]
    if locality_parts:
        parts.append(" ".join(locality_parts))

    return ", ".join(parts)

def normalize_address(
    street_address: str,
    city: str,
    province: str,
    postal_code: Optional[str] = None,
) -> str:
    """
    Canonical address used for de-duplication.

    Implemented via libpostal in address_canonicalizer.canonical_address_from_parts.
    """
    return canonical_address_from_parts(
        street_address=street_address,
        city=city,
        province=province,
        postal_code=postal_code,
    )


def _format_display_part(s: Optional[str]) -> Optional[str]:
    """
    Normalize a street / city string for display:
      - lowercases
      - trims
      - title-cases most words
      - keeps common directionals (W/E/N/S/NE/NW/SE/SW) uppercased
    """
    if not s:
        return s

    s = s.strip().lower()
    tokens = s.split()

    DIR = {"n", "s", "e", "w", "ne", "nw", "se", "sw"}

    out: list[str] = []
    for t in tokens:
        base = t.rstrip(",")
        suffix = "," if t.endswith(",") else ""

        if base in DIR:
            out.append(base.upper() + suffix)
        else:
            out.append(base.capitalize() + suffix)

    return " ".join(out)

async def get_or_create_property(
    session: AsyncSession,
    street: str,
    city: str,
    province: str,
    postal_code: Optional[str] = None,
    lat: Optional[float] = None,
    lng: Optional[float] = None,
    bbox: Optional[BBox] = None,
) -> Property:
    # Normalize for display
    street_fmt = _format_display_part(street)
    city_fmt = _format_display_part(city)
    prov_fmt = (province or "").strip().upper()
    postal_code_fmt = (postal_code or "").upper()

    canonical = normalize_address(street_fmt, city_fmt, prov_fmt, postal_code_fmt)

    result = await session.execute(
        select(Property).where(Property.canonical_address == canonical)
    )
    prop = result.scalar_one_or_none()
    if prop:
        updated = False
        if prop.lat is None and lat is not None:
            prop.lat = lat
            updated = True
        if prop.lng is None and lng is not None:
            prop.lng = lng
            updated = True
        
        # Only set bbox if we don't have one yet and we got one
        if prop.bbox is None and bbox is not None:
            if isinstance(bbox, BBox):
                prop.bbox = bbox.to_dict()
            else:
                prop.bbox = bbox
            updated = True

        if updated:
            await session.flush()
        return prop

    if isinstance(bbox, BBox):
        bbox_dict = bbox.to_dict()
    else:
        bbox_dict = bbox

    prop = Property(
        street_address=street_fmt,
        city=city_fmt,
        province=prov_fmt,
        postal_code=postal_code_fmt,
        lat=lat,
        lng=lng,
        bbox=bbox_dict,
        canonical_address=canonical,
    )
    session.add(prop)
    await session.commit()
    await session.refresh(prop)
    return prop
