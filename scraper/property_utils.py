from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import Property


def normalize_address(
    street_address: str,
    city: str,
    province: str,
    postal_code: Optional[str] = None,
) -> str:
    def norm(s: Optional[str]) -> Optional[str]:
        if not s:
            return None
        return (
            s.lower()
            .replace(",", " ")
            .replace(".", " ")
            .strip()
        )

    street_n = norm(street_address)
    city_n = norm(city)
    prov_n = norm(province)
    postal_n = norm(postal_code.replace(" ", "")) if postal_code else None

    parts = [p for p in [street_n, city_n, prov_n, postal_n] if p]
    return "|".join(parts)

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
        return prop

    prop = Property(
        street_address=street_fmt,
        city=city_fmt,
        province=prov_fmt,
        postal_code=postal_code_fmt,
        lat=lat,
        lng=lng,
        canonical_address=canonical,
    )
    session.add(prop)
    await session.commit()
    await session.refresh(prop)
    return prop
