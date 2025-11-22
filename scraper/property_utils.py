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


async def get_or_create_property(
    session: AsyncSession,
    street: str,
    city: str,
    province: str,
    postal_code: Optional[str] = None,
    lat: Optional[float] = None,
    lng: Optional[float] = None,
) -> Property:
    canonical = normalize_address(street, city, province, postal_code)

    result = await session.execute(
        select(Property).where(Property.canonical_address == canonical)
    )
    prop = result.scalar_one_or_none()
    if prop:
        return prop

    prop = Property(
        street_address=street,
        city=city,
        province=province,
        postal_code=postal_code,
        lat=lat,
        lng=lng,
        canonical_address=canonical,
    )
    session.add(prop)
    await session.commit()
    await session.refresh(prop)
    return prop
