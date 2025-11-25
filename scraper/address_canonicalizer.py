# scraper/address_canonicalizer.py
from __future__ import annotations

from typing import Optional, Dict

from postal.parser import parse_address  # libpostal


def _parse_with_libpostal(raw: str) -> Dict[str, str]:
    """
    Run libpostal's parser on a full address string and return a simple dict.

    Example:
        '101-3950 W 10th Ave, Vancouver BC V6R2G8'
        -> {'unit': '101', 'house_number': '3950', 'road': 'w 10th ave', ...}
    """
    components: Dict[str, str] = {}
    for value, label in parse_address(raw):
        value = (value or "").strip().lower()
        if not value:
            continue
        components[label] = value
    return components


def canonical_address_from_parts(
    street_address: str,
    city: str,
    province: str,
    postal_code: Optional[str] = None,
) -> str:
    """
    Build the canonical address string used for de-duplication.

    - Uses libpostal to parse the *full* address line.
    - Keeps unit (if present) as part of the canonical address.
    - Canonical format:
        'unit 101 3950 w 10th ave|vancouver|bc|V6R2G8'
    """
    # Defensive: normalize inputs a bit before handing off to libpostal
    street_address = (street_address or "").strip()
    city = (city or "").strip()
    province = (province or "").strip()
    postal_code = (postal_code or "").strip() if postal_code else ""

    # Build a single-line string for libpostal to parse
    line_parts = [street_address]
    locality_parts = [city, province, postal_code]
    locality_str = " ".join(p for p in locality_parts if p)
    if locality_str:
        line_parts.append(locality_str)

    raw_line = ", ".join(p for p in line_parts if p)

    components = _parse_with_libpostal(raw_line) if raw_line else {}

    # Pull out the interesting bits
    unit = components.get("unit") or components.get("level")
    house_number = components.get("house_number")
    road = components.get("road")

    # Fall back to the original street if libpostal couldn't parse it
    street_bits = []
    if unit:
        street_bits.append(f"unit {unit}")
    if house_number:
        street_bits.append(house_number)
    if road:
        street_bits.append(road)

    if not street_bits and street_address:
        street_bits.append(street_address.strip().lower())

    street_norm = " ".join(street_bits)

    # City/province/postal
    city_norm = components.get("city", city.lower())
    province_norm = components.get("state", province.lower())

    # Postal code as uppercase, no spaces
    postcode = components.get("postcode") or postal_code
    postcode_norm = postcode.replace(" ", "").upper() if postcode else ""

    parts = [p for p in [street_norm, city_norm, province_norm] if p]

    if postcode_norm:
        parts.append(postcode_norm)

    return "|".join(parts)
