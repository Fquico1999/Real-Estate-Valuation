# scraper/address_canonicalizer.py
from __future__ import annotations

from typing import Optional, Dict
import re

from postal.parser import parse_address  # libpostal

DIR_TOKENS = {"n", "s", "e", "w", "ne", "nw", "se", "sw"}
DIR_WORDS = {
    "n": "n", "north": "n",
    "s": "s", "south": "s",
    "e": "e", "east": "e",
    "w": "w", "west": "w",
    "ne": "ne", "northeast": "ne",
    "nw": "nw", "northwest": "nw",
    "se": "se", "southeast": "se",
    "sw": "sw", "southwest": "sw",
}
SUFFIXES = {
    "avenue": "ave",
    "ave": "ave",
    "street": "st",
    "st": "st",
    "road": "rd",
    "rd": "rd",
    "drive": "dr",
    "dr": "dr",
    "boulevard": "blvd",
    "blvd": "blvd",
    "lane": "ln",
    "ln": "ln",
    "court": "ct",
    "ct": "ct",
    "crescent": "cres",
    "cres": "cres",
    "place": "pl",
    "pl": "pl",
    "terrace": "ter",
    "ter": "ter",
    "circle": "cir",
    "cir": "cir",
    "way": "way",
    "trail": "trl",
    "trl": "trl",
    "highway": "hwy",
    "hwy": "hwy",
    # you can add more, but this covers 99% of Vancouver streets
}
UNIT_PREFIX_RE = re.compile(r"^\s*(?:#?\d+[/-])\s*(.+)$")


def _strip_leading_zeros(s: str) -> str:
    s = s.strip()
    m = re.match(r"^0+(\d+)$", s)
    return m.group(1) if m else s

def _extract_direction_from_raw(street_address: str) -> str | None:
    """
    Look at the original street string and try to detect a directional
    (W, E, N, S, West, etc) attached to the street name.

    This is used to fix cases where libpostal assigned the 'W' to city
    instead of road, e.g. '553 26th Ave W'.
    """
    if not street_address:
        return None

    s = street_address.lower()
    s = s.replace(".", " ")
    s = re.sub(r"[-–—]", " ", s)  # normalize dashes
    tokens = [t for t in s.split() if t]

    # Find the house number token
    house_idx = None
    for i, t in enumerate(tokens):
        if re.match(r"^\d+[a-z]?$", t):
            house_idx = i
            break

    if house_idx is None:
        return None

    # Everything after the house number is street name-ish
    after = tokens[house_idx + 1:]
    if not after:
        return None

    first = after[0]
    last = after[-1]

    # Direction at the *end* ('26th ave w')
    if last in DIR_WORDS:
        return DIR_WORDS[last]

    # Direction at the *start* ('w 26th ave')
    if first in DIR_WORDS:
        return DIR_WORDS[first]

    return None

def _normalize_unit(unit: str) -> str:
    """
    Normalize unit strings:
      - strip leading zeros
      - drop common prefixes like '#', 'apt', 'suite' if they sneak in
    """
    unit = unit.strip().lower()
    unit = unit.lstrip("#").strip()

    prefixes = ["apt", "apartment", "suite", "ste", "unit"]
    for p in prefixes:
        if unit.startswith(p + " "):
            unit = unit[len(p) + 1 :].strip()
            break

    unit = _strip_leading_zeros(unit)
    return unit

def _normalize_road(road: str) -> str:
    """
    Normalize roads:
      - move directionals to the end
      - normalize ordinates (26th -> 26)
      - normalize suffixes (avenue -> ave)
      - strip punctuation
    """
    tokens = road.strip().lower().split()

    cleaned = []
    dir_token = None

    for t in tokens:
        base = t.strip(",.")

        # direction? defer until end
        if base in DIR_TOKENS:
            dir_token = base
            continue

        # ordinal? -> numeric
        m = re.match(r"^(\d+)(st|nd|rd|th)$", base)
        if m:
            base = m.group(1)

        # suffix normalization
        base = SUFFIXES.get(base, base)

        cleaned.append(base)

    if dir_token:
        cleaned.append(dir_token)

    return " ".join(cleaned)

def _normalize_city(raw_city: str, libpostal_city: str | None = None) -> str:
    """
    Use the original city as the source of truth, but:
      - lowercase and trim
      - strip a leading direction token like 'w vancouver' -> 'vancouver'
    If original is empty, fall back to libpostal's city.
    """
    city = (raw_city or "").strip().lower()
    if not city and libpostal_city:
        city = libpostal_city.strip().lower()

    if not city:
        return city

    tokens = city.split()
    if tokens and tokens[0] in DIR_TOKENS:
        tokens = tokens[1:]
    return " ".join(tokens)


def normalize_street_for_geocoding(street_address: str) -> str:
    """
    Normalize a raw street string into something Nominatim-friendly using the
    same DIR_TOKENS/DIR_WORDS/SUFFIXES as canonicalization:

      - strip unit prefixes like '214-2235 Broadway E' -> '2235 Broadway E'
      - extract house number
      - normalize the road part with _normalize_road (direction + suffix logic)
    """
    if not street_address:
        return ""

    # 1) Strip unit prefixes like '214-2235 Something St'
    s = street_address.strip()
    m = UNIT_PREFIX_RE.match(s)
    if m:
        s = m.group(1)

    # 2) Lowercase, normalize punctuation similar to _extract_direction_from_raw
    s_lower = s.lower()
    s_lower = s_lower.replace(".", " ")
    s_lower = re.sub(r"[-–—]", " ", s_lower)
    tokens = [t for t in s_lower.split() if t]

    if not tokens:
        return s.strip()

    # 3) First numeric-ish token is the house number
    house = None
    rest_tokens: list[str] = []
    for t in tokens:
        if house is None and re.match(r"^\d+[a-z]?$", t):
            house = _strip_leading_zeros(t)
        else:
            rest_tokens.append(t)

    road_norm = None
    if rest_tokens:
        road_norm = _normalize_road(" ".join(rest_tokens))

    if house and road_norm:
        return f"{house} {road_norm}"
    if house:
        return house
    if road_norm:
        return road_norm

    # Fallback: return original, trimmed
    return s.strip()

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

    if unit:
        unit = _normalize_unit(unit)
    if house_number:
        house_number = _strip_leading_zeros(house_number)
    
    raw_dir = _extract_direction_from_raw(street_address or "")

    road_norm = None
    if road:
        road_norm = _normalize_road(road)

        # If libpostal didn't keep the direction on the road, but we see it in the
        # raw street string, append it.
        if raw_dir and raw_dir not in (road_norm.split()):
            road_norm = (road_norm + " " + raw_dir).strip()

    # Fall back to the original street if libpostal couldn't parse it
    street_bits = []
    if unit:
        street_bits.append(f"unit {unit}")
    if house_number:
        street_bits.append(house_number)
    if road_norm:
        street_bits.append(road_norm)

    if not street_bits and street_address:
        street_bits.append(street_address.strip().lower())

    street_norm = " ".join(street_bits)

    # City/province/postal
    city_norm = _normalize_city(city, components.get("city"))
    province_norm = (province or "").strip().lower()

    # Postal code as uppercase, no spaces (you already do this)
    postcode = components.get("postcode") or postal_code
    postcode_norm = postcode.replace(" ", "").upper() if postcode else ""

    parts = [p for p in [street_norm, city_norm, province_norm] if p]
    if postcode_norm:
        parts.append(postcode_norm)

    return "|".join(parts)

