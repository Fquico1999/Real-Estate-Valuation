# scraper/geocoding.py
import os
import asyncio
import logging
from dataclasses import dataclass
from typing import Tuple, Optional, Dict

import aiohttp

logger = logging.getLogger(__name__)

GEOCODER_URL = os.getenv("GEOCODER_URL", "http://nominatim:8080/search")
GEOCODER_TIMEOUT_SECONDS = float(os.getenv("GEOCODER_TIMEOUT_SECONDS", "5.0"))

# Classes/types we consider “good enough” for a property-level match
GOOD_CLASSES = {
    ("building", "yes"),
    ("building", "house"),
    ("building", "residential"),
    ("building", "apartments"),
    ("building", "detached"),
    ("building", "semi-detached"),
    ("building", "terrace"),
    ("building", "duplex"),
    ("building", "triplex"),
    ("place", "house"),
    ("place", "isolated_dwelling"),
}

@dataclass
class BBox:
    south: float
    north: float
    west: float
    east: float

    def to_dict(self) -> Dict[str, float]:
        return {
            "south": self.south,
            "north": self.north,
            "west": self.west,
            "east": self.east,
        }

@dataclass
class GeocodeResult:
    lat: Optional[float]
    lng: Optional[float]
    bbox: Optional[BBox] = None
    osm_class: Optional[str] = None
    osm_type: Optional[str] = None
    display_name: Optional[str] = None

    @property
    def has_bbox(self) -> bool:
        return self.bbox is not None


async def geocode(address: str) -> GeocodeResult:
    """
    Geocode an address using local Nominatim, returning centroid and (when
    appropriate) a bounding box representing the building footprint.

    Returns a GeocodeResult with lat/lng possibly None on failure.
    """
    if not address:
        return GeocodeResult(lat=None, lng=None)

    params = {
        "q": address,
        "format": "json",
        "limit": 5,           # fetch a few candidates
        "countrycodes": "ca",
        "addressdetails": 1,
    }

    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=GEOCODER_TIMEOUT_SECONDS)
        ) as session:
            async with session.get(GEOCODER_URL, params=params) as resp:
                if resp.status != 200:
                    logger.warning(f"Geocode failed ({resp.status}) for {address!r}")
                    return GeocodeResult(lat=None, lng=None)

                data = await resp.json()
    except asyncio.TimeoutError:
        logger.warning(f"Geocode timeout for {address!r}")
        return GeocodeResult(lat=None, lng=None)
    except Exception as e:
        logger.exception(f"Geocode error for {address!r}: {e}")
        return GeocodeResult(lat=None, lng=None)

    if not isinstance(data, list) or not data:
        logger.info(f"No geocode results for {address!r}")
        return GeocodeResult(lat=None, lng=None)

    # Filter ONLY building-level results
    building_candidates = []
    for cand in data:
        osm_class = cand.get("class")
        osm_type = cand.get("type")
        key = (osm_class, osm_type)

        # Option 1: fixed allowlist (strict)
        if key in GOOD_CLASSES:
            building_candidates.append(cand)

        # Option 2: or class == 'building' regardless of type
        elif osm_class == "building":
            building_candidates.append(cand)

    # If NO building is found → treat as NO RESULT
    if not building_candidates:
        logger.info(f"No building-level geocode results for {address!r}")
        return GeocodeResult(lat=None, lng=None)

    # Pick the first building candidate (usually most relevant)
    best = building_candidates[0]

    try:
        lat = float(best["lat"])
        lng = float(best["lon"])
    except Exception:
        logger.warning(f"Best geocode candidate missing lat/lon: {best}")
        return GeocodeResult(lat=None, lng=None)

    bbox_list = best.get("boundingbox") or []
    bbox_obj: Optional[BBox] = None
    if len(bbox_list) == 4:
        try:
            bbox_obj = BBox(
                south=float(bbox_list[0]),
                north=float(bbox_list[1]),
                west=float(bbox_list[2]),
                east=float(bbox_list[3]),
            )
        except Exception:
            bbox_obj = None

    result = GeocodeResult(
        lat=lat,
        lng=lng,
        bbox=bbox_obj,
        osm_class=best.get("class"),
        osm_type=best.get("type"),
        display_name=best.get("display_name"),
    )

    # Reject clearly too-broad matches like boundaries (city-level, etc.)
    if result.osm_class == "boundary":
        logger.info(
            f"Rejecting boundary-level geocode for {address!r}: {result.display_name}"
        )
        return GeocodeResult(lat=None, lng=None)

    return result
