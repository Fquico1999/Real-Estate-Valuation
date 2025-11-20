# scraper/parsers.py
import json
import re
from typing import Any, Dict, Optional

from bs4 import BeautifulSoup


def _find_json_ld_blocks(soup: BeautifulSoup) -> list[dict]:
    blocks: list[dict] = []
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            txt = tag.string or tag.text
            if not txt:
                continue
            data = json.loads(txt)
            if isinstance(data, list):
                blocks.extend(data)
            else:
                blocks.append(data)
        except Exception:
            continue
    return blocks


def _extract_singlefamily(blocks: list[dict]) -> Optional[dict]:
    for b in blocks:
        if isinstance(b, dict) and b.get("@type") in (
            "SingleFamilyResidence",
            "Apartment",
            "Condominium",
        ):
            return b
    return None


def _extract_event(blocks: list[dict]) -> Optional[dict]:
    for b in blocks:
        if isinstance(b, dict) and b.get("@type") == "Event":
            return b
    return None


def _parse_int(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    s = "".join(ch for ch in text if ch.isdigit())
    return int(s) if s else None


def _parse_days(text: str) -> Optional[int]:
    return _parse_int(text)


def _parse_views(text: str) -> Optional[int]:
    return _parse_int(text)


def _simple_canonical_address(street: Optional[str], city: Optional[str], postal: Optional[str]) -> Optional[str]:
    """
    Very simple canonicalization:
      - lowercases
      - strips spaces at ends
      - removes commas
      - removes internal spaces in postal code
    Later you can replace this with a libpostal-based parser.
    """
    if not street and not city and not postal:
        return None

    def norm(s: Optional[str]) -> Optional[str]:
        if not s:
            return None
        return (
            s.lower()
            .replace(",", " ")
            .replace(".", " ")
            .strip()
        )

    street_n = norm(street)
    city_n = norm(city)
    postal_n = norm(postal.replace(" ", "")) if postal else None

    parts = [p for p in [street_n, city_n, postal_n] if p]
    return "|".join(parts) if parts else None


def parse_rew_listing(html: str, url: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")

    data: Dict[str, Any] = {
        "rew_url": url,
        "rew_slug": url.rstrip("/").split("/")[-1] if url else None,
    }

    # 1) JSON-LD
    blocks = _find_json_ld_blocks(soup)
    single = _extract_singlefamily(blocks)
    event = _extract_event(blocks)

    if single:
        addr = single.get("address") or {}
        geo = single.get("geo") or {}

        data["street_address"] = addr.get("streetAddress")
        data["neighbourhood"] = addr.get("addressLocality")
        data["province"] = addr.get("addressRegion")
        data["postal_code"] = addr.get("postalCode")
        data["lat"] = geo.get("latitude")
        data["lng"] = geo.get("longitude")

    if event:
        offers = event.get("offers") or {}
        price = offers.get("price")
        currency = offers.get("priceCurrency")
        if isinstance(price, (int, float, str)):
            data["price_cad"] = int(price)
        data["currency"] = currency

    # 2) DataLayer info
    for script in soup.find_all("script"):
        txt = script.string or script.text or ""
        if "dataLayer.push" not in txt:
            continue

        city_match = re.search(r"propertyCity': '([^']+)'", txt)
        neigh_match = re.search(r"propertyNeighbourhood': '([^']+)'", txt)
        price_match = re.search(r"propertyPrice': '([^']+)'", txt)
        type_match = re.search(r"propertyType': '([^']+)'", txt)
        subcity_match = re.search(r"propertySubcity': '([^']+)'", txt)
        id_match = re.search(r"listingID': '([^']+)'", txt)
        section_match = re.search(r"propertySection': '([^']+)'", txt)

        if city_match:
            data["city"] = city_match.group(1)
        if neigh_match:
            data.setdefault("neighbourhood", neigh_match.group(1))
        if price_match and "price_cad" not in data:
            data["price_cad"] = _parse_int(price_match.group(1))
        if type_match:
            data["property_type"] = type_match.group(1)
        if subcity_match:
            data["subcity"] = subcity_match.group(1)
        if id_match:
            data["rew_listing_id"] = id_match.group(1)
        if section_match:
            data["section"] = section_match.group(1)
        break

    # 3) header details for beds, baths, sqft, property type label
    details_ul = soup.select_one("ul.listingheader-details")
    if details_ul:
        bed_li = details_ul.find("li", attrs={"data-listing-num-bedrooms": True})
        bath_li = details_ul.find("li", attrs={"data-listing-num-bathrooms": True})
        sqft_li = details_ul.find("li", attrs={"data-listing-sqft": True})

        if bed_li:
            data["beds"] = _parse_int(bed_li.get("data-listing-num-bedrooms"))
        if bath_li:
            data["baths"] = _parse_int(bath_li.get("data-listing-num-bathrooms"))
        if sqft_li:
            data["sqft"] = _parse_int(sqft_li.get("data-listing-sqft"))

        li_tags = details_ul.find_all("li")
        if li_tags:
            last_text = li_tags[-1].get_text(strip=True)
            data["property_type_human"] = last_text

    # 4) label-value generic helper
    def find_value_for_label(label_text: str) -> Optional[str]:
        label_div = soup.find("div", string=lambda t: t and label_text in t)
        if not label_div:
            return None
        val_div = label_div.find_next("div")
        return val_div.get_text(strip=True) if val_div else None

    data["mls_number"] = find_value_for_label("MLS")
    days_text = find_value_for_label("Days")
    views_text = find_value_for_label("Property Views")
    data["days_on_rew"] = _parse_days(days_text) if days_text else None
    data["views"] = _parse_views(views_text) if views_text else None
    data["source"] = find_value_for_label("Source")
    data["board"] = find_value_for_label("Board")

    # building name, if present
    bld = soup.select_one(".buildingoverview header a")
    if bld:
        data["building_name"] = bld.get_text(strip=True)

    # office name heuristic
    for script in soup.find_all("script"):
        txt = script.string or script.text or ""
        if '"office"' in txt:
            office_match = re.search(r'"office":"([^"]+)"', txt)
            if office_match:
                data["office_name"] = office_match.group(1)
                break

    # address canonicalization for dedupe
    data["canonical_address"] = _simple_canonical_address(
        data.get("street_address"),
        data.get("city"),
        data.get("postal_code"),
    )

    return data
