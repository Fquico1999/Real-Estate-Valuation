# scraper/parsers.py
import json
import re
from datetime import date
from typing import Any, Dict, Optional, List, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from property_utils import normalize_address



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

def _parse_float(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    s = "".join(ch for ch in text if (ch.isdigit() or ch == "."))
    return float(s) if s else None


def _parse_days(text: str) -> Optional[int]:
    return _parse_int(text)


def _parse_views(text: str) -> Optional[int]:
    return _parse_int(text)


def _simple_canonical_address(
    street: Optional[str],
    city: Optional[str],
    postal: Optional[str],
    province: str = "BC",
) -> Optional[str]:
    """
    Wrapper around property_utils.normalize_address so that REW listing
    canonicalization uses the same libpostal-based logic as Properties.

    `province` defaults to 'BC' since REW listings are all BC in this project.
    """
    if not street and not city and not postal:
        return None

    return normalize_address(
        street_address=street or "",
        city=city or "",
        province=province,
        postal_code=postal,
    )


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


def parse_rew_assessment_history(data: Dict[str, Any]) -> List[Dict]:
    """
    data = {"assessmentHistory": [...]} extracted from GraphQL payload.
    """
    items = data.get("assessmentHistory") or []
    results = []

    for row in items:
        valuation_date = row.get("valuationDate")
        if not valuation_date:
            continue

        year = date.fromisoformat(valuation_date).year

        results.append({
            "assessment_year": year,
            "total_assessed_cad": int(row.get("value") or 0),
            "land_value": int(row.get("landValue") or 0) or None,
            "building_value": int(row.get("buildingValue") or 0) or None,
            "raw": row,
        })

    return results


def parse_rew_sales_history(data: Dict[str, Any]) -> List[Dict]:
    """
    data = {"salesHistory": [...]} extracted from GraphQL payload.
    """
    items = data.get("salesHistory") or []
    results = []

    for row in items:
        valuation_date = row.get("valuationDate")
        price = row.get("value")

        if not valuation_date or price is None:
            continue

        sale_date = date.fromisoformat(valuation_date)

        results.append({
            "sale_date": sale_date,
            "sale_price_cad": int(price),
            "raw": row,
        })

    return results

def _extract_bc_main_address(html: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Parse the big heading like:
      "3950 10TH AVE W VANCOUVER V6R 2G8"
    into (street_address, city, postal_code)
    """
    soup = BeautifulSoup(html, "html.parser")
    tag = soup.select_one("#mainaddresstitle")
    if not tag:
        return None, None, None

    txt = tag.get_text(" ", strip=True)

    # Split postal code off the end if present
    m = re.search(r"(.+?)\s+([A-Z]\d[A-Z]\s*\d[A-Z]\d)$", txt)
    if m:
        pre = m.group(1)
        postal_raw = m.group(2).replace(" ", "")
        postal = postal_raw[:3] + " " + postal_raw[3:] if len(postal_raw) == 6 else postal_raw
    else:
        pre = txt
        postal = None

    parts = pre.split()
    if len(parts) >= 2:
        city = parts[-1]
        street = " ".join(parts[:-1])
    else:
        street = pre
        city = None

    return street, city, postal


def parse_bc_assessment_property_characteristics(html_content: str) -> Dict[str, Any]:
    """
    Parse BC Assessment HTML to extract physical property characteristics.

    Returns a dict that can be mapped directly into PropertyCharacteristics +
    raw_blob.
    """
    soup = BeautifulSoup(html_content, "html.parser")
    data: Dict[str, Any] = {}

    def get_text(selector: str) -> Optional[str]:
        el = soup.select_one(selector)
        return el.get_text(strip=True) if el else None

    # --- as_of_date from #lblLastAssessmentDate ---
    assessment_date_text = get_text("#lblLastAssessmentDate")
    current_year = None
    if assessment_date_text:
        m = re.search(r"(\d{4})", assessment_date_text)
        if m:
            current_year = int(m.group(1))

    if current_year:
        data["as_of_date"] = date(current_year, 1, 1)
    else:
        data["as_of_date"] = date.today()

    # Basic fields
    data["year_built"] = _parse_int(get_text("#lblYearBuilt"))
    data["beds"] = _parse_float(get_text("#lblBedrooms"))
    data["baths"] = _parse_float(get_text("#lblBathRooms"))
    data["description"] = get_text("#lblDescription")
    data["carports"] = _parse_int(get_text("#lblCarPorts"))
    data["garages"] = _parse_int(get_text("#lblGarages"))
    data["building_storeys"] = _parse_int(get_text("#lblStoriesBuilding"))
    land_size_raw = get_text("#lblLandSize")
    data["land_size_raw"] = land_size_raw

    # Floor areas
    first_floor = _parse_int(get_text("#lblFirstFloorArea"))
    second_floor = _parse_int(get_text("#lblSecondFloorArea"))
    basement_finish = _parse_int(get_text("#lblBasementFinishArea"))

    total_finished = (first_floor or 0) + (second_floor or 0) + (basement_finish or 0)
    data["sqft_finished"] = total_finished or None
    data["sqft_unfinished"] = None

    # Lot sqft from '33 x 122.5 Ft' style string
    lot_sqft: Optional[int] = None
    if land_size_raw:
        m = re.search(r"([\d\.]+)\s*x\s*([\d\.]+)\s*Ft", land_size_raw, re.IGNORECASE)
        if m:
            try:
                dim1 = float(m.group(1))
                dim2 = float(m.group(2))
                lot_sqft = int(round(dim1 * dim2))
            except Exception:
                lot_sqft = None
    data["lot_sqft"] = lot_sqft

    # Extra area fields if you want them later
    data["strata_area"] = _parse_int(get_text("#lblStrataTotalArea"))
    data["gross_leasable_area"] = _parse_int(get_text("#lblGrossLeasableArea"))
    data["net_leasable_area"] = _parse_int(get_text("#lblNetLeasableArea"))
    data["no_of_apartment_units"] = _parse_int(get_text("#lblNumberUnitApartment"))

    return data


def parse_bc_assessment_assessments(html_content: str) -> List[Dict[str, Any]]:
    """
    Parse current & previous year assessed values from BC Assessment.
    Returns a list of dicts for Assessment rows (without property_id/source).
    """
    soup = BeautifulSoup(html_content, "html.parser")

    def get_text(selector: str) -> Optional[str]:
        el = soup.select_one(selector)
        return el.get_text(strip=True) if el else None

    def clean_money(selector: str) -> Optional[int]:
        txt = get_text(selector)
        if not txt:
            return None
        cleaned = re.sub(r"[$,\s]", "", txt)
        try:
            return int(cleaned)
        except Exception:
            return None

    # Assessment year(s)
    assessment_date_text = get_text("#lblLastAssessmentDate")
    current_year = None
    prev_year = None
    if assessment_date_text:
        m = re.search(r"(\d{4})", assessment_date_text)
        if m:
            current_year = int(m.group(1))
            prev_year = current_year - 1

    rows: List[Dict[str, Any]] = []

    if current_year:
        total = clean_money("div.total-value span#lblTotalAssessedValue")
        if total is not None:
            rows.append(
                {
                    "assessment_year": current_year,
                    "total_assessed_cad": total,
                    "land_value": clean_money("div.land-building-value p#lblTotalAssessedLand"),
                    "building_value": clean_money(
                        "div.land-building-value p#lblTotalAssessedBuilding"
                    ),
                }
            )

    if prev_year:
        total_prev = clean_money("div.previous-year-value p#lblPreviousAssessedValue")
        if total_prev is not None:
            rows.append(
                {
                    "assessment_year": prev_year,
                    "total_assessed_cad": total_prev,
                    "land_value": clean_money(
                        "div.previous-year-value p#lblPreviousAssessedLand"
                    ),
                    "building_value": clean_money(
                        "div.previous-year-value p#lblPreviousAssessedBuilding"
                    ),
                }
            )

    return rows


def parse_bc_assessment_neighbor_urls(html_content: str, base_url: str = "https://www.bcassessment.ca") -> List[str]:
    """
    Extract neighbouring property detail URLs from the 'Neighbouring properties'
    panel (desktop & mobile variants).
    """
    soup = BeautifulSoup(html_content, "html.parser")
    urls: set[str] = set()

    # 1) Direct <a href="/Property/Info/.../"> links in panels
    for a in soup.select(
        "#NerbyProperties-mobile .property-panel-mobile a[href], "
        "#NerbyProperties .property-panel-desktop a[href]"
    ):
        href = a.get("href")
        if not href:
            continue
        if "/Property/Info/" in href:
            urls.add(urljoin(base_url, href))

    # 2) Buttons like: onclick="window.location.href = '/Property/Info/.../'"
    for btn in soup.select(
        "#NerbyProperties-mobile .details-button button[onclick], "
        "#NerbyProperties .details-button button[onclick]"
    ):
        onclick = btn.get("onclick") or ""
        m = re.search(r"window\.location\.href\s*=\s*'([^']+)'", onclick)
        if m:
            href = m.group(1)
            if "/Property/Info/" in href:
                urls.add(urljoin(base_url, href))

    return sorted(urls)