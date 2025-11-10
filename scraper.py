import requests, time, re
from bs4 import BeautifulSoup

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# Map user keyword to OSM tags for better coverage (focus on IT/company searches)
def build_tag_filters_for_keyword(keyword):
    k = keyword.lower()
    if any(w in k for w in ["it", "software", "tech", "technology", "startup", "company", "office", "business"]):
        return [
            'office=it',
            'office=software',
            'office=company',
            'office=business',
            'office=technology',
            'craft=electronics'
        ]
    # fallback: common business tags
    if any(w in k for w in ["restaurant","cafe","food","bar","hotel"]):
        return ['amenity=restaurant','amenity=cafe','amenity=bar','tourism=hotel','amenity=fast_food']
    if any(w in k for w in ["hospital","clinic","doctor"]):
        return ['amenity=hospital','amenity=clinic','amenity=doctors']
    if any(w in k for w in ["school","college","university"]):
        return ['amenity=school','amenity=college','amenity=university']
    return ['amenity','shop','office']

# Perform Overpass query for a keyword and city, return list of dicts
def scrape_osm_keyword(keyword, city, max_results=50, delay=1.0, progress_cb=None):
    tag_filters = build_tag_filters_for_keyword(keyword)
    # build tag query for Overpass QL
    tag_query_parts = []
    for f in tag_filters:
        tag_query_parts.append(f'node[{f}](area.searchArea); way[{f}](area.searchArea); relation[{f}](area.searchArea);')
    tag_query = " ".join(tag_query_parts)

    # Overpass QL: area by name (works often) + search by tag and name (case-insensitive)
    q = f"""
    [out:json][timeout:60];
    area["name"="{city}"]->.searchArea;
    (
      {tag_query}
      node["name"~"{keyword}",i](area.searchArea);
      way["name"~"{keyword}",i](area.searchArea);
      relation["name"~"{keyword}",i](area.searchArea);
    );
    out center {max_results};
    """

    # polite request
    resp = requests.get(OVERPASS_URL, params={"data": q}, timeout=90)
    if resp.status_code != 200:
        # raise an exception so caller can mark as failed
        raise Exception(f"Overpass returned {resp.status_code}: {resp.text[:200]}")
    data = resp.json()
    elements = data.get("elements", [])
    results = []

    # fallback: if no elements and city might not have an area tag, use approximate bounding box of city center
    if not elements:
        # we won't attempt to geocode city here; as a fallback, return empty list (caller may adjust)
        # Alternatively you may add pre-defined boxes per city if needed.
        return results

    for i, el in enumerate(elements[:max_results]):
        tags = el.get("tags", {})
        name = tags.get("name", "")
        address = tags.get("addr:full", "") or tags.get("addr:street", "") or tags.get("addr:housenumber", "")
        category = tags.get("office") or tags.get("amenity") or tags.get("shop") or tags.get("craft") or ""
        phone = tags.get("phone") or tags.get("contact:phone") or ""
        website = tags.get("website") or tags.get("contact:website") or ""
        email = tags.get("email") or tags.get("contact:email") or ""
        lat = el.get("lat") or el.get("center", {}).get("lat")
        lng = el.get("lon") or el.get("center", {}).get("lon")

        row = {
            "name": name,
            "address": address,
            "category": category,
            "phone": phone,
            "website": website,
            "email": email,
            "lat": lat,
            "lng": lng
        }
        results.append(row)
        if progress_cb:
            progress_cb({"count": i+1})
    # polite delay after the query
    time.sleep(delay)
    return results

# Optional: fetch website and try to extract emails (very basic)
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", re.I)

def scrape_website_email(url, timeout=6):
    # normalize simple missing scheme
    if not url.startswith("http"):
        url = "http://" + url
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent":"Mozilla/5.0 (compatible)"})
        if r.status_code != 200:
            return ""
        text = r.text
        # try direct email matches
        emails = EMAIL_RE.findall(text)
        if emails:
            # return first unique
            return list(dict.fromkeys([e.strip() for e in emails]))[0]
        # fallback: try to parse "mailto:" links
        soup = BeautifulSoup(text, "lxml")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("mailto:"):
                e = href.split("mailto:")[1].split("?")[0]
                if EMAIL_RE.match(e):
                    return e
        return ""
    except Exception:
        return ""
