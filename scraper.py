import requests, csv, os, time, re

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

def start_scrape_job(keyword, city, max_results, out_path, job_id, progress_callback=None):
    if progress_callback:
        progress_callback({"state": "started", "count": 0})

    keyword_lower = keyword.lower()

    # Tag filters for IT/business-related places
    if any(word in keyword_lower for word in ["it", "software", "tech", "technology", "company", "office", "business"]):
        tag_filters = [
            'office=it',
            'office=software',
            'office=company',
            'office=business',
            'office=technology',
            'craft=electronics'
        ]
    else:
        tag_filters = ['office', 'amenity']

    # Build the Overpass query
    tag_query = " ".join([
        f'node[{f}](area.searchArea); way[{f}](area.searchArea); relation[{f}](area.searchArea);'
        for f in tag_filters
    ])

    query = f"""
    [out:json][timeout:90];
    area["name"="{city}"]->.searchArea;
    (
      {tag_query}
      node["name"~"{keyword}",i](area.searchArea);
      way["name"~"{keyword}",i](area.searchArea);
      relation["name"~"{keyword}",i](area.searchArea);
    );
    out center {max_results};
    """

    try:
        response = requests.get(OVERPASS_URL, params={"data": query}, timeout=120)
        response.raise_for_status()
    except Exception as e:
        raise Exception(f"Overpass API error: {e}")

    data = response.json()
    elements = data.get("elements", [])
    results = []

    # Fallback bounding box for Ahmedabad (you can replace for other cities)
    if not elements:
        print(f"No results for {city}, retrying with fallback bounding box...")
        fallback_query = f"""
        [out:json][timeout:60];
        (
          node["office"](23.00,72.45,23.15,72.70);
          way["office"](23.00,72.45,23.15,72.70);
        );
        out center {max_results};
        """
        response = requests.get(OVERPASS_URL, params={"data": fallback_query}, timeout=90)
        elements = response.json().get("elements", [])

    for i, el in enumerate(elements[:max_results]):
        tags = el.get("tags", {})
        category = tags.get("office") or tags.get("craft") or ""
        if not category or any(c in category for c in ["school", "hospital", "clinic"]):
            continue

        name = tags.get("name", "")
        addr = tags.get("addr:full", "") or tags.get("addr:street", "")
        phone = tags.get("phone", "") or tags.get("contact:phone", "")
        website = tags.get("website", "") or tags.get("contact:website", "")
        email = tags.get("email", "") or tags.get("contact:email", "")
        lat = el.get("lat") or el.get("center", {}).get("lat")
        lng = el.get("lon") or el.get("center", {}).get("lon")

        # Filter out unwanted entries
        if name and not any(bad in name.lower() for bad in ["hospital", "school", "clinic"]):
            results.append({
                "name": name,
                "address": addr,
                "category": category,
                "phone": phone,
                "email": email,
                "website": website,
                "lat": lat,
                "lng": lng
            })

        if progress_callback:
            progress_callback({"count": len(results)})
        time.sleep(0.05)

    # Write to CSV
    keys = ["name", "address", "category", "phone", "email", "website", "lat", "lng"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(results)

    if progress_callback:
        progress_callback({"finished": True, "count": len(results)})
    return {"count": len(results)}
