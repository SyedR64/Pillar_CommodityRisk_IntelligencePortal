#!/usr/bin/env python3
"""Build prospects.json from Pillar CSV data.

- Merges prioritized (scored) and raw CSVs
- Parses address -> city/state/country
- Geocodes via built-in US city/state centroid lookup; unresolved rows get country-level or None
- Computes illustrative hedging revenue opportunity:
    notional = total_weight_MT * spot_price_per_MT
    annual_pillar_rev_opp = notional * 0.005  (50 bps)
"""
import csv, json, re, hashlib, time, urllib.parse, urllib.request
from pathlib import Path

BASE = Path(__file__).parent
SRC = BASE.parent / "Pillardataset"
OUT = BASE / "data" / "prospects.json"
GEOCACHE = BASE / "data" / "geocache.json"

# Nominatim etiquette: <=1 req/s, real UA with contact, cache results
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_UA = "PillarGTMPortal/1.0 (hrahman1207@gmail.com)"
NOMINATIM_DELAY = 1.1  # seconds between requests

# ── Commodity spot prices (USD / metric ton), April 2026 assumptions ──
SPOT = {
    "Aluminum":         2500,
    "Aluminum sheet":   2700,
    "Copper":          12630,
    "Lead":             2000,
    "Lead acid battery":2000,
    "Lithium":         15000,
    "Titanium":        10000,
    "Tin":             35000,
    "Zinc":             2800,
    "Magnesium":        2500,
    "Molybdenum":      55000,
    "Steel coil":        800,
    "Steel scrap":       350,
    "Scrap":             350,
    "Scrap metal":       350,
    "HMS":               350,
    "Iron":              150,
}
DEFAULT_SPOT = 1000
BPS = 0.005  # 50 bps

# ── City/state centroids (lat, lon). Expanded list of the most common US cities. ──
US_STATES = {
 "AL":(32.806671,-86.791130),"AK":(61.370716,-152.404419),"AZ":(33.729759,-111.431221),
 "AR":(34.969704,-92.373123),"CA":(36.116203,-119.681564),"CO":(39.059811,-105.311104),
 "CT":(41.597782,-72.755371),"DE":(39.318523,-75.507141),"FL":(27.766279,-81.686783),
 "GA":(33.040619,-83.643074),"HI":(21.094318,-157.498337),"ID":(44.240459,-114.478828),
 "IL":(40.349457,-88.986137),"IN":(39.849426,-86.258278),"IA":(42.011539,-93.210526),
 "KS":(38.526600,-96.726486),"KY":(37.668140,-84.670067),"LA":(31.169546,-91.867805),
 "ME":(44.693947,-69.381927),"MD":(39.063946,-76.802101),"MA":(42.230171,-71.530106),
 "MI":(43.326618,-84.536095),"MN":(45.694454,-93.900192),"MS":(32.741646,-89.678696),
 "MO":(38.456085,-92.288368),"MT":(46.921925,-110.454353),"NE":(41.125370,-98.268082),
 "NV":(38.313515,-117.055374),"NH":(43.452492,-71.563896),"NJ":(40.298904,-74.521011),
 "NM":(34.840515,-106.248482),"NY":(42.165726,-74.948051),"NC":(35.630066,-79.806419),
 "ND":(47.528912,-99.784012),"OH":(40.388783,-82.764915),"OK":(35.565342,-96.928917),
 "OR":(44.572021,-122.070938),"PA":(40.590752,-77.209755),"RI":(41.680893,-71.511780),
 "SC":(33.856892,-80.945007),"SD":(44.299782,-99.438828),"TN":(35.747845,-86.692345),
 "TX":(31.054487,-97.563461),"UT":(40.150032,-111.862434),"VT":(44.045876,-72.710686),
 "VA":(37.769337,-78.169968),"WA":(47.400902,-121.490494),"WV":(38.491226,-80.954570),
 "WI":(44.268543,-89.616508),"WY":(42.755966,-107.302490),"DC":(38.907192,-77.036873),
 "PR":(18.220833,-66.590149),
}
US_CITIES = {
 ("NEW YORK","NY"):(40.7128,-74.0060),("BROOKLYN","NY"):(40.6782,-73.9442),
 ("HOUSTON","TX"):(29.7604,-95.3698),("DALLAS","TX"):(32.7767,-96.7970),
 ("LOS ANGELES","CA"):(34.0522,-118.2437),("SAN FRANCISCO","CA"):(37.7749,-122.4194),
 ("SAN JOSE","CA"):(37.3382,-121.8863),("FREMONT","CA"):(37.5485,-121.9886),
 ("MIAMI","FL"):(25.7617,-80.1918),("CHICAGO","IL"):(41.8781,-87.6298),
 ("ATLANTA","GA"):(33.7490,-84.3880),("SEATTLE","WA"):(47.6062,-122.3321),
 ("BOSTON","MA"):(42.3601,-71.0589),("DETROIT","MI"):(42.3314,-83.0458),
 ("PHOENIX","AZ"):(33.4484,-112.0740),("LAS VEGAS","NV"):(36.1699,-115.1398),
 ("RENO","NV"):(39.5296,-119.8138),("DENVER","CO"):(39.7392,-104.9903),
 ("PORTLAND","OR"):(45.5152,-122.6784),("MINNEAPOLIS","MN"):(44.9778,-93.2650),
 ("CITY OF INDUSTRY","CA"):(34.0194,-117.9589),("RANCHO DOMINGUEZ","CA"):(33.8922,-118.2331),
 ("LONG BEACH","CA"):(33.7701,-118.1937),("ONTARIO","CA"):(34.0633,-117.6509),
 ("IRVINE","CA"):(33.6846,-117.8265),("TORRANCE","CA"):(33.8358,-118.3406),
 ("CERRITOS","CA"):(33.8583,-118.0647),("SAN GABRIEL","CA"):(34.0961,-118.1058),
 ("MONTEREY PARK","CA"):(34.0625,-118.1228),("LAKE FOREST","CA"):(33.6469,-117.6890),
 ("BURLINGAME","CA"):(37.5841,-122.3661),("PLACENTIA","CA"):(33.8722,-117.8703),
 ("RANCHO CUCAMONGA","CA"):(34.1064,-117.5931),("BLOOMINGTON","CA"):(34.0711,-117.3961),
 ("LAKEWOOD","NJ"):(40.0979,-74.2176),("PARSIPPANY","NJ"):(40.8579,-74.4293),
 ("SECAUCUS","NJ"):(40.7895,-74.0565),("ELIZABETH","NJ"):(40.6640,-74.2107),
 ("IRVINGTON","NY"):(41.0390,-73.8687),("ROSEDALE","NY"):(40.6598,-73.7437),
 ("FLUSHING","NY"):(40.7675,-73.8331),("ELK GROVE VILLAGE","IL"):(42.0039,-87.9703),
 ("FRANKLIN PARK","IL"):(41.9350,-87.8634),("ITASCA","IL"):(41.9753,-88.0073),
 ("NAPERVILLE","IL"):(41.7508,-88.1535),("DULUTH","GA"):(34.0029,-84.1446),
 ("LOUISVILLE","KY"):(38.2527,-85.7585),("OWENSBORO","KY"):(37.7742,-87.1133),
 ("COLUMBUS","IN"):(39.2014,-85.9214),("MEMPHIS","TN"):(35.1495,-90.0490),
 ("READING","PA"):(40.3356,-75.9269),("WILMER","TX"):(32.5907,-96.6811),
 ("MOUNT MORRIS","IL"):(42.0492,-89.4376),("BESSEMER CITY","NC"):(35.2846,-81.2845),
 ("HOLLAND","MI"):(42.7875,-86.1089),("SPARTANBURG","SC"):(34.9496,-81.9321),
 ("MORENO VALLEY","CA"):(33.9425,-117.2297),("ALTAMONTE SPRINGS","FL"):(28.6611,-81.3656),
 ("ORLANDO","FL"):(28.5383,-81.3792),("CHARLOTTE","NC"):(35.2271,-80.8431),
 ("WILMINGTON","DE"):(39.7391,-75.5398),("ANN ARBOR","MI"):(42.2808,-83.7430),
 ("TACOMA","WA"):(47.2529,-122.4443),("BELLEVUE","WA"):(47.6101,-122.2015),
 ("ANDERSON","SC"):(34.5034,-82.6501),("HENDERSON","NV"):(36.0395,-114.9817),
 ("AUSTIN","TX"):(30.2672,-97.7431),("DELAVAN","WI"):(42.6283,-88.6331),
 ("HARTLAND","WI"):(43.1050,-88.3426),("CIDRA","PR"):(18.1761,-66.1614),
 ("CATANO","PR"):(18.4411,-66.1167),("STAMFORD","CT"):(41.0534,-73.5387),
 ("WESTLAKE VILLAGE","CA"):(34.1459,-118.8057),("NEW HYDE PARK","NY"):(40.7351,-73.6879),
 ("WAKEFIELD","MA"):(42.5065,-71.0770),("CINCINNATI","OH"):(39.1031,-84.5120),
 ("MT JULIET","TN"):(36.2006,-86.5186),("COLUMBUS","OH"):(39.9612,-82.9988),
 ("BELLEVUE","OH"):(41.2734,-82.8407),("SAINT LOUIS","MO"):(38.6270,-90.1994),
 ("SULPHUR SPRINGS","TX"):(33.1384,-95.6010),("ALTAMONTE SPRINGS","FL"):(28.6611,-81.3656),
 ("JESSUP","MD"):(39.1487,-76.7769),("NEW YORK CITY","NY"):(40.7128,-74.0060),
 ("PINE CREST","FL"):(25.6767,-80.3089),("FLORIDA CITY","FL"):(25.4480,-80.4793),
 ("PARSIPPANY-TROY HILLS","NJ"):(40.8579,-74.4293),("BROOMFIELD","CO"):(39.9205,-105.0866),
 ("COHOES","NY"):(42.7734,-73.7079),("EAGAN","MN"):(44.8041,-93.1668),
 ("LAKE SUCCESS","NY"):(40.7706,-73.7115),("DUMBO","NY"):(40.7033,-73.9881),
 ("POMPANO BEACH","FL"):(26.2379,-80.1248),("WESTAMPTON","NJ"):(39.9874,-74.8121),
}
# International country centroids
COUNTRIES = {
 "CANADA":(56.1304,-106.3468),"MEXICO":(23.6345,-102.5528),"BRAZIL":(-14.2350,-51.9253),
 "BRASIL":(-14.2350,-51.9253),"TAIWAN":(23.6978,120.9605),"JAPAN":(36.2048,138.2529),
 "KOREA":(35.9078,127.7669),"CHINA":(35.8617,104.1954),"VIETNAM":(14.0583,108.2772),
 "VIET NAM":(14.0583,108.2772),"SPAIN":(40.4637,-3.7492),"UNITED KINGDOM":(55.3781,-3.4360),
 "NETHERLANDS":(52.1326,5.2913),"GERMANY":(51.1657,10.4515),"SWITZERLAND":(46.8182,8.2275),
 "AUSTRALIA":(-25.2744,133.7751),"SINGAPORE":(1.3521,103.8198),"INDIA":(20.5937,78.9629),
 "ITALY":(41.8719,12.5674),"FRANCE":(46.2276,2.2137),"ISRAEL":(31.0461,34.8516),
 "DMCC":(25.0657,55.1713),"UAE":(23.4241,53.8478),
}

STATE_RE = re.compile(r'\b([A-Z]{2})\b\s*\d{5}')
STATE_ONLY_RE = re.compile(r',\s*([A-Z]{2})[\s,.]')
CITY_RE = re.compile(r'([A-Z][A-Z .\-]{2,})\s*,?\s*([A-Z]{2})[\s,.]*\d{0,5}')
ZIP_RE = re.compile(r'\b(\d{5})(?:-\d{4})?\b')
WEIGHT_RE = re.compile(r'([\d.]+)\s*([KMB]?)', re.I)

def parse_weight(s):
    """'266M' -> 266_000_000 (assume kg). Returns metric tons."""
    if not s: return 0
    s = s.strip().replace(',','')
    m = WEIGHT_RE.match(s)
    if not m: return 0
    try:
        n = float(m.group(1))
    except ValueError:
        return 0
    mult = {'':1,'K':1e3,'M':1e6,'B':1e9}.get(m.group(2).upper(),1)
    kg = n * mult
    return kg / 1000.0  # MT

def parse_float(s):
    if not s: return 0
    s = str(s).replace('%','').strip()
    try: return float(s)
    except ValueError: return 0

def parse_int(s):
    if not s: return 0
    s = str(s).replace(',','').strip()
    try: return int(float(s))
    except ValueError: return 0

def geo_lookup(addr):
    """Return (lat, lon, city, state, country, resolved_level)."""
    if not addr:
        return None, None, "", "", "US", "none"
    A = addr.upper()
    # Country guess
    country = "US"
    for c in COUNTRIES:
        if c in A:
            country = c
            break
    # US city/state match
    if country == "US":
        # Try explicit city, STATE ZIP
        for (city, st), (lat, lon) in US_CITIES.items():
            if city in A and (f" {st}" in A or f",{st}" in A or f", {st}" in A):
                return lat, lon, city.title(), st, "US", "city"
        # Try state only
        m = STATE_RE.search(A) or STATE_ONLY_RE.search(A)
        if m:
            st = m.group(1)
            if st in US_STATES:
                lat, lon = US_STATES[st]
                return lat, lon, "", st, "US", "state"
        # Try "XX 12345" pattern without comma
        m = re.search(r'\b([A-Z]{2})\s+\d{5}', A)
        if m and m.group(1) in US_STATES:
            st = m.group(1)
            lat, lon = US_STATES[st]
            return lat, lon, "", st, "US", "state"
    else:
        lat, lon = COUNTRIES[country]
        return lat, lon, "", "", country, "country"
    return None, None, "", "", country, "none"

def clean_address_for_geocoding(addr):
    """Return a list of candidate query strings (tried in order, first hit wins)."""
    if not addr: return []
    s = addr.strip()
    # Drop obvious noise
    s = re.sub(r'\b(TEL|FAX|PH|PHONE|EMAIL|E[-\s]?MAIL|ATTN|CONTACT|VAT|EIN|TAX\s*ID|SSN|SCAC|FMC|OTI|MRN|CEP)[:#\s]*\S*', ' ', s, flags=re.I)
    s = re.sub(r'\+?\d[\d\-\(\)\s]{7,}', ' ', s)
    s = re.sub(r'[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}', ' ', s, flags=re.I)
    s = re.sub(r'https?://\S+', ' ', s)
    # Repair common line-break word splits: "CIT Y" -> "CITY", "WES TLAKE" -> "WESTLAKE", etc.
    # Strategy: if a single uppercase letter is surrounded by an uppercase token and another uppercase token starting with an uppercase letter, glue them.
    s = re.sub(r'\b([A-Z]{2,})\s+([A-Z])\b(?=\s)', r'\1\2', s)  # "CIT Y " -> "CITY "
    s = re.sub(r'\b([A-Z]{2,})\s+([A-Z]{2,})\b', lambda m: m.group(1)+m.group(2) if len(m.group(1))<=4 and len(m.group(2))<=5 else m.group(0), s)
    # Dedup repeated state code like "CA CA"
    s = re.sub(r'\b([A-Z]{2})\s+\1\b', r'\1', s)
    s = re.sub(r'\s+', ' ', s).strip(' ,.')
    if not s: return []
    candidates = [s[:180]]

    upper = s.upper()
    # ZIP + state fallback
    m = re.search(r'\b([A-Z]{2})\s*(\d{5})(?:-\d{4})?\b', upper)
    if m and m.group(1) in US_STATES:
        candidates.append(f"{m.group(2)}, {m.group(1)}, USA")
    else:
        m2 = re.search(r'\b(\d{5})(?:-\d{4})?\b', upper)
        if m2:
            candidates.append(f"{m2.group(1)}, USA")
    # City, ST structured fallback (uses US_CITIES knowledge)
    for (city, st) in US_CITIES:
        if city in upper:
            candidates.append(f"{city}, {st}, USA")
            break
    # Deduplicate while preserving order
    seen = set(); out = []
    for c in candidates:
        if c not in seen: seen.add(c); out.append(c)
    return out

_last_nominatim_call = [0.0]
def nominatim_geocode(query):
    """Query OSM Nominatim; returns (lat, lon, display_name) or None."""
    if not query: return None
    # Rate limit
    now = time.time()
    wait = NOMINATIM_DELAY - (now - _last_nominatim_call[0])
    if wait > 0: time.sleep(wait)
    _last_nominatim_call[0] = time.time()
    url = NOMINATIM_URL + "?" + urllib.parse.urlencode({
        "q": query, "format": "json", "limit": 1, "addressdetails": 1,
    })
    req = urllib.request.Request(url, headers={"User-Agent": NOMINATIM_UA})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode())
    except Exception as e:
        print(f"  ! nominatim error for {query[:60]!r}: {e}")
        return None
    if not data: return None
    hit = data[0]
    addr = hit.get("address", {})
    return {
        "lat": float(hit["lat"]), "lon": float(hit["lon"]),
        "display": hit.get("display_name", ""),
        "city": addr.get("city") or addr.get("town") or addr.get("village") or addr.get("hamlet") or "",
        "state": addr.get("state", ""),
        "country": addr.get("country_code", "").upper() or addr.get("country", ""),
    }

def load_geocache():
    if GEOCACHE.exists():
        try: return json.loads(GEOCACHE.read_text())
        except Exception: pass
    return {}

def save_geocache(cache):
    GEOCACHE.write_text(json.dumps(cache, indent=1))

def jitter(lat, lon, seed):
    """Small deterministic jitter so co-located pins spread visually."""
    h = int(hashlib.md5(seed.encode()).hexdigest()[:8], 16)
    dy = ((h & 0xFFFF) / 0xFFFF - 0.5) * 0.25
    dx = (((h >> 16) & 0xFFFF) / 0xFFFF - 0.5) * 0.25
    return lat + dy, lon + dx

def est_revenue(commodity, mt):
    spot = SPOT.get(commodity, DEFAULT_SPOT)
    notional = mt * spot
    return notional, notional * BPS

def load_csv(path):
    with open(path, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def resolve_geo(addr, cache, stats):
    """Try built-in lookup first, fall back to Nominatim with caching."""
    lat, lon, city, state, country, level = geo_lookup(addr)
    if level in ("city", "country"):
        stats['builtin'] += 1
        return lat, lon, city, state, country, level
    # State-level is coarse; try Nominatim to upgrade to city-precision
    candidates = clean_address_for_geocoding(addr)
    for q in candidates:
        if q in cache:
            hit = cache[q]
            if hit:
                stats['cache'] += 1
                return hit['lat'], hit['lon'], hit.get('city',''), hit.get('state',''), hit.get('country','US'), 'nominatim'
            continue  # cached miss → try next candidate
        hit = nominatim_geocode(q)
        cache[q] = hit
        if hit:
            stats['nominatim'] += 1
            return hit['lat'], hit['lon'], hit.get('city',''), hit.get('state',''), hit.get('country','US'), 'nominatim'
    if level == "state": stats['builtin'] += 1
    else: stats['none'] += 1
    return lat, lon, city, state, country, level

def main():
    prio = load_csv(SRC / "pillar_prospects_prioritized.csv")
    raw  = load_csv(SRC / "pillar_prospects_raw.csv")

    cache = load_geocache()
    stats = {'builtin': 0, 'cache': 0, 'nominatim': 0, 'none': 0}
    print(f"Loaded geocache with {len(cache)} entries. Geocoding may take a while on first run…")

    out = []
    seen = set()
    # Emit prioritized first (scored rows)
    for r in prio:
        name = (r.get('Buyer Name') or '').strip()
        if not name or name in seen: continue
        seen.add(name)
        commodity = (r.get('Primary Commodity') or '').strip()
        mt = parse_weight(r.get('Total Weight'))
        notional, rev = est_revenue(commodity, mt)
        lat, lon, city, state, country, level = resolve_geo(r.get('Address',''), cache, stats)
        if lat is not None:
            lat, lon = jitter(lat, lon, name)
        also = (r.get('Also Imports') or '').strip()
        also_list = sorted({x.strip() for x in also.split(';') if x.strip() and x.strip() != commodity})
        out.append({
            "name": name,
            "address": (r.get('Address') or '').strip(),
            "phone": (r.get('Phone') or '').strip(),
            "email": (r.get('Email') or '').strip(),
            "score": parse_int(r.get('Score')),
            "commodity": commodity,
            "alsoImports": also_list,
            "category": (r.get('Category') or '').strip(),
            "shipments": parse_int(r.get('Total Shipments')),
            "weightMT": round(mt, 1),
            "marketSharePct": parse_float(r.get('Market Share %')),
            "teu": parse_int(r.get('Volume TEU')),
            "volumeSharePct": parse_float(r.get('Volume Share %')),
            "dataType": (r.get('Data Type') or '').strip(),
            "notionalUSD": round(notional, 0),
            "revOppUSD": round(rev, 0),
            "lat": lat, "lon": lon,
            "city": city, "state": state, "country": country,
            "geoLevel": level,
            "tier": "scored",
        })
    # Emit raw-only rows (unscored, larger universe)
    for r in raw:
        name = (r.get('Buyer Name') or '').strip()
        if not name or name in seen: continue
        seen.add(name)
        commodity = (r.get('Primary Commodity') or '').strip()
        mt = parse_weight(r.get('Total Weight'))
        notional, rev = est_revenue(commodity, mt)
        lat, lon, city, state, country, level = resolve_geo(r.get('Address',''), cache, stats)
        if lat is not None:
            lat, lon = jitter(lat, lon, name)
        out.append({
            "name": name,
            "address": (r.get('Address') or '').strip(),
            "phone": (r.get('Phone') or '').strip(),
            "email": (r.get('Email') or '').strip(),
            "score": parse_int(r.get('Score')),
            "commodity": commodity,
            "alsoImports": [],
            "category": (r.get('Category') or '').strip(),
            "shipments": parse_int(r.get('Total Shipments')),
            "weightMT": round(mt, 1),
            "marketSharePct": parse_float(r.get('Market Share %')),
            "teu": parse_int(r.get('Volume TEU')),
            "volumeSharePct": parse_float(r.get('Volume Share %')),
            "dataType": (r.get('Data Type') or '').strip(),
            "notionalUSD": round(notional, 0),
            "revOppUSD": round(rev, 0),
            "lat": lat, "lon": lon,
            "city": city, "state": state, "country": country,
            "geoLevel": level,
            "tier": "raw",
        })

    totals = {
        "prospects": len(out),
        "scored": sum(1 for x in out if x['tier'] == 'scored'),
        "totalShipments": sum(x['shipments'] for x in out),
        "totalTEU": sum(x['teu'] for x in out),
        "totalWeightMT": round(sum(x['weightMT'] for x in out), 0),
        "totalNotionalUSD": round(sum(x['notionalUSD'] for x in out), 0),
        "totalRevOppUSD": round(sum(x['revOppUSD'] for x in out), 0),
        "geocodedPct": round(100 * sum(1 for x in out if x['lat'] is not None) / len(out), 1),
        "commodities": sorted({x['commodity'] for x in out if x['commodity']}),
    }

    payload = {
        "generatedAt": "2026-04-17",
        "assumptions": {
            "spotPricesUSDperMT": SPOT,
            "defaultSpotUSDperMT": DEFAULT_SPOT,
            "feeBps": BPS * 10000,
            "notes": "Revenue opp = weight (MT) × commodity spot × 50 bps. Illustrative agency execution fee on hedged notional.",
        },
        "totals": totals,
        "prospects": out,
    }
    OUT.write_text(json.dumps(payload, indent=1))
    # Also emit as JS for file:// loading (no CORS)
    js_path = OUT.with_suffix('.js')
    js_path.write_text("window.PILLAR_DATA = " + json.dumps(payload) + ";")
    save_geocache(cache)
    print(f"Wrote {OUT} — {totals['prospects']} prospects, {totals['geocodedPct']}% geocoded")
    print(f"Geocoding sources — builtin: {stats['builtin']} · cache: {stats['cache']} · nominatim: {stats['nominatim']} · unresolved: {stats['none']}")
    print(f"Geocache: {len(cache)} entries written to {GEOCACHE}")
    print(f"Total notional: ${totals['totalNotionalUSD']/1e9:.1f}B | Rev opp: ${totals['totalRevOppUSD']/1e6:.1f}M")

if __name__ == "__main__":
    main()
