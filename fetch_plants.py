#!/usr/bin/env python3
"""
Fetch all plants from Attio and prepare data for the territory map.
Fast version - uses city coordinate fallback only.
"""

import json
import urllib.request
import os

ATTIO_KEY = None

def load_attio_key():
    global ATTIO_KEY
    try:
        with open(os.path.expanduser("~/.config/attio/api_key")) as f:
            ATTIO_KEY = f.read().strip()
            return True
    except:
        return False

def get_all_plants():
    """Fetch all plants from Attio."""
    url = "https://api.attio.com/v2/objects/plants/records/query"
    payload = json.dumps({"limit": 500}).encode()
    
    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Authorization", f"Bearer {ATTIO_KEY}")
    req.add_header("Content-Type", "application/json")
    
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.loads(response.read().decode()).get("data", [])
    except Exception as e:
        print(f"Error: {e}")
        return []

def get_pipeline_entries():
    """Fetch Alabama Pipeline entries."""
    url = "https://api.attio.com/v2/lists/fc93ded9-7b9e-4e48-99d5-583cf8dd85d6/entries/query"
    payload = json.dumps({"limit": 500}).encode()
    
    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Authorization", f"Bearer {ATTIO_KEY}")
    req.add_header("Content-Type", "application/json")
    
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode())
            entries = {}
            for entry in data.get("data", []):
                parent_id = entry.get("parent_record_id")
                if parent_id:
                    stage = entry.get("entry_values", {}).get("stage", [{}])
                    if stage and stage[0].get("status"):
                        entries[parent_id] = stage[0]["status"].get("title", "Unknown")
            return entries
    except:
        return {}

def extract_value(values, key="value"):
    if not values:
        return ""
    val = values[0]
    if isinstance(val, dict):
        for k in ["value", "full_name", "email_address", "phone_number"]:
            if k in val:
                return str(val[k])
        if "option" in val and "title" in val["option"]:
            return val["option"]["title"]
        if "target_record_id" in val:
            return val["target_record_id"]
    return str(val) if val else ""

# City coordinates (major AL cities + Jacobs pilots)
CITY_COORDS = {
    "birmingham": (33.5207, -86.8025), "montgomery": (32.3792, -86.3077),
    "huntsville": (34.7304, -86.5861), "mobile": (30.6954, -88.0399),
    "tuscaloosa": (33.2098, -87.5692), "hoover": (33.4054, -86.8114),
    "dothan": (31.2232, -85.3905), "auburn": (32.6099, -85.4808),
    "decatur": (34.6059, -86.9833), "madison": (34.6993, -86.7483),
    "florence": (34.7998, -87.6772), "gadsden": (34.0143, -86.0066),
    "vestavia hills": (33.4487, -86.7878), "prattville": (32.4640, -86.4597),
    "phenix city": (32.4709, -85.0008), "alabaster": (33.2443, -86.8166),
    "bessemer": (33.4018, -86.9544), "enterprise": (31.3152, -85.8552),
    "opelika": (32.6454, -85.3783), "northport": (33.2290, -87.5772),
    "anniston": (33.6598, -85.8316), "mccalla": (33.2612, -87.0286),
    "fairhope": (30.5230, -87.9033), "daphne": (30.6035, -87.9036),
    "selma": (32.4074, -87.0211), "troy": (31.8088, -85.9700),
    "pelham": (33.2857, -86.8094), "oxford": (33.6140, -85.8347),
    "trussville": (33.6198, -86.6089), "alexander city": (32.9440, -85.9539),
    "cullman": (34.1748, -86.8436), "scottsboro": (34.6723, -86.0341),
    "millbrook": (32.4799, -86.3619), "athens": (34.8025, -86.9717),
    "albertville": (34.2673, -86.2089), "talladega": (33.4359, -86.1058),
    "homewood": (33.4712, -86.8008), "jasper": (33.8312, -87.2775),
    "ozark": (31.4590, -85.6405), "wetumpka": (32.5440, -86.2119),
    "foley": (30.4066, -87.6836), "gulf shores": (30.2460, -87.7008),
    "spanish fort": (30.6749, -87.9153), "rainbow city": (33.9548, -86.0419),
    "sylacauga": (33.1732, -86.2516), "pell city": (33.5862, -86.2861),
    "hartselle": (34.4434, -86.9353), "eufaula": (31.8913, -85.1455),
    "saraland": (30.8207, -88.0706), "fultondale": (33.6048, -86.7939),
    "gardendale": (33.6601, -86.8128), "center point": (33.6445, -86.6853),
    "irondale": (33.5382, -86.7072), "clay": (33.7023, -86.6014),
    "moody": (33.5909, -86.5094), "leeds": (33.5482, -86.5444),
    "helena": (33.2962, -86.8436), "calera": (33.1029, -86.7536),
    "montevallo": (33.1007, -86.8644), "clanton": (32.8388, -86.6294),
    # Jacobs pilots
    "carol stream": (41.9125, -88.1348), "lehigh": (40.5834, -75.5710),
}

def get_coords(city):
    if city:
        return CITY_COORDS.get(city.lower().strip())
    return None

def get_person_batch(person_ids):
    """Get multiple people at once."""
    people = {}
    for pid in person_ids[:20]:  # Limit to avoid too many requests
        url = f"https://api.attio.com/v2/objects/people/records/{pid}"
        req = urllib.request.Request(url)
        req.add_header("Authorization", f"Bearer {ATTIO_KEY}")
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode()).get("data", {})
                values = data.get("values", {})
                people[pid] = {
                    "name": extract_value(values.get("name", [])),
                    "email": extract_value(values.get("email_addresses", [])),
                    "phone": extract_value(values.get("phone_numbers", []))
                }
        except:
            pass
    return people

def main():
    print("🗺️  Territory Map Data Fetcher (Fast)")
    print("=" * 50)
    
    if not load_attio_key():
        print("❌ No Attio API key")
        return
    
    print("📡 Fetching plants...")
    plants = get_all_plants()
    print(f"   Found {len(plants)} plants")
    
    print("📊 Fetching pipeline...")
    pipeline = get_pipeline_entries()
    print(f"   Found {len(pipeline)} entries")
    
    # Collect person IDs
    person_ids = set()
    for p in plants:
        pid = extract_value(p.get("values", {}).get("main_contact", []))
        if pid:
            person_ids.add(pid)
    
    print(f"👤 Fetching {len(person_ids)} contacts...")
    people = get_person_batch(list(person_ids))
    
    print("🌍 Processing...")
    map_data = []
    
    for plant in plants:
        values = plant.get("values", {})
        record_id = plant.get("id", {}).get("record_id", "")
        
        name = extract_value(values.get("name", []))
        if not name:
            continue
        
        city = extract_value(values.get("city", []))
        coords = get_coords(city)
        
        if not coords:
            continue  # Skip plants without coordinates
        
        contact_id = extract_value(values.get("main_contact", []))
        contact = people.get(contact_id, {})
        
        map_data.append({
            "id": record_id,
            "name": name,
            "address": extract_value(values.get("address_7", [])),
            "city": city,
            "state": extract_value(values.get("state", [])),
            "zip": extract_value(values.get("zip", [])),
            "lat": coords[0],
            "lon": coords[1],
            "permit": extract_value(values.get("permit", [])),
            "permit_status": extract_value(values.get("permit_status", [])),
            "permit_type": extract_value(values.get("permit_type", [])),
            "stage": pipeline.get(record_id, "Not in Pipeline"),
            "contact_name": contact.get("name", ""),
            "contact_email": contact.get("email", ""),
            "contact_phone": contact.get("phone", ""),
        })
    
    with open("plants.json", "w") as f:
        json.dump(map_data, f, indent=2)
    
    print(f"\n✅ Saved {len(map_data)} plants to plants.json")
    
    # Summary
    stages = {}
    for p in map_data:
        s = p["stage"]
        stages[s] = stages.get(s, 0) + 1
    
    print("\n📊 By Stage:")
    for stage, count in sorted(stages.items(), key=lambda x: -x[1]):
        print(f"   {stage}: {count}")

if __name__ == "__main__":
    main()
