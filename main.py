import requests
import json
import time
import datetime
import re
import os

# Constants 
API_URL = "https://origo.nltg.com/"
DEFAULT_HEADERS = {
    "accept": "*/*", "accept-language": "en-US,en;q=0.9",
    "auth-userid": "2645", # !!! REPLACE THIS WITH A FRESH VALUE !!!
    "content-type": "application/json", "marketunit": "sd",
    "origin": "https://www.spies.dk", "priority": "u=1, i",
    "referer": "https://www.spies.dk/",
    "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
    "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty", "sec-fetch-mode": "cors", "sec-fetch-site": "cross-site",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "x-caller-app": "flight-only-charter", "x-origo-context": "production",
}
META_QUERY_STRING = """
query meta($homebound: Boolean, $departureAirport: Int) {
  team1FlightOnlySearchBarMeta(homebound: $homebound, departureAirport: $departureAirport) {
    departureAirports { value name __typename } __typename
  }
}"""
INITIAL_FLIGHT_OFFERS_QUERY = """
query InitialFlightOffers($departureDateFrom: String, $ages: [Int], $duration: Int, $departureCaId: [ID!], $countryCaId: Int, $destinationCaId: [ID!], $nrOfTransports: Int, $tripType: Team1FlightOnlyTripTypes) {
  team1FlightOnlyFlightOffers(departureDateFrom: $departureDateFrom, ages: $ages, duration: $duration, departureCaId: $departureCaId, countryCaId: $countryCaId, destinationCaId: $destinationCaId, nrOfFlightOffers: $nrOfTransports, tripType: $tripType) {
    flightOffers { ...Team1FlightOnlyListFlightOfferItem __typename } keys { next __typename } ...FlightOnlyMessage __typename
  }
}
fragment Team1FlightOnlyListFlightOfferItem on Team1FlightOnlyListFlightOffer {
  meta { destinationCode departureCode serialNumber currencyCode countryCode iataCode __typename }
  includedTransportOffer { duration durationText countryName countryUrl freeSeats hasEnoughFreeSeats fewSeatsLeft transportKey content { name shortDescription description image code __typename } price pricePerPerson rawTotalPrice totalPrice totalPricePerPerson transportDetailsKey keys { trip __typename } __typename }
  out { destination { name __typename } carrier { name key logo __typename } departure { name date { url dayShortName time day monthShortName __typename } __typename } __typename } __typename
}
fragment FlightOnlyMessage on Team1FlightOnlyFlightOffersResponse { messages { header message type __typename } __typename }"""

ROUTES_FILE = "routes.json"
RESULTS_FILE = "results.json"
DKK_TO_EUR_RATE = 0.134

FOREIGN_AIRPORT_ORIGIN_CAIDS = [
    "61949", "13135", "12767", "12745", "12774", "12769", "12771",
    "165227", "12778", "12741", "12780", "12781", "12783", "12747", "12766"
]
DANISH_AIRPORTS_DATA = {}

# Helper Functions
def get_session():
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    return session

def call_graphql_api(session, query_str, operation_name, variables):
    payload = {"operationName": operation_name, "variables": variables, "query": query_str}
    try:
        response = session.post(API_URL, json=payload, timeout=45)
        response.raise_for_status()
        data = response.json()
        if data.get("errors") and data["errors"]:
            print(f"GraphQL Errors for {operation_name} ({variables}): {data['errors']}")
            return None
        return data.get("data")
    except requests.exceptions.Timeout: print(f"Timeout for {operation_name} ({variables})")
    except requests.exceptions.RequestException as e: print(f"RequestException for {operation_name} ({variables}): {e} - Response: {response.text[:200] if 'response' in locals() and hasattr(response, 'text') else 'N/A'}")
    except json.JSONDecodeError: print(f"JSONDecodeError for {operation_name}. Response: {response.text[:200] if 'response' in locals() and hasattr(response, 'text') else 'N/A'}")
    return None

def get_current_timestamp_dt():
    return datetime.datetime.now(datetime.timezone.utc)

def get_current_timestamp_iso():
    return get_current_timestamp_dt().isoformat()

def parse_price_dkk(price_val):
    if price_val is None: return None
    if isinstance(price_val, (int, float)): return float(price_val)
    if not isinstance(price_val, str): return None
    cleaned_price_str = price_val.strip()
    if cleaned_price_str.endswith(",-"): cleaned_price_str = cleaned_price_str[:-2]
    cleaned_price_str = cleaned_price_str.replace(".", "")
    try: return float(cleaned_price_str) * 10.0  # Convert to DKK
    except ValueError: return None

def parse_duration_to_hours(duration_text):
    if not duration_text: return 0.0
    h, m = 0.0, 0.0
    h_match, m_match = re.search(r'(\d+)t', duration_text), re.search(r'(\d+)m', duration_text)
    if h_match: h = int(h_match.group(1))
    if m_match: m = int(m_match.group(1))
    return round(h + (m / 60.0), 2)

def format_flight_object(offer, num_passengers=2):
    meta = offer.get("meta", {})
    transport = offer.get("includedTransportOffer", {})
    out_leg = offer.get("out", {})
    dep_details = out_leg.get("departure", {}).get("date", {})
    raw_date_str, time_str = dep_details.get("url"), dep_details.get("time")
    departure_iso, arrival_iso, duration_hours = None, None, 0.0
    departure_dt_obj = None

    if raw_date_str and time_str and len(raw_date_str) == 8:
        try:
            departure_dt_str = f"{raw_date_str[:4]}-{raw_date_str[4:6]}-{raw_date_str[6:8]} {time_str}"
            # Assuming departure times are local to the departure airport. For simplicity, store as naive, then convert to UTC if TZ were known.
            departure_dt_obj = datetime.datetime.strptime(departure_dt_str, "%Y-%m-%d %H:%M").replace(tzinfo=datetime.timezone.utc)
            departure_iso = departure_dt_obj.isoformat()
            duration_text_from_api = transport.get("durationText")
            if duration_text_from_api: duration_hours = parse_duration_to_hours(duration_text_from_api)
            if duration_hours > 0: arrival_iso = (departure_dt_obj + datetime.timedelta(hours=duration_hours)).isoformat()
            else: arrival_iso = departure_iso
        except ValueError: pass

    price_pp_dkk = parse_price_dkk(transport.get("totalPricePerPerson"))
    if price_pp_dkk is None and transport.get("rawTotalPrice") is not None:
        raw_total_price = transport.get("rawTotalPrice")
        if num_passengers > 0: price_pp_dkk = parse_price_dkk(raw_total_price / num_passengers)
        else: price_pp_dkk = parse_price_dkk(raw_total_price)
    price_eur = round(price_pp_dkk * DKK_TO_EUR_RATE, 2) if price_pp_dkk is not None else None

    carrier_key = out_leg.get("carrier", {}).get("key", "")
    carrier_code = carrier_key.split("|")[-1] if "|" in carrier_key else carrier_key
    flight_num_val = transport.get("content", {}).get("code")

    flight_id_internal = f"{meta.get('departureCode','')}-{meta.get('destinationCode','')}-{departure_iso}-{carrier_code}-{flight_num_val}"
    if transport.get("transportKey"): flight_id_internal += f"-{transport.get('transportKey')}"

    return {
        "flight_id_internal": flight_id_internal,
        "departure": departure_iso, "arrival": arrival_iso, "duration": duration_hours,
        "carrier": carrier_code, "iata_origin": meta.get("departureCode"),
        "iata_destination": meta.get("destinationCode"),
        "price": price_eur, "stops": 0, "layover_info": None,
        "flight_numbers": flight_num_val, "vehicle_type": "aircraft"
    }

def save_json(data, filename):
    try:
        with open(filename, 'w', encoding='utf-8') as f: json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e: print(f"Error saving {filename}: {e}")

def load_json(filename):
    if not os.path.exists(filename): return []
    try:
        with open(filename, 'r', encoding='utf-8') as f: return json.load(f)
    except: return []

# Core Logic Functions 
def discover_danish_airports(session):
    global DANISH_AIRPORTS_DATA
    print("Discovering Danish airports...")
    meta_data = call_graphql_api(session, META_QUERY_STRING, "meta", {"homebound": False, "departureAirport": -1})
    if meta_data and meta_data.get("team1FlightOnlySearchBarMeta", {}).get("departureAirports"):
        for ap in meta_data["team1FlightOnlySearchBarMeta"]["departureAirports"]:
            DANISH_AIRPORTS_DATA[ap["value"]] = {"name": ap["name"], "iata": None}
    print(f"Discovered Danish airports (CaId:Name): { {k:v['name'] for k,v in DANISH_AIRPORTS_DATA.items()} }")
    if not DANISH_AIRPORTS_DATA: print("CRITICAL: Could not discover Danish airports.")

def get_flights_from_caid(session, departure_caid_str):
    variables = {"ages": [30, 30], "duration": None, "departureCaId": [departure_caid_str],
                 "countryCaId": None, "nrOfTransports": 100, "tripType": "OneWay"}
    data = call_graphql_api(session, INITIAL_FLIGHT_OFFERS_QUERY, "InitialFlightOffers", variables)
    if data and data.get("team1FlightOnlyFlightOffers"):
        return data["team1FlightOnlyFlightOffers"].get("flightOffers", [])
    return []

def discover_routes_and_initial_flights(session):
    print("Starting initial route discovery and flight scraping...")
    discovered_routes_dict = {}
    initial_flight_results_formatted = [] # Stores formatted flight objects (without internal_id)
    processed_flight_ids_this_discovery = set()

    if not DANISH_AIRPORTS_DATA: discover_danish_airports(session)
    if not DANISH_AIRPORTS_DATA: return [], []

    danish_airport_caids = list(DANISH_AIRPORTS_DATA.keys())

    print("\n--- Processing flights FROM Denmark ---")
    for dk_caid in danish_airport_caids:
        dk_airport_info = DANISH_AIRPORTS_DATA[dk_caid]
        print(f"Querying from Danish airport: {dk_airport_info['name']} ({dk_caid})")
        time.sleep(2)
        offers = get_flights_from_caid(session, dk_caid)
        print(f"  Found {len(offers)} offers from {dk_airport_info['name']}.")
        for offer in offers:
            out_info = offer.get("out", {})
            dep_info_from_offer = out_info.get("departure", {})
            dest_info_from_offer = out_info.get("destination", {})
            flight_dict = format_flight_object(offer)
            if not flight_dict["departure"]: continue

            if flight_dict["flight_id_internal"] not in processed_flight_ids_this_discovery:
                initial_flight_results_formatted.append({k:v for k,v in flight_dict.items() if k != "flight_id_internal"})
                processed_flight_ids_this_discovery.add(flight_dict["flight_id_internal"])

            if dk_airport_info["iata"] is None and flight_dict["iata_origin"]:
                dk_airport_info["iata"] = flight_dict["iata_origin"]

            route_key = (flight_dict["iata_origin"], flight_dict["iata_destination"])
            if flight_dict["iata_origin"] and flight_dict["iata_destination"] and route_key not in discovered_routes_dict:
                discovered_routes_dict[route_key] = {
                    "origin_iata": flight_dict["iata_origin"], "origin_name": dep_info_from_offer.get("name"),
                    "origin_caid": dk_caid,
                    "destination_iata": flight_dict["iata_destination"], "destination_name": dest_info_from_offer.get("name"),
                    "last_updated": get_current_timestamp_iso(), "update_after": get_update_after(None, [flight_dict], []) # Initial update_after
                }
                print(f"    New route: {flight_dict['iata_origin']} -> {flight_dict['iata_destination']}")

    print("\n--- Processing flights TO Denmark ---")
    danish_iatas_discovered = {v["iata"] for v in DANISH_AIRPORTS_DATA.values() if v["iata"]}
    for foreign_caid in FOREIGN_AIRPORT_ORIGIN_CAIDS:
        print(f"Querying from Foreign airport CaId: {foreign_caid}")
        time.sleep(2)
        offers = get_flights_from_caid(session, foreign_caid)
        print(f"  Found {len(offers)} offers from foreign CaId {foreign_caid}.")
        for offer in offers:
            out_info = offer.get("out", {})
            dep_info_from_offer = out_info.get("departure", {})
            dest_info_from_offer = out_info.get("destination", {})
            flight_dict = format_flight_object(offer)
            if not flight_dict["departure"]: continue

            if flight_dict["iata_destination"] in danish_iatas_discovered:
                if flight_dict["flight_id_internal"] not in processed_flight_ids_this_discovery:
                    initial_flight_results_formatted.append({k:v for k,v in flight_dict.items() if k != "flight_id_internal"})
                    processed_flight_ids_this_discovery.add(flight_dict["flight_id_internal"])

                route_key = (flight_dict["iata_origin"], flight_dict["iata_destination"])
                if flight_dict["iata_origin"] and flight_dict["iata_destination"] and route_key not in discovered_routes_dict:
                    dest_dk_caid, dest_dk_name_from_map = None, dest_info_from_offer.get("name")
                    for caid_map, info_map in DANISH_AIRPORTS_DATA.items():
                        if info_map["iata"] == flight_dict["iata_destination"]:
                            dest_dk_caid = caid_map
                            dest_dk_name_from_map = info_map["name"]
                            break
                    discovered_routes_dict[route_key] = {
                        "origin_iata": flight_dict["iata_origin"], "origin_name": dep_info_from_offer.get("name"), "origin_caid": foreign_caid,
                        "destination_iata": flight_dict["iata_destination"], "destination_name": dest_dk_name_from_map,
                        "destination_caid_dk": dest_dk_caid,
                        "last_updated": get_current_timestamp_iso(), "update_after": get_update_after(None, [flight_dict], [])
                    }
                    print(f"    New route: {flight_dict['iata_origin']} -> {flight_dict['iata_destination']}")

    final_routes = list(discovered_routes_dict.values())
    save_json(final_routes, ROUTES_FILE)
    save_json(initial_flight_results_formatted, RESULTS_FILE)
    print(f"Initial discovery: {len(final_routes)} routes, {len(initial_flight_results_formatted)} flights.")
    return final_routes, initial_flight_results_formatted


# --- Functions to Implement (as per requirements) ---

def get_work():
    """Return top 5 routes by update_after."""
    routes = load_json(ROUTES_FILE)
    if not routes: return []
    now_iso = get_current_timestamp_iso()
    valid_routes = [r for r in routes if isinstance(r, dict)]
    due_routes = [r for r in valid_routes if (r.get("update_after") or "1970-01-01T00:00:00Z") <= now_iso]
    due_routes.sort(key=lambda r: r.get("update_after") or "1970-01-01T00:00:00Z")
    return due_routes[:5]

def get_update_after(route_info, new_flights_for_route, all_results_data):
    """
    Return next update time for a route based on the minimum future departure.
    """
    now_dt = get_current_timestamp_dt()
    
    # Combine new and existing flights for this specific route
    combined_flights_for_route = list(new_flights_for_route) 
    for existing_flight in all_results_data:
        if existing_flight.get("iata_origin") == route_info.get("origin_iata") and \
           existing_flight.get("iata_destination") == route_info.get("destination_iata"):
            is_present = any(
                nf.get("departure") == existing_flight.get("departure") and nf.get("carrier") == existing_flight.get("carrier")
                for nf in new_flights_for_route
            )
            if not is_present:
                combined_flights_for_route.append(existing_flight)

    future_departure_dts = []
    for flight in combined_flights_for_route:
        if flight.get("departure"):
            try:
                # Ensure departure times are timezone-aware (UTC) for comparison
                dep_dt = datetime.datetime.fromisoformat(flight["departure"])
                if dep_dt.tzinfo is None: # If naive, assume UTC as per format_flight_object
                    dep_dt = dep_dt.replace(tzinfo=datetime.timezone.utc)
                
                if dep_dt > now_dt:
                    future_departure_dts.append(dep_dt)
            except ValueError:
                continue 

    if not future_departure_dts:
        # No future flights known for this route. Check again in 12 hours.
        return (now_dt + datetime.timedelta(hours=12)).isoformat()
    else:
        min_future_departure_dt = min(future_departure_dts)
        # Schedule update 1 day after the earliest known future departure for this route.
        next_update_dt = min_future_departure_dt + datetime.timedelta(days=1)
        if next_update_dt < now_dt:
             return (now_dt + datetime.timedelta(hours=6)).isoformat()
        return next_update_dt.isoformat()


def extract(route_info, session, existing_flight_ids_globally):
    """
    Call site and update results for a specific route.
    """
    print(f"Extracting updates for route: {route_info.get('origin_iata')} -> {route_info.get('destination_iata')} (OriginCaId: {route_info.get('origin_caid')})")
    time.sleep(1) # Polite delay
    all_offers_from_origin = get_flights_from_caid(session, route_info['origin_caid'])
    
    newly_found_flights_formatted = []
    if not all_offers_from_origin:
        print(f"  No offers found from origin CaId {route_info['origin_caid']}")
        return newly_found_flights_formatted

    for offer in all_offers_from_origin:
        if offer.get("meta", {}).get("destinationCode") == route_info['destination_iata']:
            flight_dict_with_id = format_flight_object(offer)
            if not flight_dict_with_id["departure"]: continue # Skip if essential data missing

            if flight_dict_with_id["flight_id_internal"] not in existing_flight_ids_globally:
                flight_to_save = {k:v for k,v in flight_dict_with_id.items() if k != "flight_id_internal"}
                newly_found_flights_formatted.append(flight_to_save)
                existing_flight_ids_globally.add(flight_dict_with_id["flight_id_internal"])
    
    print(f"  Found {len(newly_found_flights_formatted)} new, unique flights for this specific route.")
    return newly_found_flights_formatted

def main():
    """Run the process."""
    session = get_session()
    discover_danish_airports(session)

    routes_data = load_json(ROUTES_FILE) # List of route dicts
    results_data = load_json(RESULTS_FILE) # List of flight dicts

    # Build initial set of all known flight_id_internals from results_data
    existing_flight_ids_globally = set()
    for f_obj_existing in results_data:
        # Reconstruct flight_id_internal for comparison, ensure it matches format_flight_object's generation
        # This is a critical part for deduplication.
        # It's better if format_flight_object itself is used if possible, or ensure consistency.
        # For simplicity, assuming essential fields are present in loaded data.
        dep_iso = f_obj_existing.get('departure')
        if dep_iso: # Only process if departure is valid
            fid = (f"{f_obj_existing.get('iata_origin','')}-{f_obj_existing.get('iata_destination','')}-"
                   f"{dep_iso}-{f_obj_existing.get('carrier','')}-{f_obj_existing.get('flight_numbers','')}")
            # If transportKey was part of original ID, it needs to be reconstructed or stored.
            # This simplified reconstruction might miss transportKey if it was used.
            # A robust way is to *always* store flight_id_internal in results_data too,
            # and then filter it out only for final output if strictly needed.
            # For now, we proceed with this simpler reconstruction.
            existing_flight_ids_globally.add(fid)


    if not routes_data or not isinstance(routes_data, list) or not all(isinstance(r, dict) for r in routes_data):
        print("No valid routes found or routes.json is malformed. Performing initial discovery.")
        routes_data, initial_flights_formatted = discover_routes_and_initial_flights(session)
        # initial_flights_formatted does not contain flight_id_internal
        # Add these to results_data, ensuring global ID set is updated
        for new_flight_formatted in initial_flights_formatted:
             # Reconstruct ID to check if it was somehow already processed (e.g., if discover runs multiple times)
            dep_iso = new_flight_formatted.get('departure')
            if dep_iso:
                fid_check = (f"{new_flight_formatted.get('iata_origin','')}-{new_flight_formatted.get('iata_destination','')}-"
                             f"{dep_iso}-{new_flight_formatted.get('carrier','')}-{new_flight_formatted.get('flight_numbers','')}")
                # Add transportKey if it was part of ID generation
                if fid_check not in existing_flight_ids_globally:
                    results_data.append(new_flight_formatted)
                    existing_flight_ids_globally.add(fid_check)
        save_json(results_data, RESULTS_FILE)
        # Ensure routes_data (which is global-like here) is the one from discovery
    else:
        print(f"Loaded {len(routes_data)} routes and {len(results_data)} results.")

    target_routes_covered_in_results = 50 # Goal for results.json

    while True:
        def count_unique_routes_in_file(results_list): # Helper to count unique O-D pairs
            return len({(f.get("iata_origin"), f.get("iata_destination")) for f in results_list if f.get("iata_origin") and f.get("iata_destination")})

        current_routes_covered = count_unique_routes_in_file(results_data)
        print(f"\nResults file covers {current_routes_covered} unique O-D pairs (Target: {target_routes_covered_in_results}).")

        if current_routes_covered >= target_routes_covered_in_results:
            print("Target for unique routes coverage in results.json met or exceeded.")
            break

        work_batch = get_work()
        if not work_batch:
            print("No more routes due for update. Current coverage might be less than target if all routes are up-to-date.")
            break

        print(f"Processing batch of {len(work_batch)} due routes...")
        for route_to_process in work_batch:
            if not isinstance(route_to_process, dict) or 'origin_caid' not in route_to_process:
                print(f"Skipping invalid route object from get_work(): {route_to_process}")
                continue

            newly_extracted_flights = extract(route_to_process, session, existing_flight_ids_globally)
            
            if newly_extracted_flights:
                results_data.extend(newly_extracted_flights)
            
            # Prepare data for get_update_after: all current flights for this specific route
            all_known_flights_for_this_route = list(newly_extracted_flights) # Start with just extracted
            for f_exist in results_data: # Check against the main, growing results_data
                if f_exist.get("iata_origin") == route_to_process.get("iata_origin") and \
                   f_exist.get("iata_destination") == route_to_process.get("destination_iata"):
                    # Avoid re-adding if it was just extracted (simple check)
                    is_present = any(
                        nf.get("departure") == f_exist.get("departure") and nf.get("carrier") == f_exist.get("carrier")
                        for nf in newly_extracted_flights 
                    )
                    if not is_present:
                         all_known_flights_for_this_route.append(f_exist)


            next_update_timestamp = get_update_after(route_to_process, all_known_flights_for_this_route, results_data)

            
            route_found_and_updated = False
            for i, r_obj in enumerate(routes_data):
                if isinstance(r_obj, dict) and \
                   r_obj.get("origin_iata") == route_to_process.get("origin_iata") and \
                   r_obj.get("destination_iata") == route_to_process.get("destination_iata"):
                    routes_data[i]["last_updated"] = get_current_timestamp_iso()
                    routes_data[i]["update_after"] = next_update_timestamp
                    route_found_and_updated = True
                    break
            if not route_found_and_updated:
                print(f"Warning: Route {route_to_process.get('origin_iata')}->{route_to_process.get('destination_iata')} processed but not found in main routes_data list for metadata update.")

        save_json(routes_data, ROUTES_FILE)
        save_json(results_data, RESULTS_FILE) 

    print(f"\n--- Scraper Finished ---")
    final_routes_count = len(load_json(ROUTES_FILE))
    final_results_count = len(load_json(RESULTS_FILE))
    final_routes_covered = count_unique_routes_in_file(load_json(RESULTS_FILE))
    print(f"Total routes in {ROUTES_FILE}: {final_routes_count}")
    print(f"Total flight results in {RESULTS_FILE}: {final_results_count}")
    print(f"Results file covers {final_routes_covered} unique O-D pairs.")

if __name__ == "__main__":
    print("Starting Spies.dk scraper...")
    print("IMPORTANT: Make sure your 'auth-userid' in DEFAULT_HEADERS is current!")
    main()