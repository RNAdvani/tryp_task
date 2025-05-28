# Spies.dk Flight Scraper

A simple web scraper for collecting flight data from Spies.dk travel site using their GraphQL API (https://origo.nltg.com/).

## What it does

This tool collects one-way flight information from the Spies.dk website using their GraphQL API. It:

1. Discovers Danish airports and their routes
2. Finds flights for each route 
3. Saves the data to JSON files
4. Updates routes on a smart schedule based on upcoming flights

## Files created

- `routes.json` - Contains all discovered origin-destination pairs
- `results.json` - Contains all flight data

## How to use

1. Make sure to update the `auth-userid` in the `DEFAULT_HEADERS` constant
2. Run the script with Python:

```
python main.py
```

## Requirements

- Python 3.6+
- Requests library

To install requirements:
```
pip install requests
```

## Notes

- The script uses a polite delay between requests
- By default, it aims to cover at least 50 unique routes
- Data is stored in the same folder as the script
- Flight prices are converted from DKK to EUR

## How it works

The scraper uses a few clever tricks to efficiently collect flight data:

1. First run discovers all airports and routes from Denmark
2. Each route gets its own update schedule based on flight times
3. Routes with upcoming departures get checked more frequently
4. The scraper prioritizes routes that haven't been updated recently

This approach means we don't waste time checking routes that won't have new flights soon.

## Debugging tips

If you're running into issues:

- Check the `auth-userid` value - this expires frequently
- Look for any error messages about "GraphQL Errors" in the console
- Make sure your internet connection is stable
- If getting timeout errors, increase the timeout value in the `call_graphql_api` function

## Output format

Flight data in `results.json` includes:

```
{
  "flight_id_internal": "CPH-AGA-2023-07-15T06:30:00Z-DK-DK1234",
  "departure": "2023-07-15T06:30:00Z",
  "arrival": "2023-07-15T10:45:00Z",
  "duration": 4.25,
  "carrier": "DK",
  "iata_origin": "CPH",
  "iata_destination": "AGA",
  "price": 149.5,
  "stops": 0,
  "flight_numbers": "DK1234",
  "vehicle_type": "aircraft"
}
```

**Note:** The `duration` value might be 0 for some flights. This is because the actual flight duration information is only available on the checkout page, and I couldn't find the GraphQL endpoint that provides this data.

## Technical details

The scraper interacts with Spies.dk's GraphQL API at `https://origo.nltg.com/` using two main queries:

1. `meta` query - Discovers available airports
2. `InitialFlightOffers` query - Fetches flights for specific routes

The API requires an `auth-userid` header which expires periodically and needs to be refreshed manually. I tried finding additional endpoints for more detailed flight information (like durations), but some data seems to only be available on the checkout pages that use different endpoints.

## Limitations

- The API may change, which would require script updates
- The `auth-userid` may expire and need to be refreshed
- Only handles one-way flights currently
- No filtering by airline or specific date ranges

## Future improvements

I've been meaning to add:

- Command line arguments for more flexible usage
- Filtering options for specific dates or airlines
