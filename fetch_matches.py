import os
import requests
from dotenv import load_dotenv
from datetime import datetime
from supabase import create_client, Client

load_dotenv()

API_KEY = os.getenv("API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

HEADERS = {
    "x-apisports-key": API_KEY
}

def fetch_fixtures(league_id, season):
    url = f"https://v3.football.api-sports.io/fixtures"
    params = {
        "league": league_id,
        "season": season,
        "status": "FT"
    }
    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    return response.json().get("response", [])

def insert_matches(matches):
    if not matches:
        return
    rows = []
    for match in matches:
        fixture = match["fixture"]
        teams = match["teams"]
        goals = match["goals"]
        league = match["league"]

        rows.append({
            "match_id": fixture["id"],
            "home_team_id": teams["home"]["id"],
            "away_team_id": teams["away"]["id"],
            "home_team_name": teams["home"]["name"],
            "away_team_name": teams["away"]["name"],
            "league_id": league["id"],
            "season": str(league["season"]),
            "match_date": fixture["date"],
            "home_score": goals["home"],
            "away_score": goals["away"],
            "is_completed": True,
            "is_neutral": fixture["venue"]["name"] == "Neutral Location",
            "last_updated": datetime.utcnow().isoformat()
        })

    data = supabase.table("match_results").insert(rows).execute()
    print(f"Inserted {len(rows)} matches.")

def main():
    leagues = [39, 61, 135, 140, 78]  # Example league IDs
    season = 2020

    for league_id in leagues:
        print(f"Fetching matches for league {league_id} in {season}")
        matches = fetch_fixtures(league_id, season)
        print(f"Retrieved {len(matches)} matches.")
        insert_matches(matches)
        print("Done.\n")

if __name__ == "__main__":
    main()
    
