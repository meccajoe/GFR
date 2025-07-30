import os
import requests
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SEASON = "2020"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

def fetch_leagues():
    response = supabase.table("mens_leagues").select("api_league_id").execute()
    league_ids = [entry["api_league_id"] for entry in response.data]
    return league_ids

def fetch_matches(league_id, season):
    url = f"https://v3.football.api-sports.io/fixtures"
    params = {
        "league": league_id,
        "season": season,
        "status": "FT"
    }
    headers = {
        "x-apisports-key": API_FOOTBALL_KEY
    }
    res = requests.get(url, headers=headers, params=params)
    res.raise_for_status()
    return res.json().get("response", [])

def insert_matches(matches):
    records = []
    for match in matches:
        data = {
            "match_id": match["fixture"]["id"],
            "home_team_id": match["teams"]["home"]["id"],
            "away_team_id": match["teams"]["away"]["id"],
            "home_team_name": match["teams"]["home"]["name"],
            "away_team_name": match["teams"]["away"]["name"],
            "league_id": match["league"]["id"],
            "season": match["league"]["season"],
            "match_date": match["fixture"]["date"],
            "home_score": match["goals"]["home"],
            "away_score": match["goals"]["away"],
            "is_completed": True,
            "is_neutral": match["fixture"].get("neutral", False),
            "last_updated": match["fixture"]["date"]
        }
        records.append(data)
    if records:
        supabase.table("match_results").upsert(records).execute()
        print(f"Inserted {len(records)} matches.")
    else:
        print("No matches to insert.")

def main():
    leagues = fetch_leagues()
    for league_id in leagues:
        print(f"Fetching matches for league {league_id} in {SEASON}")
        try:
            matches = fetch_matches(league_id, SEASON)
            insert_matches(matches)
        except Exception as e:
            print(f"Error fetching matches for league {league_id}: {e}")

if __name__ == "__main__":
    main()
