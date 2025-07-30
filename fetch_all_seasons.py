import os
import requests
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime
import time

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

def fetch_and_insert_matches(league_id, season):
    print(f"Fetching matches for league {league_id} in {season}")
    url = f"https://v3.football.api-sports.io/fixtures"
    params = {
        "league": league_id,
        "season": season,
        "status": "FT"
    }
    headers = {
        "x-apisports-key": API_FOOTBALL_KEY
    }

    response = requests.get(url, headers=headers, params=params)
    data = response.json()
    fixtures = data.get("response", [])

    print(f"Found {len(fixtures)} matches.")

    for fixture in fixtures:
        match = fixture["fixture"]
        teams = fixture["teams"]
        goals = fixture["goals"]
        league = fixture["league"]

        match_data = {
            "match_id": match["id"],
            "home_team_id": teams["home"]["id"],
            "away_team_id": teams["away"]["id"],
            "home_team_name": teams["home"]["name"],
            "away_team_name": teams["away"]["name"],
            "league_id": league["id"],
            "season": str(league["season"]),
            "match_date": match["date"],
            "home_score": goals["home"],
            "away_score": goals["away"],
            "is_completed": match["status"]["short"] == "FT",
            "is_neutral": match.get("neutral", False),
            "last_updated": datetime.utcnow().isoformat()
        }

        try:
            response = supabase.table("match_results").insert(match_data).execute()
            print(f"Inserted match {match['id']}")
        except Exception as e:
            print(f"Failed to insert match {match['id']}: {e}")
    print("Done.")

def main():
    season = [2021,2022,2023,2024]
    leagues = supabase.table("mens_leagues").select("api_league_id").execute().data

    for league in leagues:
        league_id = league["api_league_id"]
        if league_id:
            fetch_and_insert_matches(league_id, season)
            time.sleep(1.5)  # Slight delay to avoid hitting rate limits

if __name__ == "__main__":
    main()
