import os
import requests
from datetime import datetime
from supabase import create_client, Client

# Load environment variables (Railway will automatically inject these)
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
API_FOOTBALL_KEY = os.environ["API_FOOTBALL_KEY"]

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# Constants
LEAGUE_IDS = [39, 61, 135, 140, 78]  # Premier League, Ligue 1, Serie A, La Liga, Bundesliga
SEASON = 2020
API_BASE_URL = "https://v3.football.api-sports.io/fixtures"

HEADERS = {
    "x-apisports-key": API_FOOTBALL_KEY
}


def fetch_matches(league_id, season):
    print(f"Fetching matches for league {league_id} in {season}")
    params = {
        "league": league_id,
        "season": season,
        "status": "FT"
    }
    response = requests.get(API_BASE_URL, headers=HEADERS, params=params)
    response.raise_for_status()
    data = response.json()
    return data.get("response", [])


def insert_matches(matches):
    inserted_count = 0
    for match in matches:
        fixture = match["fixture"]
        teams = match["teams"]
        goals = match["goals"]
        league = match["league"]

        home_id = teams["home"]["id"]
        away_id = teams["away"]["id"]
        home_name = teams["home"]["name"]
        away_name = teams["away"]["name"]
        match_id = fixture["id"]
        date = fixture["date"]
        home_score = goals["home"]
        away_score = goals["away"]
        is_neutral = fixture.get("venue", {}).get("neutral", False)

        try:
            supabase.table("match_results").insert({
                "match_id": match_id,
                "home_team_id": home_id,
                "away_team_id": away_id,
                "home_team_name": home_name,
                "away_team_name": away_name,
                "league_id": league["id"],
                "season": str(league["season"]),
                "match_date": date,
                "home_score": home_score,
                "away_score": away_score,
                "is_completed": True,
                "is_neutral": is_neutral,
                "last_updated": datetime.utcnow().isoformat()
            }).execute()
            inserted_count += 1
        except Exception as e:
            print(f"Failed to insert match {match_id}: {e}")

    print(f"Inserted {inserted_count} matches...\n")


def main():
    for league_id in LEAGUE_IDS:
        matches = fetch_matches(league_id, SEASON)
        insert_matches(matches)


if __name__ == "__main__":
    main()
