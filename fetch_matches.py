
import os
import requests
import time
import datetime
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# API-Football & Supabase credentials
API_KEY = os.getenv("API_FOOTBALL_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# Create Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# Define headers for API-Football
headers = {
    "x-apisports-key": API_KEY
}

# Helper function to fetch match results for one league-season combo
def fetch_matches_for_league_season(league_id, season):
    url = "https://v3.football.api-sports.io/fixtures"
    page = 1
    all_results = []

    while True:
        params = {
            "league": league_id,
            "season": season,
            "status": "FT",
            "page": page
        }
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"Error fetching page {page} for league {league_id}: {response.status_code}")
            break

        data = response.json()
        matches = data["response"]

        if not matches:
            break

        for match in matches:
            fixture = match["fixture"]
            teams = match["teams"]
            goals = match["goals"]
            league = match["league"]

            result = {
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
                "is_neutral": fixture["venue"]["name"] == "Neutral",
                "last_updated": datetime.datetime.utcnow().isoformat()
            }

            all_results.append(result)

        page += 1
        time.sleep(1)  # Respect rate limits

    return all_results

# Insert into Supabase
def insert_to_supabase(match_list):
    for match in match_list:
        try:
            supabase.table("match_results").insert(match).execute()
        except Exception as e:
            print(f"Failed to insert match {match['match_id']}: {str(e)}")

# List of (league_id, season) to start with
initial_leagues = [
    39,    # EPL
    61,    # Ligue 1
    135,   # Serie A
    140,   # La Liga
    78     # Bundesliga
]

if __name__ == "__main__":
    for league_id in initial_leagues:
        print(f"Fetching matches for league {league_id} in 2020")
        matches = fetch_matches_for_league_season(league_id, 2020)
        print(f"Inserting {len(matches)} matches...")
        insert_to_supabase(matches)
        print("Done.")

