import requests
from supabase import create_client, Client
from datetime import datetime
import time

SUPABASE_URL = "https://ifluudzbwomhiveydmbb.supabase.co"
SUPABASE_SERVICE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlmbHV1ZHpid29taGl2ZXlkbWJiIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0OTI2NjYwMiwiZXhwIjoyMDY0ODQyNjAyfQ.Kb4XhzY-78mbrSCtlXymGM_G0eHFj44dHytma7eXa6A"
API_FOOTBALL_KEY = "1457bb65bdcf56cd52de247d8e03a180"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

SEASONS = [2021, 2022, 2023, 2024]
def fetch_and_insert_matches(league_id, season):
    print(f"Fetching matches for league {league_id} in {season}")
    url = "https://v3.football.api-sports.io/fixtures"
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
            "last_updated": datetime.utcnow().isoformat()
        }

        try:
            supabase.table("match_results").insert(match_data).execute()
            print(f"Inserted match {match['id']}")
        except Exception as e:
            print(f"Failed to insert match {match['id']}: {e}")
    print("Done.")

def main():
    leagues = supabase.table("mens_leagues").select("api_league_id").execute().data
    for season in SEASONS:
        for league in leagues:
            league_id = league["api_league_id"]
            fetch_and_insert_matches(league_id, season)
            time.sleep(1)

if __name__ == "__main__":
    main()
