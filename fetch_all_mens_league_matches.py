import requests
import time
from supabase import create_client, Client

# Hardcoded Supabase and API-Football credentials
SUPABASE_URL = "https://ifluudzbwomhiveydmbb.supabase.co"
SUPABASE_SERVICE_ROLE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlmbHV1ZHpid29taGl2ZXlkbWJiIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0OTI2NjYwMiwiZXhwIjoyMDY0ODQyNjAyfQ.Kb4XhzY-78mbrSCtlXymGM_G0eHFj44dHytma7eXa6A"
API_FOOTBALL_KEY = "1457bb65bdcf56cd52de247d8e03a180"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

HEADERS = {"x-apisports-key": API_FOOTBALL_KEY}
BASE_URL = "https://v3.football.api-sports.io/fixtures"

# Define the seasons you want to fetch (excluding 2020 since it's already done)
seasons = [2021, 2022, 2023, 2024]

def fetch_and_store_matches(league_id, season):
    print(f"Fetching matches for league {league_id} in {season}")
    url = f"{BASE_URL}?league={league_id}&season={season}&status=FT"
    response = requests.get(url, headers=HEADERS)
    data = response.json()

    if data.get("errors") or data["results"] == 0:
        print(f"No matches or error for league {league_id} in {season}")
        return

    matches = data["response"]
    inserted = 0
    for match in matches:
        try:
            supabase.table("match_results").insert({
                "match_id": match["fixture"]["id"],
                "league_id": match["league"]["id"],
                "season": match["league"]["season"],
                "date": match["fixture"]["date"],
                "home_team_id": match["teams"]["home"]["id"],
                "away_team_id": match["teams"]["away"]["id"],
                "home_goals": match["goals"]["home"],
                "away_goals": match["goals"]["away"],
                "status": match["fixture"]["status"]["short"]
            }).execute()
            inserted += 1
        except Exception as e:
            print(f"Failed to insert match {match['fixture']['id']}: {e}")

    print(f"Inserted {inserted} matches for league {league_id} in {season}")

def main():
    leagues = supabase.table("mens_leagues").select("api_league_id").execute().data
    league_ids = [int(row["api_league_id"]) for row in leagues]

    for season in seasons:
        for league_id in league_ids:
            fetch_and_store_matches(league_id, season)
            time.sleep(1.5)  # Be kind to the API

if __name__ == "__main__":
    main()