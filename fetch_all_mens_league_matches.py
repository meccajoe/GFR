
import requests
from supabase import create_client, Client
import time

# Hardcoded credentials
SUPABASE_URL = "https://ifluudzbwomhiveydmbb.supabase.co"
SUPABASE_SERVICE_ROLE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlmbHV1ZHpid29taGl2ZXlkbWJiIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0OTI2NjYwMiwiZXhwIjoyMDY0ODQyNjAyfQ.Kb4XhzY-78mbrSCtlXymGM_G0eHFj44dHytma7eXa6A"
API_FOOTBALL_KEY = "1457bb65bdcf56cd52de247d8e03a180"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

HEADERS = {
    "x-apisports-key": API_FOOTBALL_KEY
}

SEASONS = [2021, 2022, 2023, 2024]

def fetch_and_store_matches(league_id: int, season: int):
    print(f"Fetching matches for league {league_id} in {season}")
    url = f"https://v3.football.api-sports.io/fixtures?league={league_id}&season={season}&status=FT"
    response = requests.get(url, headers=HEADERS)
    data = response.json()

    if data.get("errors"):
        print(f"API error for league {league_id} season {season}: {data['errors']}")
        return

    matches = data.get("response", [])
    print(f"Found {len(matches)} matches.")

    for match in matches:
        match_data = {
            "match_id": match["fixture"]["id"],
            "date": match["fixture"]["date"],
            "league_id": match["league"]["id"],
            "season": match["league"]["season"],
            "home_team": match["teams"]["home"]["name"],
            "away_team": match["teams"]["away"]["name"],
            "home_score": match["goals"]["home"],
            "away_score": match["goals"]["away"]
        }

        try:
            supabase.table("match_results").insert(match_data).execute()
        except Exception as e:
            print(f"Failed to insert match {match_data['match_id']}: {e}")

        time.sleep(0.25)  # Stay well below rate limits

def main():
    leagues_data = supabase.table("mens_leagues").select("api_league_id").execute()
    league_ids = [row["api_league_id"] for row in leagues_data.data if "api_league_id" in row]

    for league_id in league_ids:
        for season in SEASONS:
            fetch_and_store_matches(league_id, season)

if __name__ == "__main__":
    main()
