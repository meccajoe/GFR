import requests
from supabase import create_client, Client

# Hardcoded config values
API_FOOTBALL_KEY = "1457bb65bdcf56cd52de247d8e03a180"
SUPABASE_URL = "https://ifluudzbwomhiveydmbb.supabase.co"
SUPABASE_SERVICE_ROLE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlmbHV1ZHpid29taGl2ZXlkbWJiIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0OTI2NjYwMiwiZXhwIjoyMDY0ODQyNjAyfQ.Kb4XhzY-78mbrSCtlXymGM_G0eHFj44dHytma7eXa6A"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

headers = {
    "x-apisports-key": API_FOOTBALL_KEY
}

seasons = [2021, 2022, 2023, 2024]

def fetch_leagues():
    data = supabase.table("mens_leagues").select("api_league_id").execute().data
    return [int(item["api_league_id"]) for item in data if item.get("api_league_id")]

def fetch_matches(league_id, season):
    print(f"Fetching matches for league {league_id} in {season}")
    url = f"https://v3.football.api-sports.io/fixtures"
    params = {
        "league": league_id,
        "season": season,
        "status": "FT"
    }
    res = requests.get(url, headers=headers, params=params)
    res.raise_for_status()
    return res.json().get("response", [])

def insert_match(match, season):
    try:
        match_data = {
            "match_id": match["fixture"]["id"],
            "home_team_id": match["teams"]["home"]["id"],
            "away_team_id": match["teams"]["away"]["id"],
            "home_team_name": match["teams"]["home"]["name"],
            "away_team_name": match["teams"]["away"]["name"],
            "league_id": match["league"]["id"],
            "season": season,
            "match_date": match["fixture"]["date"],
            "home_score": match["goals"]["home"],
            "away_score": match["goals"]["away"],
            "is_completed": match["fixture"]["status"]["short"] == "FT",
            "is_neutral": match.get("neutral", False),
            "last_updated": match["fixture"]["updated"]
        }

        supabase.table("match_results").insert(match_data).execute()
    except Exception as e:
        print(f"Failed to insert match {match['fixture']['id']}: {e}")

def main():
    leagues = fetch_leagues()
    for league_id in leagues:
        for season in seasons:
            matches = fetch_matches(league_id, season)
            print(f"Found {len(matches)} matches.")
            for match in matches:
                insert_match(match, season)

if __name__ == "__main__":
    main()
