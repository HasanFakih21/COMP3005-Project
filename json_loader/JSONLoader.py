import json
import requests
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="soccer_database",
    user="postgres",
    password="postgres"
)

repo_owner = "statsbomb"
repo_name = "open-data"
branch = "master"
base_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/contents"

def load_matches():
    print("Loading matches...")
    url = f"{base_url}/data/matches"
    response = requests.get(url)
    folders = response.json()

    for folder in folders:
        folder_url = folder["url"]
        folder_response = requests.get(folder_url)
        files = folder_response.json()

        for file in files:
            if file["name"].endswith(".json"):
                file_url = file["download_url"]
                file_response = requests.get(file_url)
                match_data = file_response.json()

                for match in match_data:
                    match_id = match["match_id"]
                    season_name = match["season"]["season_name"]

                    cur = conn.cursor()
                    cur.execute("SELECT COUNT(*) FROM matches WHERE match_id = %s", (match_id,))
                    count = cur.fetchone()[0]
                    if count == 0:
                        cur.execute("INSERT INTO matches (match_id, season) VALUES (%s, %s)", (match_id, season_name))
                        conn.commit()
                        print(f"Inserted match: {match_id}")
                    else:
                        print(f"Match {match_id} already exists. Skipping.")
                    cur.close()
    print("Matches loaded.")

def load_teams():
    print("Loading teams...")
    url = f"{base_url}/data/lineups"
    response = requests.get(url)
    files = response.json()

    for file in files:
        if file["name"].endswith(".json"):
            file_url = file["download_url"]
            file_response = requests.get(file_url)
            lineup_data = file_response.json()

            for lineup in lineup_data:
                team_id = lineup["team_id"]
                team_name = lineup["team_name"]

                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM teams WHERE team_id = %s", (team_id,))
                count = cur.fetchone()[0]
                if count == 0:
                    cur.execute("INSERT INTO teams (team_id, team_name) VALUES (%s, %s)", (team_id, team_name))
                    conn.commit()
                    print(f"Inserted team: {team_name}")
                else:
                    print(f"Team {team_name} already exists. Skipping.")
                cur.close()
    print("Teams loaded.")

def load_players():
    print("Loading players...")
    url = f"{base_url}/data/lineups"
    response = requests.get(url)
    files = response.json()

    for file in files:
        if file["name"].endswith(".json"):
            file_url = file["download_url"]
            file_response = requests.get(file_url)
            lineup_data = file_response.json()

            for lineup in lineup_data:
                for player in lineup["lineup"]:
                    player_id = player["player_id"]
                    player_name = player["player_name"]

                    cur = conn.cursor()
                    cur.execute("SELECT COUNT(*) FROM players WHERE player_id = %s", (player_id,))
                    count = cur.fetchone()[0]
                    if count == 0:
                        cur.execute("INSERT INTO players (player_id, player_name) VALUES (%s, %s)", (player_id, player_name))
                        conn.commit()
                        print(f"Inserted player: {player_name}")
                    else:
                        print(f"Player {player_name} already exists. Skipping.")
                    cur.close()
    print("Players loaded.")

def load_player_statistics():
    print("Loading player statistics...")
    url = f"{base_url}/data/events"
    response = requests.get(url)
    files = response.json()

    for file in files:
        if file["name"].endswith(".json"):
            file_url = file["download_url"]
            file_response = requests.get(file_url)
            event_data = file_response.json()

            # Extract the match_id from the filename
            match_id = int(file["name"].split(".")[0])

            for event in event_data:
                if "player" in event:
                    player_id = event["player"]["id"]
                    shots = 1 if "shot" in event else 0
                    first_time_shots = 1 if event.get("shot", {}).get("first_time") else 0
                    through_balls = 1 if event.get("pass", {}).get("type", {}).get("id") == 108 else 0
                    successful_dribbles = 1 if event.get("dribble", {}).get("outcome", {}).get("id") == 8 else 0
                    dribbled_past = 1 if "dribbled_past" in event else 0
                    intended_recipient_of_passes = event.get("pass", {}).get("recipient", {}).get("id")
                    average_xg = event.get("shot", {}).get("statsbomb_xg")

                    cur = conn.cursor()
                    cur.execute(
                        "SELECT COUNT(*) FROM playerstatistics WHERE match_id = %s AND player_id = %s",
                        (match_id, player_id)
                    )
                    count = cur.fetchone()[0]
                    if count == 0:
                        cur.execute(
                            "INSERT INTO playerstatistics (match_id, player_id, shots, first_time_shots, through_balls, successful_dribbles, dribbled_past, intended_recipient_of_passes, average_xg) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            (match_id, player_id, shots, first_time_shots, through_balls, successful_dribbles, dribbled_past, intended_recipient_of_passes, average_xg)
                        )
                        conn.commit()
                        print(f"Inserted player statistics for match {match_id} and player {player_id}")
                    else:
                        cur.execute(
                            "UPDATE playerstatistics SET shots = shots + %s, first_time_shots = first_time_shots + %s, through_balls = through_balls + %s, successful_dribbles = successful_dribbles + %s, dribbled_past = dribbled_past + %s, intended_recipient_of_passes = COALESCE(intended_recipient_of_passes, %s), average_xg = COALESCE(average_xg, %s) WHERE match_id = %s AND player_id = %s",
                            (shots, first_time_shots, through_balls, successful_dribbles, dribbled_past, intended_recipient_of_passes, average_xg, match_id, player_id)
                        )
                        conn.commit()
                        print(f"Updated player statistics for match {match_id} and player {player_id}")
                    cur.close()
    print("Player statistics loaded.")
    
def load_team_statistics():
    print("Loading team statistics...")
    url = f"{base_url}/data/events"
    response = requests.get(url)
    files = response.json()

    for file in files:
        if file["name"].endswith(".json"):
            file_url = file["download_url"]
            file_response = requests.get(file_url)
            event_data = file_response.json()

            team_stats = {}

            for event in event_data:
                match_id = event["match_id"]
                team_id = event["team"]["id"]

                if match_id not in team_stats:
                    team_stats[match_id] = {}

                if team_id not in team_stats[match_id]:
                    team_stats[match_id][team_id] = {
                        "passes": 0,
                        "through_balls": 0,
                        "shots": 0
                    }

                if "pass" in event:
                    team_stats[match_id][team_id]["passes"] += 1
                    if event["pass"]["type"]["id"] == 2:
                        team_stats[match_id][team_id]["through_balls"] += 1

                if "shot" in event:
                    team_stats[match_id][team_id]["shots"] += 1

            for match_id, teams in team_stats.items():
                for team_id, stats in teams.items():
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT COUNT(*) FROM teamstatistics WHERE match_id = %s AND team_id = %s",
                        (match_id, team_id)
                    )
                    count = cur.fetchone()[0]
                    if count == 0:
                        cur.execute(
                            "INSERT INTO teamstatistics (match_id, team_id, passes, through_balls, shots) VALUES (%s, %s, %s, %s, %s)",
                            (match_id, team_id, stats["passes"], stats["through_balls"], stats["shots"])
                        )
                        conn.commit()
                        print(f"Inserted team statistics for match {match_id} and team {team_id}")
                    else:
                        print(f"Team statistics for match {match_id} and team {team_id} already exist. Skipping.")
                    cur.close()
    print("Team statistics loaded.")

load_matches()
load_teams()
load_players()
load_player_statistics()
load_team_statistics()

conn.close()
