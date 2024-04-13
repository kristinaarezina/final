import json
import os
import psycopg
from psycopg.rows import dict_row

def load_json_data_from_match(conn, json_dir_path, filter_criteria):
    with conn.cursor() as cur:
        for comp_id, season_id in filter_criteria:
            comp_dir_path = os.path.join(json_dir_path, str(comp_id))

            if os.path.isdir(comp_dir_path):
                for file_name in os.listdir(comp_dir_path):
                    if file_name.endswith('.json'):
                        file_path = os.path.join(comp_dir_path, file_name)

                        with open(file_path, 'r') as file:
                            matches_data = json.load(file)

                            for match_data in matches_data:
                                if match_data['season']['season_id'] == season_id:
                                    manager_id = None
                                    home_team_id = None
                                    away_team_id = None
                                    stadium_id = None
                                    referee_id = None
                                    competition_stage_id = None
                                    managers = match_data.get('home_team', {}).get('managers', []) + match_data.get('away_team', {}).get('managers', [])
                                    for m in managers:  
                                        manager_id = m.get('id')
                                        manager_name = m.get('name')
                                        manager_nickname = m.get('nickname')  
                                        manager_dob = m.get('dob')  
                                        manager_country_name = m.get('country', {}).get('name', None)
                                        cur.execute("""
                                            INSERT INTO Managers (
                                                manager_id, manager_name, manager_nickname, manager_dob, manager_country_name
                                            ) 
                                            VALUES (%s, %s, %s, %s, %s)
                                            ON CONFLICT DO NOTHING;
                                        """, (
                                            manager_id, manager_name, manager_nickname,
                                            manager_dob, manager_country_name
                                        ))

                                    stadium = match_data.get('stadium', None)
                                    if stadium:
                                        stadium_id = stadium.get('id')
                                        stadium_name = stadium.get('name')
                                        stadium_country_name = stadium.get('country', {}).get('name', None)
                                        cur.execute("""
                                                INSERT INTO Stadiums (
                                                    stadium_id, name, country_name
                                                ) 
                                                VALUES (%s, %s, %s)
                                                ON CONFLICT DO NOTHING;
                                            """, (
                                                stadium_id, stadium_name, stadium_country_name,
                                            ))

                                    referees = match_data.get('referee', None)
                                    if referees:
                                        referees_id = referees.get('id')
                                        referees_name = referees.get('name')
                                        referees_name_country_name = referees.get('country', {}).get('name', None)
                                        cur.execute("""
                                                INSERT INTO Referees (
                                                    referee_id, name, country_name
                                                ) 
                                                VALUES (%s, %s, %s)
                                                ON CONFLICT DO NOTHING;
                                            """, (
                                                referees_id, referees_name, referees_name_country_name,
                                            ))
                                    
                                    # teams
                                    home_team = match_data.get('home_team', None)
                                    if home_team:
                                        home_team_id = home_team.get('home_team_id')
                                        home_team_name = home_team.get('home_team_name')
                                        home_team_gender = home_team.get('home_team_gender')
                                        home_team_group = home_team.get('home_team_group')
                                        home_team_group_country_name = home_team.get('country', {}).get('name', None)
                                        cur.execute("""
                                                INSERT INTO Teams (
                                                    team_id, team_name, team_gender, manager_id, country_name, group_name 
                                                ) 
                                                VALUES (%s, %s, %s,%s, %s, %s)
                                                ON CONFLICT DO NOTHING;
                                            """, (
                                                home_team_id, home_team_name, home_team_gender, manager_id, home_team_group_country_name, home_team_group
                                            ))

                                    away_team = match_data.get('away_team', None)
                                    if away_team:
                                        away_team_id = away_team.get('away_team_id')
                                        away_team_name = away_team.get('away_team_name')
                                        away_team_gender = away_team.get('away_team_gender')
                                        away_team_group = away_team.get('away_team_group')
                                        away_team_group_country_name = away_team.get('country', {}).get('name', None)
                                        cur.execute("""
                                                INSERT INTO Teams (
                                                    team_id, team_name, team_gender, manager_id, country_name, group_name 
                                                ) 
                                                VALUES (%s, %s, %s,%s, %s, %s)
                                                ON CONFLICT DO NOTHING;
                                            """, (
                                                away_team_id, away_team_name, away_team_gender, manager_id, away_team_group_country_name, away_team_group
                                            ))


                                    # CompetitionStages
                                    competition_stage = match_data.get('competition_stage', None)
                                    if competition_stage:
                                        competition_stage_id = competition_stage.get('id')
                                        competition_stage_name = competition_stage.get('name')
                                        cur.execute("""
                                                INSERT INTO CompetitionStages (
                                                    stage_id, name
                                                ) 
                                                VALUES (%s, %s)
                                                ON CONFLICT DO NOTHING;
                                            """, (
                                                competition_stage_id, competition_stage_name
                                            ))


                                    # matches
                                    match_id = match_data.get('match_id', None)
                                    season_id = match_data.get('season', {}).get('season_id', None)
                                    competition_id = match_data.get('competition', {}).get('competition_id', None)
                                    match_date = match_data.get('match_date', None)
                                    kick_off = match_data.get('kick_off', None)
                                    home_score = match_data.get('home_score', None)
                                    away_score = match_data.get('away_score', None)
                                    match_status = match_data.get('match_status', None)
                                    match_week = match_data.get('match_week', None)
                                    last_updated = match_data.get('last_updated', None)
                                    last_updated_360 = match_data.get('last_updated_360', None)
                                    cur.execute("""
                                                INSERT INTO Matches (
                                                    match_id, season_id, competition_id, home_team_id, away_team_id, match_date, kick_off, stadium_id, referee_id, home_score, away_score, match_status, match_week,stage_id, last_updated, last_updated_360 
                                                ) 
                                                VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s)
                                                ON CONFLICT DO NOTHING;
                                            """, (
                                                match_id, season_id, competition_id,home_team_id, away_team_id, match_date, kick_off, stadium_id, referee_id, home_score, away_score, match_status, match_week, competition_stage_id, last_updated, last_updated_360
                                            ))
                conn.commit()
                                    
                                
            else:
                print(f"Competition directory {comp_dir_path} does not exist.")

conn = psycopg.connect("dbname=project_database user=postgres password=1234", row_factory=dict_row)
filter_criteria_data_for_competition_and_session = {(11, 90), (11, 42), (11, 4), (2, 44)}
load_json_data_from_match(conn, '../open-data/data/matches', filter_criteria_data_for_competition_and_session)
