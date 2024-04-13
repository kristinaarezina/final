import json
import os
import psycopg
from psycopg.rows import dict_row

def load_lineups(conn, json_dir_path, x= 0):
    with conn.cursor() as cur:
        cur.execute('SELECT DISTINCT match_id FROM Matches')
        match_ids = cur.fetchall() 

        for match_id_tuple in match_ids:
            x += 1
            print(x)
            match_id = match_id_tuple['match_id'] 
            file_name = f"{match_id}.json" 
            file_path = os.path.join(json_dir_path, file_name)

            if os.path.exists(file_path):
                with open(file_path, 'r') as file:
                    lineup_data = json.load(file)
                    for team_lineup in lineup_data:
                        team_id = team_lineup.get('team_id', None)
                        team_name = team_lineup.get('team_name', None)
                        cur.execute("""
                            INSERT INTO Teams (
                                team_id, team_name
                            ) 
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING;
                        """, (
                            team_id, team_name
                        ))

                        team_id = team_lineup.get('team_id', None)
                        team_name = team_lineup.get('team_name', None)
                        lineup = team_lineup.get('lineup', None)
                        cur.execute("""
                            INSERT INTO Lineup (
                                team_id, team_name, lineup
                            ) 
                            VALUES (%s, %s, %s)
                            ON CONFLICT DO NOTHING;
                        """, (
                            team_id, team_name, json.dumps(lineup)
                        ))

                        for player in team_lineup.get('lineup', []):
                            player_id = player.get('player_id')
                            player_name = player.get('player_name')
                            player_nickname = player.get('player_nickname')
                            jersey_number = player.get('jersey_number')
                            country_name = player.get('country', {}).get('name')  
                            
                            # Insert player
                            cur.execute("""
                                INSERT INTO Players (
                                    player_id, player_name, nickname, team_id, country_name, jersey_number
                                ) 
                                VALUES (%s, %s, %s, %s, %s, %s)
                                ON CONFLICT (player_id) DO NOTHING;
                            """, (
                                player_id, player_name, player_nickname, team_id, country_name, jersey_number
                            ))

                            for position in player.get('positions', []):
                                position_id = position.get('position_id')
                                name = position.get('position')
                                from_p = position.get('from')
                                to = position.get('to')
                                from_period = position.get('from_period')
                                to_period = position.get('to_period')
                                start_reason = position.get('start_reason')
                                end_reason = position.get('end_reason')
                                cur.execute("""
                                    INSERT INTO PositionsLineup (
                                        position_id, player_id, name, from_t, to_t, from_period, to_period, start_reason, end_reason
                                    ) 
                                    VALUES (%s, %s, %s,%s, %s, %s,%s,%s,%s)
                                    ON CONFLICT DO NOTHING;
                                """, (
                                    position_id, player_id, name, from_p, to, from_period, to_period, start_reason, end_reason
                                ))

    conn.commit()

conn = psycopg.connect("dbname=project_database user=postgres password=1234", row_factory=dict_row)
json_dir_path = '../open-data/data/lineups'
load_lineups(conn, json_dir_path)
