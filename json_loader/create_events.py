import json
import os
import psycopg
from psycopg.rows import dict_row

def load_json_data_from_events(conn, json_dir_path, x = 0):
    
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
                    event_data = json.load(file)
                    for event in event_data:

                        match_id = int(os.path.splitext(os.path.basename(file_path))[0])

                        cur.execute("""
                            INSERT INTO Matches (
                                match_id
                            ) 
                            VALUES (%s)
                            ON CONFLICT DO NOTHING;
                        """, (match_id,)) 

                        team_id = event.get('team', {}).get('id', None)
                        cur.execute("""
                            INSERT INTO Teams (
                                team_id
                            ) 
                            VALUES (%s)
                            ON CONFLICT DO NOTHING;
                        """, (
                            team_id,
                        ))

                        possession_team_id = event.get('possession_team', {}).get('id', None)
                        cur.execute("""
                            INSERT INTO Teams (
                                team_id
                            ) 
                            VALUES (%s)
                            ON CONFLICT DO NOTHING;
                        """, (
                            possession_team_id,
                        ))

                        # event type
                        type_id = event.get('type', {}).get('id', None)
                        EventTypes_name = event.get('type', {}).get('name', None)
                        cur.execute("""
                            INSERT INTO EventTypes (
                                type_id, name
                            ) 
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING;
                        """, (
                            type_id, EventTypes_name
                        ))

                        # players data
                        player_id = event.get('player', {}).get('id', None)
                        player_name = event.get('player', {}).get('name', None)
                        if player_id is not None: 
                            cur.execute("""
                                INSERT INTO Players (
                                    player_id, player_name
                                ) 
                                VALUES (%s, %s)
                                ON CONFLICT DO NOTHING;
                            """, (
                                player_id, player_name
                            ))

                        # positions data
                        position_id = event.get('position', {}).get('id', None)
                        position_name = event.get('position', {}).get('name', None)
                        if position_id is not None:
                            cur.execute("""
                                INSERT INTO Positions (
                                    position_id, name
                                ) 
                                VALUES (%s, %s)
                                ON CONFLICT DO NOTHING;
                            """, (
                                position_id, position_name
                            ))

                        # play pattern data
                        play_pattern_id = event.get('play_pattern', {}).get('id', None)
                        play_pattern_name = event.get('play_pattern', {}).get('name', None)
                        cur.execute("""
                            INSERT INTO PlayPattern (
                                play_pattern_id, name
                            ) 
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING;
                        """, (
                            play_pattern_id, play_pattern_name
                        ))


                        # all event data
                        event_id = event.get('id',None)
                        index = event.get('index',None)
                        team_id = event.get('team', {}).get('id')
                        possession = event.get('possession',None)
                        possession_team_id = event.get('possession_team', {}).get('id', None)
                        type_id = event.get('type', {}).get('id', None)
                        player_id = event.get('player',{}).get('id', None)
                        play_pattern_id = event.get('play_pattern',{}).get('id', None)
                        timestamp = event.get('timestamp', None)
                        minute = event.get('minute',None)
                        second = event.get('second',None)
                        period = event.get('period',None)
                        position_id = event.get('position',{}).get('id', None)
                        location = str(event.get('location'))
                        under_pressure = event.get('under_pressure',None)
                        duration = event.get('duration',None)
                        off_camera = event.get('off_camera',None)
                        out = event.get('out',None)
                        related_events = event.get('related_events',None)
                        related_events_serialized = json.dumps(related_events) if related_events is not None else None

                        cur.execute("""
                            INSERT INTO Events (
                                event_id, index, match_id,team_id, possession, possession_team_id, type_id, player_id, play_pattern_id, timestamp, minute, second, period, position_id, location, under_pressure, duration, off_camera, out, related_events
                            ) VALUES (
                                %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                            ON CONFLICT (event_id) DO NOTHING;
                        """, (
                            event_id, index, match_id, team_id, possession, possession_team_id, type_id, player_id, play_pattern_id, timestamp, minute, second, period, position_id, location, under_pressure, duration, off_camera, out, related_events_serialized
                        ))

                        # CardTypes
                        card_id = event.get('bad_behaviour',{}).get('card', {}).get("id")
                        card_name = event.get('bad_behaviour',{}).get('card', {}).get("name")
                        if card_id is not None:
                            cur.execute("""
                                INSERT INTO CardTypes (
                                    card_id, card_name
                                ) 
                                VALUES (%s, %s)
                                ON CONFLICT DO NOTHING;
                            """, (
                                card_id, card_name
                            ))

                        # BodyParts
                        body_part_id = event.get('pass',{}).get('body_part', {}).get("id")
                        name = event.get('pass',{}).get('body_part', {}).get("name")
                        if body_part_id is not None:
                            cur.execute("""
                                INSERT INTO BodyParts (
                                    body_part_id, name
                                ) 
                                VALUES (%s, %s)
                                ON CONFLICT DO NOTHING;
                            """, (
                                body_part_id, name
                            ))

                        # ShotTechniques
                        technique_id = event.get('shot',{}).get('technique', {}).get("id")
                        name = event.get('shot',{}).get('technique', {}).get("name")
                        if technique_id is not None:
                            cur.execute("""
                                INSERT INTO ShotTechniques (
                                    technique_id, name
                                ) 
                                VALUES (%s, %s)
                                ON CONFLICT DO NOTHING;
                            """, (
                                technique_id, name
                            ))

                        # PassTypes
                        pass_type_id = event.get('pass',{}).get('type', {}).get("id")
                        name = event.get('pass',{}).get('type', {}).get("name")
                        if pass_type_id is not None:
                            cur.execute("""
                                INSERT INTO PassTypes (
                                    pass_type_id, name
                                ) 
                                VALUES (%s, %s)
                                ON CONFLICT DO NOTHING;
                            """, (
                                pass_type_id, name
                            ))

                        # PassTechniques
                        pass_technique_id = event.get('pass',{}).get('technique', {}).get("id")
                        name = event.get('pass',{}).get('technique', {}).get("name")
                        if pass_technique_id is not None:
                            cur.execute("""
                                INSERT INTO PassTechniques (
                                    pass_technique_id, name
                                ) 
                                VALUES (%s, %s)
                                ON CONFLICT DO NOTHING;
                            """, (
                                pass_technique_id, name
                            ))

                        # GoalkeeperOutcomes
                        goalkeeperOutcomes_id = event.get('goalkeeper',{}).get('outcome', {}).get("id")
                        goalkeeperOutcomes_name = event.get('goalkeeper',{}).get('outcome', {}).get("name")
                        if goalkeeperOutcomes_id is not None:
                            cur.execute("""
                                INSERT INTO GoalkeeperOutcomes (
                                    goalkeeperOutcomes_id, goalkeeperOutcomes_name
                                ) 
                                VALUES (%s, %s)
                                ON CONFLICT DO NOTHING;
                            """, (
                                goalkeeperOutcomes_id, goalkeeperOutcomes_name
                            ))


                        # GoalkeeperTypes
                        goal_keep_type_id = event.get('goalkeeper',{}).get('type', {}).get("id")
                        name = event.get('goalkeeper',{}).get('type', {}).get("name")
                        if goal_keep_type_id is not None:
                            cur.execute("""
                                INSERT INTO GoalkeeperTypes (
                                    goal_keep_type_id, name
                                ) 
                                VALUES (%s, %s)
                                ON CONFLICT DO NOTHING;
                            """, (
                                goal_keep_type_id, name
                            ))

                        # GoalkeeperPositions
                        position_id = event.get('goalkeeper',{}).get('position', {}).get("id")
                        name = event.get('goalkeeper',{}).get('position', {}).get("name")
                        if position_id is not None:
                            cur.execute("""
                                INSERT INTO GoalkeeperPositions (
                                    position_id, name
                                ) 
                                VALUES (%s, %s)
                                ON CONFLICT DO NOTHING;
                            """, (
                                position_id, name
                            ))

                        # GoalkeeperTechniques
                        technique_id = event.get('goalkeeper',{}).get('technique', {}).get("id")
                        name = event.get('goalkeeper',{}).get('technique', {}).get("name")
                        if technique_id is not None:
                            cur.execute("""
                                INSERT INTO GoalkeeperTechniques (
                                    technique_id, name
                                ) 
                                VALUES (%s, %s)
                                ON CONFLICT DO NOTHING;
                            """, (
                                technique_id, name
                            ))

                        # BodyParts
                        body_part_id = event.get('goalkeeper',{}).get('body_part', {}).get("id")
                        name = event.get('goalkeeper',{}).get('body_part', {}).get("name")
                        if body_part_id is not None:
                            cur.execute("""
                                INSERT INTO BodyParts (
                                    body_part_id, name
                                ) 
                                VALUES (%s, %s)
                                ON CONFLICT DO NOTHING;
                            """, (
                                body_part_id, name
                            ))
                        
                        # Goalkeeper
                        if EventTypes_name == "Goal Keeper":
                            position_id =   event.get('goalkeeper',{}).get('position', {}).get('id', None)
                            technique_id =   event.get('goalkeeper',{}).get('technique', {}).get('id', None)
                            goal_keep_type_id =   event.get('goalkeeper',{}).get('type', {}).get('id', None)
                            goal_keep_outcome_id =   event.get('goalkeeper',{}).get('outcome', {}).get('id', None)
                            body_part_id =   event.get('goalkeeper',{}).get('body_part', {}).get('id', None)
                            cur.execute("""
                                INSERT INTO Goalkeeper (
                                    event_id, position_id, technique_id, goal_keep_type_id, goal_keep_outcome_id, body_part_id
                                ) 
                                VALUES (%s, %s, %s, %s, %s, %s)
                                ON CONFLICT (event_id) DO NOTHING;
                            """, (
                                event_id, position_id, technique_id, goal_keep_type_id, goal_keep_outcome_id, body_part_id
                            ))

                        # PlayerOff
                        if EventTypes_name == "Player Off":
                            permanent = event.get('player_off',{}).get('permanent', None)
                            cur.execute("""
                                INSERT INTO PlayerOff (
                                    event_id, permanent
                                ) 
                                VALUES (%s, %s)
                                ON CONFLICT DO NOTHING;
                            """, (
                                event_id, permanent
                            ))

                        # Pressure
                        if EventTypes_name == "Pressure":
                            counterpress = event.get('pressure',{}).get('counterpress', None)
                            cur.execute("""
                                INSERT INTO Pressure (
                                    event_id, counterpress
                                ) 
                                VALUES (%s, %s)
                                ON CONFLICT DO NOTHING;
                            """, (
                                event_id, counterpress
                            ))

                        # FoulWon
                        if EventTypes_name == "Foul Won":
                            defensive = event.get('foul_won',{}).get('defensive', None)
                            advantage = event.get('foul_won',{}).get('advantage', None)
                            penalty = event.get('foul_won',{}).get('penalty', None)
                            cur.execute("""
                                INSERT INTO FoulWon (
                                    event_id, defensive, advantage, penalty
                                ) 
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT DO NOTHING;
                            """, (
                                event_id, defensive, advantage, penalty
                            ))

                        # Dribbles
                        if EventTypes_name == "Dribble":
                            outcome_id = event.get('dribble',{}).get('outcome', {}).get("id")
                            outcome_name = event.get('dribble',{}).get('outcome', {}).get("name")
                            overrun = event.get('dribble',{}).get('overrun', None)
                            nutmeg = event.get('dribble',{}).get('nutmeg', None)
                            no_touch = event.get('dribble',{}).get('no_touch', None)
                            cur.execute("""
                                INSERT INTO Dribbles (
                                    event_id, outcome_id, outcome_name, overrun, nutmeg, no_touch 
                                ) 
                                VALUES (%s, %s,%s, %s,%s, %s)
                                ON CONFLICT DO NOTHING;
                            """, (
                                event_id, outcome_id, outcome_name, overrun, nutmeg, no_touch
                            ))

                        # Shots
                        if EventTypes_name == "Shot":
                            outcome_id = event.get('shot',{}).get('outcome', {}).get("id")
                            freeze_frame = event.get('shot',{}).get('freeze_frame', {})
                            freeze_frame_json = json.dumps(freeze_frame) if freeze_frame else None
                            body_part_id = event.get('shot',{}).get('body_part',{}).get('id', None)
                            technique_id = event.get('shot',{}).get('technique',{}).get('id', None)
                            first_time = event.get('shot',{}).get('first_time', None)
                            follows_dribble = event.get('shot',{}).get('follows_dribble', None)
                            statsbomb_xg =  event.get('shot',{}).get('statsbomb_xg', None)
                            outcome_id = event.get('shot',{}).get('outcome',{}).get('id', None)
                            outcome_name = event.get('shot',{}).get('outcome',{}).get('name', None)
                            end_location = event.get('shot', {}).get('end_location', None)
                            if end_location is not None:
                                end_location_x = end_location[0]
                                end_location_y = end_location[1]
                                if len(end_location) == 3:
                                    end_location_z = end_location[2]
                                else:
                                    end_location_z = None
                            else:
                                end_location_x = None 
                                end_location_y = None
                                end_location_z = None

                            cur.execute("""
                                INSERT INTO Shots (
                                    event_id, freeze_frame, body_part_id, end_location_x, end_location_y, end_location_z, technique_id, first_time, follows_dribble, statsbomb_xg, outcome_id,outcome_name  
                                ) 
                                VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s,%s,%s,%s)
                                ON CONFLICT DO NOTHING;
                            """, (
                                event_id, freeze_frame_json, body_part_id, end_location_x, end_location_y, end_location_z, technique_id, first_time, follows_dribble, statsbomb_xg, outcome_id,outcome_name  
                            ))

                        if EventTypes_name == "Pass":
                            # Passes
                            recipient_id = event.get('pass', {}).get('recipient', {}).get("id", None)
                            player_name = event.get('pass', {}).get('recipient', {}).get("name", None)
                            if recipient_id is not None: 
                                cur.execute("""
                                    INSERT INTO Players (
                                        player_id, player_name
                                    ) 
                                    VALUES (%s,%s)
                                    ON CONFLICT DO NOTHING;
                                """, (
                                    recipient_id, player_name
                                ))

                            length = event.get('pass', {}).get('length', None)
                            angle = event.get('pass', {}).get('angle', None)
                            height_id = event.get('pass', {}).get('height_', {}).get('id', None)
                            end_location = event.get('pass', {}).get('end_location', None)
                            if end_location is not None:
                                end_location_x = end_location[0]
                                end_location_y = end_location[1]
                            else:
                                end_location_x = None 
                                end_location_y = None
                            height_id = event.get('pass',{}).get('height', {}).get("id")
                            height_name = event.get('pass',{}).get('height', {}).get("name")
                            assisted_shot_id = event.get('pass',{}).get('assisted_shot_id', None)
                            backheel =  event.get('pass',{}).get('backheel', None)
                            deflected = event.get('pass',{}).get('deflected',None)
                            miscommunication = event.get('pass',{}).get('miscommunication',None)
                            cross = event.get('pass',{}).get('cross', None)
                            cut_back = event.get('pass', {}).get('cut_back', None)
                            switch = event.get('pass',{}).get('switch',None)
                            through_ball = event.get('pass',{}).get('through_ball',None)
                            shot_assist = event.get('pass',{}).get('shot_assist', None)
                            goal_assist = event.get('pass',{}).get('goal_assist', None)
                            body_part_id =  event.get('pass',{}).get('body_part', {}).get('id', None)
                            pass_type_id = event.get('pass',{}).get('type',{}).get('id', None)
                            outcome_id = event.get('pass',{}).get('outcome',{}).get('id', None)
                            outcome_name =  event.get('pass',{}).get('outcome', {}).get('name', None)
                            pass_technique_id = event.get('pass',{}).get('technique',{}).get('id', None)

                            cur.execute("""
                                INSERT INTO Passes (
                                    event_id, recipient_id, length, angle, height_id,height_name, end_location_x, end_location_y, assisted_shot_id,
                                    backheel, deflected, miscommunication, "cross", cut_back, switch, through_ball, shot_assist,
                                    goal_assist, body_part_id, pass_type_id, outcome_id, outcome_name, pass_technique_id
                                ) 
                                VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (event_id) DO NOTHING;
                            """, (
                                event_id, recipient_id, length, angle, height_id,height_name, end_location_x, end_location_y, assisted_shot_id,
                                backheel, deflected, miscommunication, cross, cut_back, switch, through_ball, shot_assist,
                                goal_assist, body_part_id, pass_type_id, outcome_id, outcome_name, pass_technique_id
                            ))

                        # BREAKKK
                        if EventTypes_name == "Substitution":
                            outcome_id =  event.get('substitution',{}).get('outcome', {}).get('id', None)
                            outcome_name =  event.get('substitution',{}).get('outcome', {}).get('name', None)
                            replacement_id =  event.get('substitution',{}).get('replacement', {}).get('id', None)
                            replacement_name =  event.get('substitution',{}).get('replacement', {}).get('name', None)
                            # Substitution
                            cur.execute("""
                                INSERT INTO Substitution (
                                    event_id, outcome_id, outcome_name, replacement_id, replacement_name
                                ) 
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (event_id) DO NOTHING;
                            """, (
                                event_id, outcome_id, outcome_name, replacement_id, replacement_name
                            ))

                        if EventTypes_name == "Duel":
                            counterpress =  event.get('duel',{}).get('counterpress', None)
                            type_id =  event.get('duel',{}).get('type',{}).get('id', None)
                            type_name =  event.get('duel',{}).get('type',{}).get('name', None)
                            outcome_id =   event.get('duel',{}).get('outcome',{}).get('id', None)
                            outcome_name =   event.get('duel',{}).get('outcome',{}).get('name', None)
                            # Duel
                            cur.execute("""
                                INSERT INTO Duel (
                                    event_id, counterpress, type_id, type_name, outcome_id, outcome_name
                                ) 
                                VALUES (%s, %s, %s, %s, %s, %s)
                                ON CONFLICT (event_id) DO NOTHING;
                            """, (
                                event_id, counterpress, type_id, type_name, outcome_id, outcome_name
                            ))

                        if EventTypes_name == "FivetyFivety":
                            counterpress = event.get('50_50', {}).get('counterpress', None)
                            outcome_id = event.get('50_50', {}).get('outcome', {}).get('id', None)  
                            outcome_name = event.get('50_50', {}).get('outcome', {}).get('name', None) 

                            cur.execute("""
                                INSERT INTO FivetyFivety (
                                    event_id, outcome_id, outcome_name, counterpress
                                ) 
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (event_id) DO NOTHING;
                            """, (
                                event_id, outcome_id, outcome_name, counterpress
                            ))

                        if EventTypes_name == "Foul Committed":
                            counterpress = event.get('foul_committed', {}).get('counterpress', None)
                            offensive = event.get('foul_committed', {}).get('offensive', None)
                            type_id = event.get('foul_committed', {}).get('type', {}).get('id', None)
                            type_name = event.get('foul_committed', {}).get('type', {}).get('name', None)
                            advantage = event.get('foul_committed', {}).get('advantage', None)
                            penalty = event.get('foul_committed', {}).get('penalty', None)
                            card_id = event.get('foul_committed', {}).get('card', {}).get('id', None)  # Changed {} to None
                            card_name = event.get('foul_committed', {}).get('card', {}).get('name', None)
                            if card_id:
                                cur.execute("""
                                    INSERT INTO CardTypes (
                                        card_id,card_name
                                    ) 
                                    VALUES (%s,%s)
                                    ON CONFLICT (card_id) DO NOTHING;
                                """, (
                                    card_id, card_name
                                ))


                            cur.execute("""
                                INSERT INTO FoulCommitted (
                                    event_id, counterpress, offensive, type_id, type_name, advantage, penalty, card_id
                                ) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (event_id) DO NOTHING;
                            """, (
                                event_id, counterpress, offensive, type_id, type_name, advantage, penalty, card_id
                            ))


                        # card_id =  event.get('50_50',{}).get('counterpress', None)
                        if EventTypes_name == "Bad Behaviour":
                            # BadBehaviour
                            card_id = event.get('bad_behaviour', {}).get('card', {}).get('id', None)  # Changed {} to None
                            card_name = event.get('bad_behaviour', {}).get('card', {}).get('name', None)  # Changed {} to None

                            cur.execute("""
                                INSERT INTO BadBehaviour (
                                    event_id, card_id
                                ) 
                                VALUES (%s, %s)
                                ON CONFLICT (event_id) DO NOTHING;
                            """, (
                                event_id, card_id
                            ))

                        
                        if EventTypes_name == "Ball Receipt":
                            # BallReceipt
                            ball_receipt_outcome_id =  event.get('ball_receipt',{}).get('outcome', {}).get('id', None)
                            ball_receipt_outcome_name =   event.get('ball_receipt',{}).get('outcome', {}).get('name', None)
                            cur.execute("""
                                INSERT INTO BallReceipt (
                                    event_id, ball_receipt_outcome_id, ball_receipt_outcome_name
                                ) 
                                VALUES (%s, %s, %s)
                                ON CONFLICT (event_id) DO NOTHING;
                            """, (
                                event_id, ball_receipt_outcome_id, ball_receipt_outcome_name
                            ))

                        if EventTypes_name == "Ball Recovery":
                            # BallRecovery
                            ball_recovery_failure =   event.get('ball_recovery',{}).get('recovery_failure', None)
                            ball_recovery_offensive =   event.get('ball_recovery',{}).get('offensive', None)
                            cur.execute("""
                                INSERT INTO BallRecovery (
                                    event_id, ball_recovery_failure, ball_recovery_offensive
                                ) 
                                VALUES (%s, %s, %s)
                                ON CONFLICT (event_id) DO NOTHING;
                            """, (
                                event_id, ball_recovery_failure, ball_recovery_offensive
                            ))

                        if EventTypes_name == "Block":
                            # Block
                            deflection =   event.get('block',{}).get('deflection', None)
                            offensive =   event.get('block',{}).get('offensive', None)
                            save_block =   event.get('ball_receipt',{}).get('save_block', None)
                            counterpress =   event.get('ball_receipt',{}).get('counterpress', None)
                            cur.execute("""
                                INSERT INTO Block (
                                    event_id, deflection, offensive, save_block, counterpress
                                ) 
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (event_id) DO NOTHING;
                            """, (
                                event_id, deflection, offensive, save_block, counterpress
                            ))

                        if EventTypes_name == "Carry":
                            # Carry
                            end_location =   event.get('carry',{}).get('end_location', None)
                            cur.execute("""
                                INSERT INTO Carry (
                                    event_id, end_location
                                ) 
                                VALUES (%s, %s)
                                ON CONFLICT (event_id) DO NOTHING;
                            """, (
                                event_id, end_location
                            ))

                        if EventTypes_name == "Clearance":
                            body_part_id = event.get('clearance',{}).get('body_part', {}).get("id")
                            name = event.get('clearance',{}).get('body_part', {}).get("name")
                            if body_part_id is not None:
                                cur.execute("""
                                    INSERT INTO BodyParts (
                                        body_part_id, name
                                    ) 
                                    VALUES (%s, %s)
                                    ON CONFLICT DO NOTHING;
                                """, (
                                    body_part_id, name
                                ))
                            # Clearance
                            aerial_won =  event.get('clearance',{}).get('aerial_won', None)
                            cur.execute("""
                                INSERT INTO Clearance (
                                    event_id, aerial_won, body_part_id
                                ) 
                                VALUES (%s, %s, %s)
                                ON CONFLICT (event_id) DO NOTHING;
                            """, (
                                event_id, aerial_won, body_part_id
                            ))

                        if EventTypes_name == "Interception":
                            # Interceptions
                            outcome_id =  event.get('interception',{}).get('outcome', {}).get('id')
                            outcome_id_name =  event.get('interception',{}).get('outcome', {}).get('name')
                            cur.execute("""
                                INSERT INTO Interceptions (
                                    event_id, outcome_id, outcome_id_name
                                ) 
                                VALUES (%s, %s, %s)
                                ON CONFLICT (event_id) DO NOTHING;
                            """, (
                                event_id, outcome_id, outcome_id_name
                            ))

                        if EventTypes_name == "Injury Stoppage":
                            # InjuryStoppage
                            in_chain =  event.get('injury_stoppage',{}).get('in_chain', None)
                            cur.execute("""
                                INSERT INTO InjuryStoppage (
                                    event_id, in_chain
                                ) 
                                VALUES (%s, %s)
                                ON CONFLICT (event_id) DO NOTHING;
                            """, (
                                event_id, in_chain
                            ))

                        if EventTypes_name == "Half End":
                            # HalfEnd
                            early_video_end =  event.get('half_end',{}).get('early_video_end', None)
                            match_suspended =  event.get('half_end',{}).get('match_suspended', None)
                            cur.execute("""
                                INSERT INTO HalfEnd (
                                    event_id, early_video_end, match_suspended
                                ) 
                                VALUES (%s, %s, %s)
                                ON CONFLICT (event_id) DO NOTHING;
                            """, (
                                event_id, early_video_end, match_suspended
                            ))

                        # HalfStart
                        if EventTypes_name == "Half Start":
                            late_video_start =  event.get('half_start',{}).get('late_video_start', None)
                            cur.execute("""
                                INSERT INTO HalfStart (
                                    event_id, late_video_start
                                ) 
                                VALUES (%s, %s)
                                ON CONFLICT (event_id) DO NOTHING;
                            """, (
                                event_id, late_video_start
                            ))
        conn.commit()


conn = psycopg.connect("dbname=project_database user=postgres password=1234", row_factory=dict_row)
load_json_data_from_events(conn, '../open-data/data/events')  