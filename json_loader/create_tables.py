import psycopg


def create_database():
    with psycopg.connect("user=postgres password=1234") as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("CREATE DATABASE project_database")
            print("Created database")

def create_a_table():
    command ="""
    CREATE TABLE IF NOT EXISTS Shots (
        event_id UUID PRIMARY KEY,
        freeze_frame JSONB,
        body_part_id INT,
        end_location_x INT,
        end_location_y INT,
        end_location_z INT,
        technique_id INT,
        first_time BOOLEAN,
        follows_dribble BOOLEAN,
        statsbomb_xg FLOAT,
        outcome_id INT,
        outcome_name VARCHAR(255),
        FOREIGN KEY (event_id) REFERENCES Events(event_id),
        FOREIGN KEY (body_part_id) REFERENCES BodyParts(body_part_id),
        FOREIGN KEY (technique_id) REFERENCES ShotTechniques(technique_id)
    );
    """

    conn = psycopg.connect("dbname=project_database user=postgres password=1234")
    conn.autocommit = True

    with conn.cursor() as cur:
        try:
            cur.execute(command)
            print("Table created successfully.")
        except (psycopg.errors.SyntaxError, Exception) as e:
            print(f"error: {e}")
            print(f"command: {command}")

    conn.close()

def drop_all_tables():
    drop_commands = (
        "DROP TABLE IF EXISTS HalfStart CASCADE;",
        "DROP TABLE IF EXISTS PassHeight CASCADE;",
        "DROP TABLE IF EXISTS PositionsLineup CASCADE;",
        "DROP TABLE IF EXISTS HalfEnd CASCADE;",
        "DROP TABLE IF EXISTS Cards CASCADE;",
        "DROP TABLE IF EXISTS InjuryStoppage CASCADE;",
        "DROP TABLE IF EXISTS Interceptions CASCADE;",
        "DROP TABLE IF EXISTS Clearance CASCADE;",
        "DROP TABLE IF EXISTS Carry CASCADE;",
        "DROP TABLE IF EXISTS Block CASCADE;",
        "DROP TABLE IF EXISTS BallRecovery CASCADE;",
        "DROP TABLE IF EXISTS BallReceipt CASCADE;",
        "DROP TABLE IF EXISTS Goalkeeper CASCADE;",
        "DROP TABLE IF EXISTS BadBehaviour CASCADE;",
        "DROP TABLE IF EXISTS FoulCommitted CASCADE;",
        "DROP TABLE IF EXISTS FivetyFivety CASCADE;",
        "DROP TABLE IF EXISTS Duel CASCADE;",
        "DROP TABLE IF EXISTS Substitution CASCADE;",
        "DROP TABLE IF EXISTS Passes CASCADE;",
        "DROP TABLE IF EXISTS Shots CASCADE;",
        "DROP TABLE IF EXISTS Dribbles CASCADE;",
        "DROP TABLE IF EXISTS FoulWon CASCADE;",
        "DROP TABLE IF EXISTS Pressure CASCADE;",
        "DROP TABLE IF EXISTS PlayerOff CASCADE;",
        "DROP TABLE IF EXISTS Events CASCADE;",
        "DROP TABLE IF EXISTS Players CASCADE;",
        "DROP TABLE IF EXISTS Matches CASCADE;",
        "DROP TABLE IF EXISTS CompetitionSeasons CASCADE;",
        "DROP TABLE IF EXISTS GoalkeeperPositions CASCADE;",
        "DROP TABLE IF EXISTS GoalkeeperTypes CASCADE;",
        "DROP TABLE IF EXISTS GoalkeeperTechniques CASCADE;",
        "DROP TABLE IF EXISTS GoalkeeperOutcomes CASCADE;",
        "DROP TABLE IF EXISTS PassHeight CASCADE;",
        "DROP TABLE IF EXISTS PassTechniques CASCADE;",
        "DROP TABLE IF EXISTS PassTypes CASCADE;",
        "DROP TABLE IF EXISTS Outcomes CASCADE;",
        "DROP TABLE IF EXISTS ShotTechniques CASCADE;",
        "DROP TABLE IF EXISTS BodyParts CASCADE;",
        "DROP TABLE IF EXISTS CardTypes CASCADE;",
        "DROP TABLE IF EXISTS Positions CASCADE;",
        "DROP TABLE IF EXISTS Teams CASCADE;",
        "DROP TABLE IF EXISTS Referees CASCADE;",
        "DROP TABLE IF EXISTS Stadiums CASCADE;",
        "DROP TABLE IF EXISTS Managers CASCADE;",
        "DROP TABLE IF EXISTS EventTypes CASCADE;",
        "DROP TABLE IF EXISTS CompetitionStages CASCADE;",
        "DROP TABLE IF EXISTS Lineup CASCADE;",
        "DROP TABLE IF EXISTS LineupDetails CASCADE;",
    )
    conn = psycopg.connect("dbname=project_database user=postgres password=1234")
    conn.autocommit = True
    with conn.cursor() as cur:
        for command in drop_commands:
            cur.execute(command)
    conn.close()


def create_tables():
    commands = (
    """
    CREATE TABLE IF NOT EXISTS CompetitionStages (
        stage_id INT PRIMARY KEY,
        name VARCHAR(255)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS EventTypes (
        type_id INT PRIMARY KEY,
        name VARCHAR(255)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Managers (
        manager_id INT PRIMARY KEY,
        manager_name VARCHAR(255),
        manager_nickname VARCHAR(255),
        manager_dob DATE,
        manager_country_name VARCHAR(255)
    );
    """,
     """
    CREATE TABLE IF NOT EXISTS Teams (
        team_id INT PRIMARY KEY,
        team_name VARCHAR(255),
        team_gender VARCHAR(50),
        manager_id INT,
        country_name VARCHAR(255),
        group_name VARCHAR(255),
        FOREIGN KEY (manager_id) REFERENCES Managers(manager_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Players (
        player_id INT PRIMARY KEY,
        player_name VARCHAR(255),
        nickname VARCHAR(255),
        team_id INT,
        country_name VARCHAR(255),
        jersey_number INT,
        FOREIGN KEY (team_id) REFERENCES Teams(team_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Positions (
        position_id INT PRIMARY KEY,
        name VARCHAR(255)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS PositionsLineup (
        position_id INT PRIMARY KEY,
        player_id INT,
        name VARCHAR(255),
        from_t VARCHAR(255),
        to_t VARCHAR(255),
        from_period INT,
        to_period INT,
        start_reason VARCHAR(255),
        end_reason VARCHAR(255),
        FOREIGN KEY (player_id) REFERENCES Players(player_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Stadiums (
        stadium_id INT PRIMARY KEY,
        name VARCHAR(255),
        country_name VARCHAR(255)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Referees (
        referee_id INT PRIMARY KEY,
        name VARCHAR(255),
        country_name VARCHAR(255)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS CardTypes (
        card_id INT PRIMARY KEY,
        card_name VARCHAR(255)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS BodyParts (
        body_part_id INT PRIMARY KEY,
        name VARCHAR(255)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS ShotTechniques (
        technique_id INT PRIMARY KEY,
        name VARCHAR(255)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS PassTypes (
        pass_type_id INT PRIMARY KEY,
        name VARCHAR(255)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS PassTechniques (
        pass_technique_id INT PRIMARY KEY,
        name VARCHAR(255)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS GoalkeeperOutcomes (
        goalkeeperOutcomes_id INT PRIMARY KEY,
        goalkeeperOutcomes_name VARCHAR(255)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS GoalkeeperTechniques (
        technique_id INT PRIMARY KEY,
        name VARCHAR(255)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS GoalkeeperTypes (
        goal_keep_type_id INT PRIMARY KEY,
        name VARCHAR(255)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS GoalkeeperPositions (
        position_id INT PRIMARY KEY,
        name VARCHAR(255)
    );
    """,
    """
        CREATE TABLE IF NOT EXISTS Lineup (
            team_id INT PRIMARY KEY,
            team_name VARCHAR(255),
            lineup JSON
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS LineupDetails (
            lineup_detail_id SERIAL PRIMARY KEY,
            lineup_id INT,
            player_id INT,
            position_id INT,
            FOREIGN KEY (lineup_id) REFERENCES Lineup(team_id),
            FOREIGN KEY (player_id) REFERENCES Players(player_id),
            FOREIGN KEY (position_id) REFERENCES Positions(position_id)
        );
        """,
    """
    CREATE TABLE IF NOT EXISTS CompetitionSeasons (
        competition_id INT,
        season_id INT,
        season_name VARCHAR(255),
        name VARCHAR(255),
        country_name VARCHAR(255),
        gender VARCHAR(50),
        youth BOOLEAN,
        match_updated TIMESTAMP,
        match_available TIMESTAMP,
        PRIMARY KEY (competition_id, season_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Matches (
        match_id INT PRIMARY KEY,
        season_id INT,
        competition_id INT,
        home_team_id INT,
        away_team_id INT,
        match_date DATE,
        kick_off TIME,
        stadium_id INT,
        referee_id INT,
        home_score INT,
        away_score INT,
        match_status VARCHAR(50),
        match_week INT,
        stage_id INT,
        last_updated TIMESTAMP,
        last_updated_360 TIMESTAMP,
        FOREIGN KEY (competition_id, season_id) REFERENCES CompetitionSeasons(competition_id, season_id),
        FOREIGN KEY (home_team_id) REFERENCES Teams(team_id),
        FOREIGN KEY (away_team_id) REFERENCES Teams(team_id),
        FOREIGN KEY (stadium_id) REFERENCES Stadiums(stadium_id),
        FOREIGN KEY (referee_id) REFERENCES Referees(referee_id),
        FOREIGN KEY (stage_id) REFERENCES CompetitionStages(stage_id)
    );
    """
    ,
    """
        CREATE TABLE IF NOT EXISTS PlayPattern (
        play_pattern_id INT,
        name VARCHAR(255),
        FOREIGN KEY (event_id) REFERENCES Events(event_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Events (
        event_id UUID PRIMARY KEY,
        index INT,
        match_id INT,
        team_id INT,
        tactics_formation INT,
        tactics_lineup JSONB,
        possession INT,
        possession_team_id INT,
        type_id INT,
        player_id INT,
        play_pattern_id INT,
        timestamp VARCHAR(255),
        minute INT,
        second INT,
        period INT,
        position_id INT,
        location VARCHAR(255),
        under_pressure BOOLEAN,
        duration DECIMAL,
        off_camera BOOLEAN,
        out BOOLEAN,
        related_events JSON,
        FOREIGN KEY (match_id) REFERENCES Matches(match_id),
        FOREIGN KEY (team_id) REFERENCES Teams(team_id),
        FOREIGN KEY (possession_team_id) REFERENCES Teams(team_id),
        FOREIGN KEY (type_id) REFERENCES EventTypes(type_id),
        FOREIGN KEY (player_id) REFERENCES Players(player_id),
        FOREIGN KEY (position_id) REFERENCES Positions(position_id),
        FOREIGN KEY (play_pattern_id) REFERENCES PlayPattern(play_pattern_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS PlayerOff (
        event_id UUID PRIMARY KEY,
        permanent BOOLEAN,
        FOREIGN KEY (event_id) REFERENCES Events(event_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Pressure (
        event_id UUID PRIMARY KEY,
        counterpress BOOLEAN,
        FOREIGN KEY (event_id) REFERENCES Events(event_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS FoulWon (
        event_id UUID PRIMARY KEY,
        defensive BOOLEAN,
        advantage BOOLEAN,
        penalty BOOLEAN,
        FOREIGN KEY (event_id) REFERENCES Events(event_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Dribbles (
        event_id UUID PRIMARY KEY,
        outcome_id INT,
        outcome_name VARCHAR(255),
        overrun BOOLEAN,
        nutmeg BOOLEAN,
        no_touch BOOLEAN,
        FOREIGN KEY (event_id) REFERENCES Events(event_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Shots (
        event_id UUID PRIMARY KEY,
        freeze_frame JSONB,
        body_part_id INT,
        end_location_x INT,
        end_location_y INT,
        end_location_z INT,
        technique_id INT,
        first_time BOOLEAN,
        follows_dribble BOOLEAN,
        statsbomb_xg FLOAT,
        outcome_id INT,
        outcome_name VARCHAR(255),
        FOREIGN KEY (event_id) REFERENCES Events(event_id),
        FOREIGN KEY (body_part_id) REFERENCES BodyParts(body_part_id),
        FOREIGN KEY (technique_id) REFERENCES ShotTechniques(technique_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Passes (
        event_id UUID PRIMARY KEY,
        recipient_id INT,
        length DECIMAL,
        angle DECIMAL,
        height_id INT,
        height_name VARCHAR(255),
        end_location_x INT,
        end_location_y INT,
        end_location_z INT,
        assisted_shot_id UUID,
        backheel BOOLEAN,
        deflected BOOLEAN,
        miscommunication BOOLEAN,
        "cross" BOOLEAN,
        cut_back BOOLEAN,
        switch BOOLEAN,
        through_ball BOOLEAN,
        shot_assist BOOLEAN,
        goal_assist BOOLEAN,
        body_part_id INT,
        pass_type_id INT,
        outcome_id INT,
        outcome_name VARCHAR(255),
        pass_technique_id INT,
        FOREIGN KEY (event_id) REFERENCES Events(event_id),
        FOREIGN KEY (recipient_id) REFERENCES Players(player_id),
        FOREIGN KEY (body_part_id) REFERENCES BodyParts(body_part_id),
        FOREIGN KEY (pass_type_id) REFERENCES PassTypes(pass_type_id),
        FOREIGN KEY (pass_technique_id) REFERENCES PassTechniques(pass_technique_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Substitution (
        event_id UUID PRIMARY KEY,
        outcome_id INT,
        outcome_name VARCHAR(255),
        replacement_id INT,
        replacement_name VARCHAR(255),
        FOREIGN KEY (event_id) REFERENCES Events(event_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Duel (
        event_id UUID PRIMARY KEY,
        counterpress BOOLEAN,
        type_id INT,
        type_name VARCHAR(255),
        outcome_id INT,
        outcome_name VARCHAR(255),
        FOREIGN KEY (event_id) REFERENCES Events(event_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS FivetyFivety (
        event_id UUID PRIMARY KEY,
        outcome_id INT,
        outcome_name VARCHAR(255),
        counterpress BOOLEAN,
        FOREIGN KEY (event_id) REFERENCES Events(event_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS FoulCommitted (
        event_id UUID PRIMARY KEY,
        counterpress BOOLEAN,
        offensive BOOLEAN,
        type_id INT,
        type_name VARCHAR(255),
        advantage BOOLEAN,
        penalty BOOLEAN,
        card_id INT,
        FOREIGN KEY (event_id) REFERENCES Events(event_id),
        FOREIGN KEY (card_id) REFERENCES CardTypes(card_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS BadBehaviour (
        event_id UUID PRIMARY KEY,
        card_id INT,
        FOREIGN KEY (event_id) REFERENCES Events(event_id),
        FOREIGN KEY (card_id) REFERENCES CardTypes(card_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Goalkeeper (
        event_id UUID PRIMARY KEY,
        position_id INT,
        technique_id INT,
        goal_keep_type_id INT,
        goal_keep_outcome_id INT,
        body_part_id INT,
        FOREIGN KEY (event_id) REFERENCES Events(event_id),
        FOREIGN KEY (position_id) REFERENCES GoalkeeperPositions(position_id),
        FOREIGN KEY (technique_id) REFERENCES GoalkeeperTechniques(technique_id),
        FOREIGN KEY (goal_keep_type_id) REFERENCES GoalkeeperTypes(goal_keep_type_id),
        FOREIGN KEY (goal_keep_outcome_id) REFERENCES GoalkeeperOutcomes(goalkeeperOutcomes_id),
        FOREIGN KEY (body_part_id) REFERENCES BodyParts(body_part_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS BallReceipt (
        event_id UUID PRIMARY KEY,
        ball_receipt_outcome_id INT,
        ball_receipt_outcome_name VARCHAR(255),
        FOREIGN KEY (event_id) REFERENCES Events(event_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS BallRecovery (
        event_id UUID PRIMARY KEY,
        ball_recovery_failure BOOLEAN,
        ball_recovery_offensive BOOLEAN,
        FOREIGN KEY (event_id) REFERENCES Events(event_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Block (
        event_id UUID PRIMARY KEY,
        deflection BOOLEAN,
        offensive BOOLEAN,
        save_block BOOLEAN,
        counterpress BOOLEAN,
        FOREIGN KEY (event_id) REFERENCES Events(event_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Carry (
        event_id UUID PRIMARY KEY,
        end_location DOUBLE PRECISION[],
        FOREIGN KEY (event_id) REFERENCES Events(event_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Clearance (
        event_id UUID PRIMARY KEY,
        aerial_won BOOLEAN,
        body_part_id INT,
        FOREIGN KEY (event_id) REFERENCES Events(event_id),
        FOREIGN KEY (body_part_id) REFERENCES BodyParts(body_part_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS Interceptions (
        event_id UUID PRIMARY KEY,
        outcome_id INT,
        outcome_id_name VARCHAR(255),
        FOREIGN KEY (event_id) REFERENCES Events(event_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS InjuryStoppage (
        event_id UUID PRIMARY KEY,
        in_chain BOOLEAN,
        FOREIGN KEY (event_id) REFERENCES Events(event_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS HalfEnd (
        event_id UUID PRIMARY KEY,
        early_video_end BOOLEAN,
        match_suspended BOOLEAN,
        FOREIGN KEY (event_id) REFERENCES Events(event_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS HalfStart (
        event_id UUID PRIMARY KEY,
        late_video_start BOOLEAN,
        FOREIGN KEY (event_id) REFERENCES Events(event_id)
    );
    """
    )
    conn = psycopg.connect("dbname=project_database user=postgres password=1234")
    conn.autocommit = True
    with conn.cursor() as cur:
        for command in commands:
            try:
                cur.execute(command)
            except psycopg.errors.SyntaxError as e:
                print(f"error: {e}")
                print(f"Command: {command}")
                break  
    conn.close()

def drop_a_tables():
    drop_commands = (
        """
        DROP TABLE IF EXISTS DribbledPast CASCADE;
        """,
    )
    conn = psycopg.connect("dbname=project_database user=postgres password=1234")
    conn.autocommit = True
    with conn.cursor() as cur:
        for command in drop_commands:
            cur.execute(command)
    conn.close()


drop_all_tables()
create_tables()
# for tophatting 
# drop_a_tables()
# create_a_table()
    


