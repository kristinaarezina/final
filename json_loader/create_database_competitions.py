import json
import psycopg
from psycopg.rows import dict_row

def load_json_data_into_competition_season(json_file_path, filter_criteria=None):
    with open(json_file_path, 'r') as file:
        data = json.load(file)

    if filter_criteria:
        data = [item for item in data if (item['competition_id'], item['season_id']) in filter_criteria]

    with psycopg.connect("dbname=project_database user=postgres password=1234") as conn:
        with conn.cursor() as cur:
            for item in data:
                insert_query = """
                INSERT INTO CompetitionSeasons (competition_id, season_id, season_name, name, country_name, gender, 
                                                youth, match_updated, match_available)
                VALUES (%s, %s, %s,%s, %s, %s, %s, %s::TIMESTAMP, %s::TIMESTAMP)
                ON CONFLICT (competition_id, season_id) DO NOTHING;
                """
                match_updated = item.get('match_updated').replace('T', ' ') if item.get('match_updated') else None
                match_available = item.get('match_available').replace('T', ' ') if item.get('match_available') else None
                values_to_insert = (item['competition_id'], item['season_id'], item['season_name'], item['competition_name'],
                                    item['country_name'], item['competition_gender'], item['competition_youth'], 
                                    match_updated, match_available)
                cur.execute(insert_query, values_to_insert)

            conn.commit()

            cur.execute("SELECT * FROM CompetitionSeasons")
            records = cur.fetchall()
            for record in records:
                print(record)



if __name__ == "__main__":

    conn = psycopg.connect("dbname=project_database user=postgres password=1234", row_factory=dict_row)

    filter_criteria_data_for_competition_and_session = {(11, 90), (11, 42), (11, 4), (2, 44)}
    load_json_data_into_competition_season('../open-data/competitions.json', filter_criteria_data_for_competition_and_session)









