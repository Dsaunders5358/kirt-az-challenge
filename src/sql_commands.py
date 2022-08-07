import psycopg2

def create_sql_database():
    conn = psycopg2.connect("dbname=postgres user=root password=password")
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute('DROP DATABASE IF EXISTS kirtstatsdata')
    cur.execute('CREATE DATABASE kirtstatsdata')
    conn.commit()
    cur.close()
    conn.close()

def connect_to_database():
    return psycopg2.connect("dbname=kirtstatsdata user=root password=password")

def apply_database_schema():
    conn = connect_to_database()
    cur = conn.cursor()
    cur.execute(\
        """CREATE TABLE IF NOT EXISTS champion_list (
            champion_id SERIAL,
            champion_name VARCHAR(255),
            PRIMARY KEY (champion_id),
            UNIQUE (champion_name)
            )
            """
    )
    cur.execute(\
        """CREATE TABLE IF NOT EXISTS skin_list(
            skin_id SERIAL,
            skin_name VARCHAR(255),
            PRIMARY KEY (skin_id),
            UNIQUE (skin_name)
        )"""
    )
    # cur.execute(\
    #     """CREATE TABLE IF NOT EXISTS game_list(
    #         game_id SERIAL,
    #         champion_id INT,
    #         Kills INT,
    #         deaths INT,
    #         assists INT,
    #         time_taken TIME,
    #         PRIMARY KEY (game_id),
    #         CONSTRAINT fk_champion
    #             FOREIGN KEY(champion_id)
    #                 REFERENCES champion_list(champion_id)
    #     )"""
    # )
    cur.execute(\
        """CREATE TABLE IF NOT EXISTS game_list_staging(
            game_id SERIAL,
            champion_id INT,
            champion_name VARCHAR(255),
            Kills INT,
            deaths INT,
            assists INT,
            time_taken INT
        )"""
    )
    cur.execute(\
        """CREATE TABLE IF NOT EXISTS challenge_stats(
            champion_id INT,
            games_played INT,
            winning_gameid INT,
            first_bloods INT,
            sit_ups INT,
            total_kills INT,
            total_deaths INT,
            total_assists INT,
            played_with VARCHAR(255)
            )"""
        )
    cur.execute(\
        """CREATE TABLE IF NOT EXISTS challenge_stats_staging(
            champion_name VARCHAR(255),
            champion_id INT,
            first_bloods INT,
            played_with VARCHAR(255)
            )"""
        )    
    cur.execute(\
        """CREATE TABLE IF NOT EXISTS player_list(
            person_id SERIAL,
            person_name VARCHAR(255),
            PRIMARY KEY (person_id)
            )"""
        )
    conn.commit()
    cur.close()
    conn.close()

def add_ids_to_games_list():
    conn = connect_to_database()
    cur = conn.cursor()
    cur.execute(\
        """ UPDATE game_list_staging a
        SET champion_id = b.champion_id
        FROM champion_list b
        WHERE a.champion_name = b.champion_name
        """)
    cur.execute(\
        """ ALTER TABLE game_list_staging
        DROP COLUMN champion_name"""
        )
    cur.execute(\
        """ALTER TABLE game_list_staging
        RENAME TO game_list""")
    conn.commit()
    cur.close()
    conn.close()

def add_champion_id_to_challenge():
    conn = connect_to_database()
    cur=conn.cursor()
    cur.execute(\
        """INSERT INTO challenge_stats (champion_id)
        (SELECT champion_id FROM champion_list)"""
    )
    conn.commit()
    conn.close()

def add_static_values_to_staging():
    conn = connect_to_database()
    cur = conn.cursor()
    cur.execute(\
        """UPDATE challenge_stats
        SET games_played = 
        (select COUNT(champion_id) from game_list
        where game_list.champion_id = challenge_stats.champion_id)
        """)
    cur.execute(\
        """UPDATE challenge_stats
        SET winning_gameid = 
        (select MAX(game_id) from game_list
        where game_list.champion_id = challenge_stats.champion_id)
        """)
    cur.execute(\
        """UPDATE challenge_stats
        SET sit_ups = (games_played - 1) * 10
        """)
    cur.execute(\
        """UPDATE challenge_stats a
        SET total_kills = (select SUM(kills) from game_list b where a.champion_id = b.champion_id),
        total_deaths = (select SUM(deaths) from game_list b where a.champion_id = b.champion_id),
        total_assists = (select SUM(assists) from game_list b where a.champion_id = b.champion_id)""")      
    conn.commit()
    conn.close()

def drop_staging_table():
    conn = connect_to_database()
    cur = conn.cursor()
    cur.execute(\
        """DROP TABLE challenge_stats_staging""")
    conn.commit()
    conn.close()

def add_data_to_challenge_staging():
    conn = connect_to_database()
    cur = conn.cursor()
    # cur.execute(\
    #     """ALTER TABLE challenge_stats_staging
    #     ALTER COLUMN champion_name TYPE VARCHAR(255),
    #     ALTER COLUMN played_with TYPE VARCHAR(255),
    #     ALTER COLUMN first_bloods TYPE INT""")
    cur.execute(\
        """UPDATE challenge_stats_staging a
        SET champion_id = b.champion_id
        FROM champion_list b
        WHERE a.champion_name = b.champion_name""")
    cur.execute(\
        """UPDATE challenge_stats a
        SET first_bloods = b.first_bloods, played_with = b.played_with
        FROM challenge_stats_staging b
        WHERE a.champion_id = b.champion_id""")
    cur.execute(\
        """UPDATE challenge_stats
        SET played_with = 'Pogo'
        WHERE played_with = 'Pre with Pogo'""")
    conn.commit()
    conn.close()
    drop_staging_table()