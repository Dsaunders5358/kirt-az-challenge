import pandas as pd
import requests
import csv
from sqlalchemy import create_engine
from src import sql_commands as sql


engine = create_engine('postgresql://root:password@localhost:5432/kirtstatsdata')

def create_staging_area():
    return pd.read_csv('https://docs.google.com/spreadsheets/d/1AGHKXAd4hPa12kvyjAr9WGb4Cm2ZsYNIhiEAj_Hu1Ss/export?format=csv&gid=0')

def add_champions_data(df):
    champions = df[['Champions']].rename(columns = {'Champions' : 'champion_name'})
    champions.to_sql('champion_list', engine, if_exists = 'append', index = False)

def add_skins_data(df):
    skins = df[['Skin Used?']].rename(columns = {'Skin Used?' : 'skin_name'}).drop_duplicates('skin_name')
    skins.to_sql('skin_list', engine, if_exists = 'append', index = False)

def generate_game_dataframes(df):
    games = df[['Champions',
                'Games Played',
                'Game 1 Score',
                'Game 1 Time',
                'Game 2 Score',
                'Game 2 Time',
                'Game 3 Score',
                'Game 3 Time',
                'Game 4 Score',
                'Game 4 Time',
                'Game 5 Score',
                'Game 5 Time',
                'Game 6 Score',
                'Game 6 Time',
                'Game 7 Score',
                'Game 7 Time']]
    games_data = generate_new_games_list(games)
    games_dataframe = pd.DataFrame(columns = ['champion_name', 'kills', 'deaths', 'assists', 'time_taken'])
    for games in games_data:
        games_dataframe = games_dataframe.append(games, ignore_index=True)
    games_dataframe.to_sql('game_list_staging', engine, if_exists='append', index = False)

def generate_new_games_list(df):
    new_list = []
    for i, row in df.iterrows():
        game_num = row['Games Played']
        champ_name = row['Champions']
        for i in range(1, game_num + 1):
            kda = row[f'Game {str(i)} Score'].split('/')
            time = row[f'Game {str(i)} Time']
            h, m, s = time.split(':')
            time = int(h) * 3600 + int(m) * 60 + int(s)
            new_row = {'champion_name' : champ_name, 'kills' : kda[0], 'deaths' : kda[1], 'assists' : kda [2], 'time_taken' : time}
            new_list.append(new_row)
    return new_list

def calculate_players_data(df):
    player_list = []
    players = df[['Solo or Pre','Games Played']]
    players = players.replace('Pre with Pogo', 'Pogo')
    for i, row in players.iterrows():
        game_num = row['Games Played']
        players = row['Solo or Pre'].split("/")
        for player in players:
            for i in range(0, game_num):
                if player == "Bo" or player == "Bobbi":
                    player = 'Bobby'
                player_dict = {'person_name' : player}
                player_list.append(player_dict)
    players_data = pd.DataFrame(player_list).drop_duplicates('person_name')
    players_data.to_sql('player_list', engine, if_exists='append', index = False)

def add_fb_and_played_with(df):
    df = df[['Champions', 'First Bloods', 'Solo or Pre']]
    df = df.rename(columns={'Champions' : 'champion_name', 'First Bloods' : 'first_bloods', 'Solo or Pre' : 'played_with'})
    # df['champion_id'] = ""
    df.to_sql('challenge_stats_staging', engine, if_exists='append', index = False)
    sql.add_data_to_challenge_staging()

staging_area = create_staging_area()
staging_area = staging_area.iloc[1:-1, :]
staging_area = staging_area[staging_area['Winning Score'].notna()]
staging_area[['Games Played', 'Total Kills', 'Total Deaths', 'Total Assists']] = staging_area[['Games Played', 'Total Kills', 'Total Deaths', 'Total Assists']].astype(int) 
sql.create_sql_database()
sql.apply_database_schema()
add_champions_data(staging_area)
add_skins_data(staging_area)
generate_game_dataframes(staging_area)
sql.add_ids_to_games_list()
sql.add_champion_id_to_challenge()
calculate_players_data(staging_area)
sql.add_static_values_to_staging()
add_fb_and_played_with(staging_area)

