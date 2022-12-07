import cfbd
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

#Configure API
config = cfbd.Configuration()
config.api_key['Authorization'] = '8om4vZLNAnCGJAIgUpd8ssn1to1r79RduXsVvdazw/z9xWMq+cfI3AR88yCVFkwj'
config.api_key_prefix['Authorization'] = 'Bearer'
api_config = cfbd.ApiClient(config)

#Connecting to games API and getting game data
games_api = cfbd.GamesApi(api_config)
game_results_df = pd.DataFrame()

for i in range(2015, 2022):
    game_results_temp = games_api.get_games(year = i)
    game_results_df_temp = pd.DataFrame.from_records([dict(game_id = game.id \
                                                          , season = game.season \
                                                          , week = game.week \
                                                          , conference_game = game.conference_game \
                                                          , excitement_index = game.excitement_index \
                                                          , attendance = game.attendance  \
                                                          , neutral_site = game.neutral_site \
                                                          , away_conference = game.away_conference \
                                                          , away_division = game.away_division \
                                                          , away_id = game.away_id \
                                                          , away_points = game.away_points \
                                                          , away_post_win_prob = game.away_post_win_prob \
                                                          , away_postgame_elo = game.away_postgame_elo \
                                                          , away_pregame_elo = game.away_pregame_elo \
                                                          , away_team = game.away_team \
                                                          , home_conference = game.home_conference \
                                                          , home_division = game.home_division \
                                                          , home_id = game.home_id \
                                                          , home_points = game.home_points \
                                                          , home_post_win_prob = game.home_post_win_prob \
                                                          , home_postgame_elo = game.home_postgame_elo \
                                                          , home_pregame_elo = game.home_pregame_elo \
                                                          , home_team = game.home_team) \
                                                          for game in game_results_temp])
    game_results_df = pd.concat([game_results_df, game_results_df_temp.dropna(axis=0)], axis=0)
    

#Connecting to advanced stats API and getting games stats
stats_api = cfbd.StatsApi(api_config)
adv_stats_df = pd.DataFrame()

for i in range(2015,2022):
    adv_stats_temp = stats_api.get_advanced_team_game_stats(year = i)
    adv_stats_df_temp = pd.DataFrame.from_records([dict(game_id = adv.game_id \
                                                        , team = adv.team \
                                                        , opponent = adv.opponent \
                                                        , week = adv.week \
                                                        , offense = adv.offense.to_dict() \
                                                        , defense = adv.defense.to_dict()) \
                                                        for adv in adv_stats_temp])
    adv_stats_df = pd.concat([adv_stats_df, adv_stats_df_temp.dropna(axis = 0)], axis = 0)
    
#Fixing offense and defense columns
offense_temp = adv_stats_df['offense'].apply(pd.Series)
offense_temp.drop(['standard_downs', 'passing_downs', 'rushing_plays', 'passing_plays'], axis = 1, inplace = True)
offense_temp.columns = ['offense_' + offcol for offcol in offense_temp.columns]

defense_temp = adv_stats_df['defense'].apply(pd.Series)
defense_temp.drop(['standard_downs', 'passing_downs', 'rushing_plays', 'passing_plays'], axis = 1, inplace = True)
defense_temp.columns = ['defense_' + defcol for defcol in defense_temp.columns]

#Dropping original offense and defense columns from adv_stats_df and rejoining
adv_stats_df_final = pd.concat([adv_stats_df, offense_temp, defense_temp], axis = 1)
adv_stats_df_final.drop(['offense', 'defense'], axis = 1, inplace = True)
adv_stats_df_final.dropna(axis = 0, inplace = True)

#Connecting to betting API and getting lines
betting_api = cfbd.BettingApi(api_config)
betting_df = pd.DataFrame()

#Setting book to pull from
book = 'consensus'

for i in range(2015, 2022):
        betting_temp = betting_api.get_lines(year = i)
        betting_df_temp = pd.DataFrame.from_records([dict(game_id = bet.id \
                                                       , season = bet.season \
                                                       , week = bet.week \
                                                       , home_team = bet.home_team \
                                                       , home_score = bet.home_score \
                                                       , away_team = bet.away_team \
                                                       , lines = bet.lines) \
                                                       for bet in betting_temp if bet.lines != []])
        for index, row in betting_df_temp.iterrows():
            line_list = [record.to_dict() for record in row['lines']]
            try:
                betting_df_temp.at[index, 'lines'] = [line for line in line_list if book in list(line.values())][0]
            except IndexError:
                betting_df_temp.at[index, 'lines'] = None
        betting_df = pd.concat([betting_df, betting_df_temp.dropna(axis = 0)], axis = 0)

#Fixing lines columns of betting df and creating final betting df
lines_df = betting_df['lines'].apply(pd.Series)

betting_df_final= pd.concat([betting_df.drop('lines', axis = 1), lines_df], axis = 1)

#CREATING FINAL DATAFRAMES
game_stats_df = game_results_df.merge(adv_stats_df_final \
                                    , how = 'inner'\
                                    , on = 'game_id').dropna(axis = 0)
game_stats_bets_df = game_stats_df.merge(betting_df_final \
                                       , how = 'inner' \
                                       , on = 'game_id')
game_stats_bets_df_clean = game_stats_bets_df.dropna(axis = 0)
