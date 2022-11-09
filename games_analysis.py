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

for i in range(2012, 2022):
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
adv_stats_temp = stats_api.get_advanced_team_game_stats(year = 2021)

#Connecting to betting API and getting lines
betting_api = cfbd.BettingApi(api_config)
betting_temp = betting_api.get_lines(year = 2021)
