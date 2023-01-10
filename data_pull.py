import cfbd
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

#Working directory for writing csv files
cwd = os.getcwd()

#Configure API
config = cfbd.Configuration()
config.api_key['Authorization'] = '8om4vZLNAnCGJAIgUpd8ssn1to1r79RduXsVvdazw/z9xWMq+cfI3AR88yCVFkwj'
config.api_key_prefix['Authorization'] = 'Bearer'
api_config = cfbd.ApiClient(config)

#Setting date ranges for data pull
start_year = 2015
end_year = 2023

#######
# TEAMS
#######

#Connecting to teams API and getting teams data
teams_api = cfbd.TeamsApi(api_config)
teams_df = pd.DataFrame()

#Call Teams API and get FBS Teams
teams_api = cfbd.TeamsApi(api_config)
teams = teams_api.get_fbs_teams()

#Put teams into dataframe and clean up columns
teams_df = pd.DataFrame.from_records([t.to_dict() for t in teams])
teams_df = pd.concat([teams_df.drop('location', axis = 1), teams_df['location'].apply(pd.Series)], axis = 1)
teams_df['logo1'] = [teams_df['logos'][i][0] for i in range(len(teams_df))]
teams_df['logo2'] = [teams_df['logos'][i][1] for i in range(len(teams_df))]
teams_df.drop('logos', axis = 1, inplace = True)
teams_df.rename(columns = {'school' : 'team'}, inplace = True)

#Writing to CSV
teams_df.to_csv(cwd + "\\Data\\college_football_analysis\\teams.csv", index = False)

#Pulling team talent scores
talent_df = pd.DataFrame()

for i in range(start_year, end_year):
    talent_temp = teams_api.get_talent(year = i)
    talent_df_temp = pd.DataFrame.from_records([talent.to_dict() for talent in talent_temp])
    talent_df = pd.concat([talent_df, talent_df_temp.dropna(axis = 0)], axis = 0)

#Writing to CSV
talent_df.to_csv(cwd + "\\Data\\college_football_analysis\\talent_df.csv", index = False)

#######
# GAMES
#######

#Connecting to games API and getting game data
games_api = cfbd.GamesApi(api_config)
game_results_df = pd.DataFrame()

for i in range(start_year, end_year):
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
    #if i != 2022:
     #   game_results_df = pd.concat([game_results_df, game_results_df_temp.dropna(axis=0)], axis=0)
    #else:
    game_results_df = pd.concat([game_results_df, game_results_df_temp[game_results_df_temp.home_division == 'fbs' & game_results_df_temp.away_division == 'fbs'].reset_index()], axis = 0)
    
#Writing to CSV
game_results_df.to_csv(cwd + "\\Data\\college_football_analysis\\game_results_df.csv", index = False)

#######
# ADV STATS
#######

#Connecting to advanced stats API and getting games stats
stats_api = cfbd.StatsApi(api_config)
adv_stats_df = pd.DataFrame()

for i in range(start_year, end_year):
    adv_stats_temp = stats_api.get_advanced_team_game_stats(year = i, exclude_garbage_time = True)
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

#Writing to CSV
adv_stats_df_final.to_csv(cwd + "\\Data\\college_football_analysis\\adv_stats_df.csv", index = False)

#######
# METRICS
#######

#Connecting to metrics API and getting win probabilities
metrics_api = cfbd.MetricsApi(api_config)
win_prob_df = pd.DataFrame()

for i in range(start_year, end_year):
    win_prob_temp = metrics_api.get_pregame_win_probabilities(year = i)
    win_prob_df_temp = pd.DataFrame.from_records([metric.to_dict() for metric in win_prob_temp])
    win_prob_df = pd.concat([win_prob_df, win_prob_df_temp.dropna(axis = 0)], axis = 0)

#Writing to CSV
win_prob_df.to_csv(cwd + "\\Data\\college_football_analysis\\win_prob_df.csv", index = False)

#######
# BETTING
#######

#Connecting to betting API and getting lines
betting_api = cfbd.BettingApi(api_config)
betting_df = pd.DataFrame()

#Setting book to pull from
book = 'Bovada'
sec_book = 'consensus'

for i in range(start_year, end_year):
        betting_temp = betting_api.get_lines(year = i)
        betting_df_temp = pd.DataFrame.from_records([dict(game_id = bet.id \
                                                       , season = bet.season \
                                                       , week = bet.week \
                                                       , home_team = bet.home_team \
                                                       , home_score = bet.home_score \
                                                       , away_team = bet.away_team \
                                                       , away_score = bet.away_score \
                                                       , lines = bet.lines) \
                                                       for bet in betting_temp if bet.lines != []])
        for index, row in betting_df_temp.iterrows():
            line_list = [record.to_dict() for record in row['lines']]
            try:
                betting_df_temp.at[index, 'lines'] = [line for line in line_list if book in list(line.values())][0]
            except IndexError:
                try:
                    betting_df_temp.at[index, 'lines'] = [line for line in line_list if sec_book in list(line.values())][0]
                except IndexError:    
                    betting_df_temp.at[index, 'lines'] = None
        betting_df = pd.concat([betting_df, betting_df_temp.dropna(axis = 0)], axis = 0)

#Fixing lines columns of betting df and creating final betting df
lines_df = betting_df['lines'].apply(pd.Series)
betting_df_final= pd.concat([betting_df.drop('lines', axis = 1), lines_df], axis = 1)

#Writing to CSV
betting_df_final.to_csv(cwd + "\\Data\\college_football_analysis\\betting_df.csv", index = False)

#######
# RECRUITING
#######

#Connecting to recruiting API and getting recruiting rankings
recruiting_api = cfbd.RecruitingApi(api_config)
recruiting_rank_df = pd.DataFrame()

for i in range(start_year, end_year):
    recruiting_temp = recruiting_api.get_recruiting_teams(year = i)
    recruiting_df_temp = pd.DataFrame.from_records([dict(year = recruit.year \
                                                        , recruiting_rank = recruit.rank \
                                                        , team = recruit.team \
                                                        , recruiting_points = recruit.points) \
                                                        for recruit in recruiting_temp])
    recruiting_rank_df = pd.concat([recruiting_rank_df, recruiting_df_temp.dropna(axis = 0)], axis = 0)

#Writing to CSV
recruiting_rank_df.to_csv(cwd + "\\Data\\college_football_analysis\\recruiting_df.csv", index = False)

#CONSIDER ADDING POSITION RANKINGS

#######
# PLAYERS
#######

#Connecting to players API and getting returning production
players_api = cfbd.PlayersApi(api_config)
returning_production_df = pd.DataFrame()

for i in range(start_year, end_year):
    production_temp = players_api.get_returning_production(year = i)
    returning_production_df_temp = pd.DataFrame.from_records([dict(year = prod.season \
                                                        , team = prod.team \
                                                        , returning_PPA = prod.total_ppa \
                                                        , returning_passing_ppa = prod.total_passing_ppa \
                                                        , returning_receiving_ppa = prod.total_receiving_ppa \
                                                        , returning_rushing_ppa = prod.total_rushing_ppa \
                                                        , returning_ppa_percent = prod.percent_ppa \
                                                        , returning_passing_ppa_percent = prod.percent_passing_ppa \
                                                        , returning_receiving_ppa_percent = prod.percent_receiving_ppa \
                                                        , returning_rushing_ppa_percent = prod.percent_rushing_ppa \
                                                        , returning_usage = prod.usage \
                                                        , returning_passing_usage = prod.passing_usage \
                                                        , returning_receiving_usage = prod.receiving_usage \
                                                        , returning_rushing_usage = prod.rushing_usage) \
                                                        for prod in production_temp])
    returning_production_df = pd.concat([returning_production_df, returning_production_df_temp.dropna(axis = 0)], axis = 0)

#Writing to CSV
returning_production_df.to_csv(cwd + "\\Data\\college_football_analysis\\returning_production_df.csv", index = False)

#CONSIDER ADDING TRANSFER PORTAL RANKINGS


####SAVE FOR LATER
#CREATING FINAL DATAFRAMES
#game_stats_df = game_results_df.merge(adv_stats_df_final \
#                                    , how = 'inner'\
#                                    , on = 'game_id').dropna(axis = 0)
#game_stats_bets_df = game_stats_df.merge(betting_df_final \
#                                       , how = 'inner' \
#                                       , on = 'game_id')
#game_stats_bets_df_clean = game_stats_bets_df.dropna(axis = 0)
