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
game_results_temp= games_api.get_games(year = 2021)
team_game_stats_temp = games_api.get_team_game_stats(year = 2021, week = 2)

#Connecting to advanced stats API and getting games stats
stats_api = cfbd.StatsApi(api_config)
adv_stats_temp = stats_api.get_advanced_team_game_stats(year = 2021)

#Connecting to betting API and getting lines
betting_api = cfbd.BettingApi(api_config)
betting_temp = betting_api.get_lines(year = 2021)
