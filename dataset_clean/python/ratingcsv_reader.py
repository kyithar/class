import pandas as pd
import numpy as np

def ratingreader(condition_tmp):
    hourly = 3600
    daily = 86400 # second to day
    yearly = 31536000
    condition = condition_tmp # choose 1) hourly, 2)daily, 3) yearly

    ##### load rating.csv ##########
    print("Start cleaning 'ratings.csv'")
    df_rate = pd.read_csv('dataset_original/ratings.csv', encoding='utf-8')

    if condition == 'hourly':
        df_rate[condition]=np.ceil(df_rate['timestamp']/hourly)
        df_rate=df_rate.sort_values([condition], ascending=True).drop('timestamp',1)
    elif condition == 'daily':
        df_rate[condition]=np.ceil(df_rate['timestamp']/daily)
        df_rate =df_rate.sort_values([condition], ascending=True).drop('timestamp',1)
    else:
        df_rate[condition]=np.ceil(df_rate['timestamp']/yearly)
        df_rate =df_rate.sort_values([condition], ascending=True).drop('timestamp',1)

    # print(df_rate.head(3))
    #### Save as CSV #####
    df_rate.to_csv('dataset_processed/rating_processed.csv')
    del df_rate
    print("rating_process.csv is succuseffuly saved in 'dataset_processed/'")
