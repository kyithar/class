import pandas as pd
import numpy as np

##### Load movies.csv #######
df_mov = pd.read_csv('movies.csv', encoding='utf-8')
#convert 'generes' to category type
df_cat_tmp = df_mov['genres'].astype('category')
df_cat_tmp =df_cat_tmp.unique()
print(df_cat_tmp)
#convert categories to integers
cat_to_int = {word: ii for ii, word in enumerate(df_cat_tmp, 1)}
print(cat_to_int)
#replace int in original dataset
df_mov['genres'] = df_mov['genres'].map(cat_to_int)
print(df_mov.head(3))

#use regx to extract release Date
df_tmp = df_mov['title'].str.extract('(\(\d.{3})',expand=False)
df_mov['releaseDate'] = df_tmp.str.extract('(\d+)',expand=False)
print(df_mov.head(3))

##### load rating.csv ##########
hourly = 3600
daily = 86400 # second to day
yearly = 31536000
df_rate = pd.read_csv('ratings.csv', encoding='utf-8')
df_rate['tstamp_hour']=np.ceil(df_rate['timestamp']/hourly)
df_rate['tstamp_day']=np.ceil(df_rate['timestamp']/daily)
df_rate['tstamp_year']=np.ceil(df_rate['timestamp']/yearly)
print(df_rate.head(3))


####join two datasets

cols = ['movieId']
ext = ['genres','releaseDate']
df_rate = df_rate.join(df_mov.set_index(cols)[ext], on=cols)

print(df_rate.head(3))

#### Save as CSV #####
df_rate.to_csv('joined.csv')
print("Succuseffuly saved")
