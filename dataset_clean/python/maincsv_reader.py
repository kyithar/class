import os
import pandas as pd
import numpy as np
import dask.dataframe as dd #to load large csv
import moviecsv_reader as movreader
import ratingcsv_reader as ratingreader


condition = 'daily' # choose 1) hourly, 2)daily, 3) yearly
complete = False

#####run mov reader and rating reader
movreader.movreader()
ratingreader.ratingreader(condition)

##### Load csv #######
print("Start combining 'movies.csv and rating.csv'")
df_movie = pd.read_csv('dataset_processed/movies_processed.csv').drop('Unnamed: 0',1)
df_rating = dd.read_csv('dataset_processed/rating_processed.csv').drop(['Unnamed: 0','userId'],1)
# print(df_movie.head(3))
# print(df_rating.head(3))

####extract the timeslots ID #####
t_slot_df =df_rating[condition].astype('category').unique().compute()
print("This dataset_clean has",len(t_slot_df), "timeslots" )

###### construct the dataset_clean for each timeslot ID #######
if complete == False:
    len = 1
else:
    len =len(t_slot_df)

for i in range(len):
    df_tmp=df_rating[df_rating[condition]==t_slot_df[i]].compute()
    t_stamp = df_tmp[condition].iloc[0] #extract timeslot ID
    movie_stats = df_tmp.groupby('movieId').agg({'rating': [np.size, np.mean]}) #np.size =>to get request count for each timeslot, np.mean => to get average rating for each time slot
    movie_stats=movie_stats['rating'].reset_index().rename(columns={'size': 'label', 'mean': 'avg_rating'}) #rename colums name
    movie_stats['label_n']= movie_stats['label']/np.sum(movie_stats['label']) #calclualte normalized request count for each timeslot and named as 'label_n'
    # print(movie_stats)
    ####join two datasets
    col = ['movieId'] # define the index
    ext = ['genres','releaseDate'] #define the colums you want to add
    df_tmp = movie_stats.join(df_movie.set_index(col)[ext], on=col)
    df_tmp[condition] = t_stamp
    # print(df_tmp)


    ######Save df_tmp #######
    # if file does not exist write header
    if not os.path.isfile('dataset_processed/combined.csv'):
        df_tmp.to_csv('dataset_processed/combined.csv', header='column_names')
        del df_tmp
        print("timeslot", i, 'is completed and remaining ', (len-1)-i, 'slots' )
    else:  # else it exists so append without writing the header
        df_tmp.to_csv('dataset_processed/combined.csv', mode='a', header=False)
        del df_tmp
        print("timeslot", i, 'is completed and remaining ', (len - 1) - i, 'slots')