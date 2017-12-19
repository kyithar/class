from pandas import read_csv
import os

def to_seq(data, t_slot):
    movlist=data['movieId'].unique()
    j=0
    for i in movlist:
    # for i in range(1):
        k=0
        df_tmp = data[data['movieId'] == i]
        # print(df_tmp['daily'].values[-1])
        # print(df_tmp['daily'][t_slot:].values)
        for day in df_tmp['daily'][t_slot:].values:
            df_tmp2 = df_tmp[k:k + t_slot].assign(y=df_tmp['label_n'][k + t_slot:k + (t_slot+1)])
            k +=1

            # ######Save df_tmp #######
            # # if file does not exist write header
            if not os.path.isfile('dataset_processed/seq.csv'):
                df_tmp2.to_csv('dataset_processed/seq.csv', header='column_names')
                del df_tmp2
                # print("Procressing of movieId", i, 'is completed and remaining ', (len(movlist)) - j, 'movies')
            else:  # else it exists so append without writing the header
                df_tmp2.to_csv('dataset_processed/seq.csv', mode='a', header=False)
                del df_tmp2
        print("Procressing of movieId", i, 'is completed and remaining ', (len(movlist)) - j, 'movies')
        j +=1
#
# #############load dataset #################################
rm_cols = ['label'] # the coloums to be drop from the original dataset

dataset = read_csv('dataset_processed/combined.csv',nrows=1000, header=0, index_col=0).drop(rm_cols,1)


## you can change time slot depending on your model, in here we use 3 historical information as inputs and out is one
##we denote "y" as an outputlabel
to_seq(dataset, t_slot = 3)