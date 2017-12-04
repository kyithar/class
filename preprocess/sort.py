import pandas as pd
import numpy as np
import os

##### Load joined.csv #######
df = pd.read_csv('preprocess.csv', encoding='utf-8',nrows=100).drop('Unnamed: 0',1)
df_tmp = df.sort_values(["timestamp"],ascending=True)
print(df_tmp.head(10))

df_tmp.to_csv('sorted.csv')
print("Succuseffuly saved")

