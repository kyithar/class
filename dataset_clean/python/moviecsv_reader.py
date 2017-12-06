import pandas as pd

def movreader():
    print("Start cleaning 'movies.csv'")
    ##### Load movies.csv #######
    df_mov = pd.read_csv('dataset_original/movies.csv', encoding='utf-8')
    #convert 'generes' to category type
    df_cat_tmp = df_mov['genres'].astype('category')
    df_cat_tmp =df_cat_tmp.unique()
    # print(df_cat_tmp)
    #convert categories to integers
    cat_to_int = {word: ii for ii, word in enumerate(df_cat_tmp, 1)}
    # print(cat_to_int)
    #replace int in original dataset_clean
    df_mov['genres'] = df_mov['genres'].map(cat_to_int)
    # print(df_mov.head(3))

    #use regx to extract release Date
    df_tmp = df_mov['title'].str.extract('(\(\d.{3})',expand=False)
    df_mov['releaseDate'] = df_tmp.str.extract('(\d+)',expand=False)
    # print(df_mov.head(3))

    #### Save as CSV #####
    df_mov.to_csv('dataset_processed/movies_processed.csv')
    del df_mov
    print("movies_processed.csv is Succuseffuly saved in 'dataset_processed/'")

