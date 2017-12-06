# To get complete dataset
If you want to get complete dataset for the class project, you can run this [code](https://github.com/kyithar/class/tree/master/dataset_clean). 
It includes two versions as follows, 
1) [python files](https://github.com/kyithar/class/tree/master/dataset_clean/python), and 
2) [ipython note book files](https://github.com/kyithar/class/tree/master/dataset_clean/ipynb).

### Getting started
1) Install followings packages by using pip or pip3.
* `pip install pands`
* `pip install dask[complete] toolz cloudpickle`
* `pip install matplotlib`
* `pip install seaborn`

2) Download the [code](https://github.com/kyithar/class/tree/master/dataset_clean) to your pc
3) Download [dataset](http://files.grouplens.org/datasets/movielens/ml-latest.zip)
4) Extract 'ml-latest.zip', copy 'movies.csv' and 'ratings.csv' files from the dataset and past those files to the 'dataset_original' folder.
All of the processed files will be saved in the 'dataset_processed' folder.

5) Run the 'maincsv_reader.py' or 'maincsv.ipynb'

# Sample codes for class project
### Ipython Notebook
* [join_dataset](https://github.com/kyithar/class/blob/master/preprocess/join_dataset.ipynb)
* [preprocessing](https://github.com/kyithar/class/blob/master/preprocess/preprocessing.ipynb)
* [sort](https://github.com/kyithar/class/blob/master/preprocess/sort.ipynb)
