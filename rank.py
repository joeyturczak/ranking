import pandas as pd
import os

cur_dir = os.getcwd()
data_dir = cur_dir + '/data/'
data_ext = (('.xls', '.xlsx', '.csv', '.htm'))

def clean_dataset(func):
    def wrapper(*args, **kwargs):
        df = func(*args, **kwargs)
        df.rename(columns=lambda x: x.strip().lower().replace(' ', '_'), inplace=True)
        return df
    return wrapper

def get_files_list(directory='/', extensions='', abs_path=False):
    file_dir = ''
    if abs_path:
        file_dir = directory
    files = [file_dir + f for f in os.listdir(directory) if f.endswith(extensions)]
    return files

@clean_dataset
def get_dataset(filename):
    print('\nLoading data from {}'.format(filename))
    
    if filename.endswith('.csv'):
        df = pd.read_csv(filename)
    else:
        df = pd.read_excel(filename)
    print(df.head(1))
    return df


if __name__ == '__main__':
    print('\n*******************************************************************')
    print('******************************RANKING******************************')
    print('*******************************************************************')
    for f in get_files_list(directory=data_dir, extensions=data_ext, abs_path=True):
        print(get_dataset(f).head(1))

