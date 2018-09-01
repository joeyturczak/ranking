import pandas as pd
import os

def get_files_list(directory='/', extensions='', abs_path=False):
    file_dir = ''
    if abs_path:
        file_dir = directory
    files = [file_dir + f for f in os.listdir(directory) if f.endswith(extensions)]
    return files

def clean_dataset(func):
    def wrapper(*args, **kwargs):
        df = func(*args, **kwargs)
        df.rename(columns=lambda x: x.strip().lower().replace(' ', '_'), inplace=True)
        return df
    return wrapper

@clean_dataset
def get_dataset(filename):
    print('\nLoading data from {}'.format(filename))
    
    if filename.endswith('.csv'):
        df = pd.read_csv(filename)
    else:
        df = pd.read_excel(filename)
    return df

def main():
    cur_dir = os.getcwd()
    data_dir = cur_dir + '/data/'
    data_ext = (('.xls', '.xlsx', '.csv', '.htm'))

    print('\n*******************************************************************')
    print('******************************RANKING******************************')
    print('*******************************************************************')
    
    for f in get_files_list(directory=data_dir, extensions=data_ext, abs_path=True):
        if 'att' in f:
            att_df = get_dataset(f)
            att_df = att_df[['file_number', 'points']]
        elif 'emp' in f:
            emp_df = get_dataset(f)
            emp_df = emp_df[['file_number', 'name']]
        elif 'eval' in f:
            eval_df = get_dataset(f)
            eval_df = eval_df[['file_number', 'overall_score']]
        elif 'role' in f:
            role_df = get_dataset(f)
            role_df = role_df[['file_number', 'role_date']]
    print(att_df.head(1))
    print(emp_df.head(1))
    print(eval_df.head(1))
    print(role_df.head(1))
    df_combined = pd.merge(emp_df, att_df, on='file_number')
    df_combined = pd.merge(df_combined, eval_df, on='file_number')
    df_combined = pd.merge(df_combined, role_df, on='file_number')
    print(df_combined.head(1))

if __name__ == '__main__':
    main()