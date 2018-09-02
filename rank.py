import pandas as pd
import numpy as np
import os
import datetime as dt

# Current working directory
cur_dir = os.getcwd()
# Directory for data files
data_dir = cur_dir + '/data/'
# Output directory
output_dir = cur_dir + '/output/'
# Extensions to include in file list
data_ext = (('.xls', '.xlsx', '.csv', '.htm'))

# Set pandas to display all columns
pd.set_option('display.max_columns', None)


def get_files_list(directory='/', extensions='', abs_path=False):
    """
    Retrieves a list of files for a given directory.

    Args:
        (str) directory - path of directory to find files. defaults to root directory.
        (Tuple) extensions - extensions to include. includes all extensions by default.
        (bool) abs_path - set to true if provided directory is an absolute path. 
                          defaults to false for relative path.
    Returns:
        files - List of files in specified directory
    """
    file_dir = ''
    if abs_path:
        file_dir = directory
    files = [file_dir + f for f in os.listdir(directory) if f.endswith(extensions)]
    return files


def clean_dataset(func):
    """
    Cleans dataset returned by func.

    Args:
        (function) func - function that returns a Pandas DataFrame.
    Returns:
        df - Pandas DataFrame that has been cleaned up
    """
    def wrapper(*args, **kwargs):
        df = func(*args, **kwargs)
        # Strip whitespace from columns names and make them lowercase
        df.rename(columns=lambda x: x.strip().lower().replace(' ', '_'), inplace=True)
        # Convert file_number column to string datatype
        df['file_number'] = df['file_number'].astype(str)
        return df
    return wrapper


@clean_dataset
def get_dataset(filename):
    """
    Retrieve dataset from specified file

    Args:
        (str) filename - file path of dataset
    Returns:
        df - Pandas DataFrame that has been loaded from file
    """
    print('\nLoading data from {}'.format(filename))
    
    if filename.endswith('.csv'):
        df = pd.read_csv(filename)
    else:
        df = pd.read_excel(filename)
    return df


def get_employee_data():
    """
    Combine employee data from multiple datasets into one dataset

    Returns:
        df_combined - Pandas DataFrame that contains all relavent employee data for ranking
    """
    for f in get_files_list(directory=data_dir, extensions=data_ext, abs_path=True):
        if 'att' in f:
            att_df = get_dataset(f)
            att_df = att_df[['file_number', 'points']]
            att_df['points'] = att_df['points'].apply(lambda x: 12 if x > 12 else x)
        elif 'emp' in f:
            emp_df = get_dataset(f)
            emp_df = emp_df[['file_number', 'name']]
        elif 'eval' in f:
            eval_df = get_dataset(f)
            eval_df = eval_df[['file_number', 'overall_score']]
        elif 'role' in f:
            role_df = get_dataset(f)
            role_df = role_df[['file_number', 'role_date']]

    df_combined = pd.merge(emp_df, att_df, how='left', on='file_number')
    df_combined = pd.merge(df_combined, eval_df, how='left', on='file_number')
    df_combined = pd.merge(df_combined, role_df, how='left', on='file_number')
    
    df_combined.fillna(0, inplace=True)
    df_combined['role_date'] = pd.to_datetime(df_combined['role_date'])

    return df_combined


def calculate_rank(df):
    """
    Calculates employee ranking from provided employee information

    Args:
        (pandas.DataFrame) df - dataset containing employee information
    Returns:
        df - Pandas DataFrame that has a calculated rank for each employee
    """
    eval_per = 0.7
    att_per = 0.2
    role_per = 0.1
    
    att_range = np.linspace(0.5, 12, 24)
    att_range = np.insert(att_range, 0, 0)
    att_score = np.linspace(0, 1, 25)

    eval_range = np.linspace(1, 5, 21)
    eval_range = np.insert(eval_range, 0, 0)
    eval_score = np.linspace(0, 1, 22)

    role_date_range = pd.date_range(df['role_date'].min(), df['role_date'].max(), freq='D')[::-1]
    role_date_len = len(pd.date_range(df['role_date'].min(), df['role_date'].max(), freq='D'))
    role_score = np.linspace(0, 1, role_date_len)

    df['att_score'] = df['points'].astype(int).apply(lambda x: att_score[np.where(att_range==x)]).astype(float)
    df['eval_score'] = df['overall_score'].astype(int).apply(lambda x: eval_score[np.where(eval_range==x)]).astype(float)
    df['role_score'] = df['role_date'].apply(lambda x: role_score[np.where(role_date_range==x)]).astype(float)
    df['rank_score'] = df['att_score'] * att_per + df['eval_score'] * eval_per + df['role_score'] * role_per

    df_score = df.query('overall_score > 0')
    df_noscore = df.loc[(df['overall_score'] == 0) & (df['file_number'].str.contains('^[0-9]+'))]
    df_temp = df.loc[(df['overall_score'] == 0) & (df['file_number'].str.startswith('C'))]

    df_score = df_score.sort_values(by=['rank_score'], ascending=False)
    df_score.reset_index(drop=True, inplace=True)

    df_noscore = df_noscore.sort_values(by=['role_score', 'att_score', 'file_number'], ascending=False)
    df_noscore.reset_index(drop=True, inplace=True)

    df_temp = df_temp.sort_values(by=['role_score', 'att_score', 'file_number'], ascending=False)
    df_temp.reset_index(drop=True, inplace=True)

    df = df_score.append(df_noscore, ignore_index=True)
    df = df.append(df_temp, ignore_index=True)
    df.reset_index(drop=True, inplace=True)
    df['rank'] = df.index + 1

    return df


def main():
    df = get_employee_data()
    df = calculate_rank(df)

    print('\n')
    print(df)
    print('\n')
    print(df.info())
    print('\n')
    print(df.describe())

    filename = output_dir + 'ranking_' + dt.datetime.now().strftime('%Y%m%d_%H%M') + '.csv'

    print('\nSaving data to file: {}'.format(filename))
    df.to_csv(filename, index=False)


if __name__ == '__main__':
    main()