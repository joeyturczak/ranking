import pandas as pd
import numpy as np
import os
import datetime as dt


cur_dir = os.getcwd()
data_dir = cur_dir + '/data/'
output_dir = cur_dir + '/output/'
data_ext = (('.xls', '.xlsx', '.csv', '.htm'))


pd.set_option('display.max_columns', None)


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


def get_employee_data():
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
    df_score = df.query('overall_score != 0')
    df_noscore = df.query('overall_score == 0')
    df_score.sort_values(by=['rank_score'], inplace=True, ascending=False)
    df_score.reset_index(drop=True, inplace=True)
    df_noscore.sort_values(by=['role_score', 'att_score', 'file_number'], inplace=True, ascending=False)
    df_noscore.reset_index(drop=True, inplace=True)
    df = df_score.append(df_noscore, ignore_index=True)
    df.reset_index(drop=True, inplace=True)
    df['rank'] = df.index + 1

    return df


def main():
    df = get_employee_data()
    df = calculate_rank(df)

    print(df)
    print(df.describe())
    print(df.info())

    filename = output_dir + 'ranking_' + dt.datetime.now().strftime('%Y%m%d_%H%M') + '.csv'

    print('\nSaving data to file: {}'.format(filename))
    df.to_csv(filename, index=False)


if __name__ == '__main__':
    main()