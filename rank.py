#!/usr/bin/env python3
import pandas as pd
import numpy as np
import file_utils, df_utils, vr
import os
import time

# Current working directory
CURRENT_DIR = os.getcwd()

# Ouput directory - change to desired location
OUTPUT_DIR = CURRENT_DIR + '/output/'

# Directory to scan for data input files - change to desired location
SCAN_DIR = CURRENT_DIR + '/data/'

EXCLUDE_DIRS = ['old']

# Extensions to include in file list
EXT = (('.xls', '.xlsx', '.csv', '.htm'))

# Set pandas to display all columns
pd.set_option('display.max_columns', None)


def create_dirs():
    file_utils.create_dir(OUTPUT_DIR)
    file_utils.create_dir(SCAN_DIR)


def calculate_rank(df):
    """
    Calculates employee ranking from provided employee information

    Args:
        (pandas.DataFrame) df - dataset containing employee information
    Returns:
        df - Pandas DataFrame that has a calculated rank for each employee
    """
    # Ranking percentage weights
    eval_pct = 0.7
    att_pct = 0.2
    role_pct = 0.1

    # Attendance range: 0 to 12 reversed, increments of 0.5
    att_range = np.array([12.0, 11.5, 11.0, 10.5, 10.0, 9.5, 9.0, 8.5, 8.0,
                          7.5, 7.0, 6.5, 6.0, 5.5, 5.0, 4.5, 4.0, 3.5, 3.0,
                          2.5, 2.0, 1.5, 1.0, 0.5, 0])
    att_score = np.linspace(0, 1, att_range.size)

    # Eval score range: 1 to 5, increments of 0.2, includes 0.
    # Values over the max for the dataset are removed
    eval_range = np.array([0, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6,
                           2.8, 3.0, 3.2,  3.4, 3.6, 3.8, 4.0, 4.2, 4.4, 4.6,
                           4.8, 5.0])
    eval_range = np.delete(eval_range,
                           np.argwhere(eval_range > df['competency_score']
                                       .max()))
    eval_score = np.linspace(0, 1, eval_range.size)

    # Role date range: min role date to max role date reversed,
    # increments of 1 day
    role_date_range = pd.date_range(df['role_date'].min(),
                                    df['role_date'].max(), freq='D')[::-1]
    role_date_len = len(pd.date_range(df['role_date'].min(),
                                      df['role_date'].max(), freq='D'))
    role_score = np.linspace(0, 1, role_date_len)

    # Set point maximum to 12
    df['capped_points'] = df['points'].apply(lambda x: 12 if x > 12 else x)

    # Lookup index of values from appropriate scale
    df['att_score'] = df['capped_points'] \
        .apply(lambda x: att_score[att_range == x]).astype(float)

    df['eval_score'] = df['competency_score'] \
        .apply(lambda x: eval_score[eval_range == x]).astype(float)

    df['role_score'] = df['role_date'] \
        .apply(lambda x: role_score[role_date_range == x]).astype(float)

    # Calculate total ranking score using percentage weights
    df['rank_score'] = df['att_score'] * att_pct + df['eval_score'] \
        * eval_pct + df['role_score'] * role_pct

    # Separate employees into three groups: has an eval score,
    # no eval score, contingent employees
    df_score = df.query('competency_score > 0')

    df_noscore = df.loc[(df['competency_score'] == 0) &
                        (df['payroll_number'].str.contains('^[0-9]+'))]

    df_temp = df.loc[df['payroll_number'].str.startswith('C')]

    # Sort employees with an eval score by rank score
    df_score = df_score.sort_values(by=['rank_score'], ascending=False)
    df_score.reset_index(drop=True, inplace=True)

    # Sort employees without an eval score by seniority,
    # then attendance, then payroll number
    df_noscore = df_noscore \
        .sort_values(by=['role_score', 'att_score', 'payroll_number'],
                     ascending=(False, False, True))
    df_noscore.reset_index(drop=True, inplace=True)

    # Sort contingent employees without an eval score by seniority,
    # then attendance, then payroll number
    df_temp = df_temp \
        .sort_values(by=['role_score', 'att_score', 'payroll_number'],
                     ascending=(False, False, True))
    df_temp.reset_index(drop=True, inplace=True)

    # Stitch all three groups back together and create a number rank
    df = df_score.append(df_noscore, ignore_index=True)
    df = df.append(df_temp, ignore_index=True)
    df.reset_index(drop=True, inplace=True)
    df['rank'] = df.index + 1

    return df


if __name__ == '__main__':
    create_dirs()

    files = file_utils.get_files_list(directory=SCAN_DIR, extensions=EXT,
                                      abs_path=True, sub_dirs=True, exclude_dirs=EXCLUDE_DIRS)

    datasets = []

    start_time = time.time()
    for f in files:
        datasets.append(vr.Dataset(f))
    
    print("\nLoading all files took {} seconds.".format(time.time() - start_time))

    df_perf = df_utils.append_dfs([x.df for x in datasets \
        if x.df_type == vr.Dataset.DF_TYPES['performance']])

    df_role = [x.df for x in datasets \
        if x.df_type == vr.Dataset.DF_TYPES['role_date']][0]

    df_att = [x.df for x in datasets \
        if x.df_type == vr.Dataset.DF_TYPES['attendance']][0]

    groups = [(x.df, x.df_group) for x in datasets \
        if x.df_type == vr.Dataset.DF_TYPES['employee_list']]

    for group in groups:
        df_emp = group[0]
        group_name = group[1]

        print('\n\nCalculating ranking for {}\n'.format(group_name))
        start_time = time.time()

        df = vr.get_employee_data(df_emp, df_att, df_role, df_perf)

        df = calculate_rank(df)

        # first_two = [x[:2].upper() for x in group_name.split('_') if x.isalpha()]
        # df['import_rank'] = df['rank'].apply(lambda x: ''.join(first_two) + '-' + '{0:0>3}'.format(x))

        # Reorder columns
        df_dist = df[['payroll_number', 'last_name', 'first_name',
                 'competency_score', 'capped_points', 'role_date', 'rank']]

        df = df[['payroll_number', 'last_name', 'first_name',
                 'competency_score', 'points', 'capped_points', 'role_date', 'eval_score',
                 'att_score', 'role_score', 'rank_score', 'rank']]

        print(df.head())
        print('\n')
        print(df.info())
        print('\n')
        print(df.describe())

        filename = OUTPUT_DIR + pd.Timestamp.now().strftime('%Y%m%d%H%M') + \
            '_' + group_name

        # Save raw file
        df.to_csv(filename + '_ranking_raw.csv', index=False) 

        # Save distribution file
        df_dist.to_csv(filename + '_ranking_dist.csv', index=False)

        print("\nRanking calculation took {} seconds.".format(time.time() - start_time))

    # Open output directory
    os.startfile(OUTPUT_DIR)