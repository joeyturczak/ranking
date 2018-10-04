#!/usr/bin/env python3
import pandas as pd
import numpy as np
import file_utils, df_utils, vr
import os, sys, time

# Current working directory
CURRENT_DIR = os.getcwd()

# Output directory - change to desired location
OUTPUT_DIR = CURRENT_DIR + '/output/rank/'

# Directory to scan for data input files - change to desired location
SCAN_DIR = CURRENT_DIR + '/data/rank/'

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
    att_range = np.arange(50, 1250, 50)[::-1]
    att_range = att_range / 100
    att_range = np.append(att_range, 0)
    att_scale = np.linspace(0, 1, att_range.size)

    # Eval score range: 1 to 5, increments of 0.05, includes 0.
    # Values over the max for the dataset are removed
    eval_range = np.arange(100, 505, 5)
    eval_range = eval_range / 100
    eval_range = np.insert(eval_range, 0, 0)
    eval_range = np.delete(eval_range,
                           np.argwhere(eval_range > df['competency_score']
                                       .max()))
    perf_scale = np.linspace(0, 1, eval_range.size)

    # Role date range: min role date to max role date reversed,
    # increments of 1 day
    role_date_range = pd.date_range(df['role_date'].min(),
                                    df['role_date'].max(), freq='D')[::-1]
    role_date_len = len(pd.date_range(df['role_date'].min(),
                                      df['role_date'].max(), freq='D'))
    role_scale = np.linspace(0, 1, role_date_len)

    # Set point maximum to 12
    df['capped_points'] = df['points'].apply(lambda x: 12 if x > 12 else x)

    # Lookup index of values from appropriate scale
    df['att_scaled'] = df['capped_points'] \
        .apply(lambda x: att_scale[att_range == x]).astype(float)

    df['perf_scaled'] = df['competency_score'] \
        .apply(lambda x: perf_scale[eval_range == x]).astype(float)

    df['role_scaled'] = df['role_date'] \
        .apply(lambda x: role_scale[role_date_range == x]).astype(float)

    # Calculate total ranking score using percentage weights
    df['rank_scaled'] = df['att_scaled'] * att_pct + df['perf_scaled'] \
        * eval_pct + df['role_scaled'] * role_pct

    # Separate employees into three groups: has an eval score,
    # no eval score, contingent employees
    df_score = df.query('competency_score > 0')

    df_noscore = df.loc[(df['competency_score'] == 0) &
                        (df['payroll_number'].str.contains('^[0-9]+'))]

    df_temp = df.loc[df['payroll_number'].str.startswith('C')]

    # Sort employees with an eval score by rank score
    df_score = df_score.sort_values(by=['rank_scaled'], ascending=False)
    df_score.reset_index(drop=True, inplace=True)

    # Sort employees without an eval score by seniority,
    # then attendance, then payroll number
    df_noscore = df_noscore \
        .sort_values(by=['role_scaled', 'att_scaled', 'payroll_number'],
                     ascending=(False, False, True))
    df_noscore.reset_index(drop=True, inplace=True)

    # Sort contingent employees without an eval score by seniority,
    # then attendance, then payroll number
    df_temp = df_temp \
        .sort_values(by=['role_scaled', 'att_scaled', 'payroll_number'],
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
    perf = False
    role = False
    lt = False
    pts = False

    start_time = time.time()
    for f in files:
        if not '/~' in f:
            datasets.append(vr.Dataset(f))
            if datasets[-1].df_type == vr.ReportType.PERFORMANCE:
                perf = True
            elif datasets[-1].df_type == vr.ReportType.ROLE_DATE:
                role = True
            elif datasets[-1].df_type == vr.ReportType.LEAVE_TAKEN:
                lt = True
            elif datasets[-1].df_type == vr.ReportType.LEAVE_ENT:
                pts = True

    if not perf:
        print('\nNo performance score data found.')
        sys.exit(0)
    elif not role:
        print('\nNo role date data found.')
        sys.exit(0)
    elif not lt and not pts:
        print('\nNo attendance data found.')
        sys.exit(0)
    
    print("\nLoading all files took {} seconds.".format(time.time() - start_time))

    df_perf = df_utils.append_dfs([x.df for x in datasets \
        if x.df_type == vr.ReportType.PERFORMANCE]) 

    df_role = [x.df for x in datasets \
        if x.df_type == vr.ReportType.ROLE_DATE][0]

    df_att_lt = pd.DataFrame()
    if lt:
        df_att_lt = [x.df for x in datasets \
            if x.df_type == vr.ReportType.LEAVE_TAKEN][0]

    df_att_pts = pd.DataFrame()
    if pts:
        df_att_pts = [x.df for x in datasets \
            if x.df_type == vr.ReportType.LEAVE_ENT][0]

    groups = [(x.df, x.df_group) for x in datasets \
        if x.df_type == vr.ReportType.EMPLOYEE_LIST]

    for group in groups:
        df_emp = group[0]
        group_name = group[1]

        print('\n\nCalculating ranking for {}\n'.format(group_name))
        start_time = time.time()

        df = vr.get_employee_data(df_emp, df_att_lt, df_att_pts, df_role, df_perf)

        df = calculate_rank(df)

        # first_two = [x[:2].upper() for x in group_name.split('_') if x.isalpha()]
        # df['import_rank'] = df['rank'].apply(lambda x: ''.join(first_two) + '-' + '{0:0>3}'.format(x))

        # Reorder columns
        df_dist = df[['payroll_number', 'last_name', 'first_name',
                 'competency_score', 'capped_points', 'role_date', 'rank']]

        df = df[['payroll_number', 'last_name', 'first_name',
                 'competency_score', 'points', 'capped_points', 'role_date', 'perf_scaled',
                 'att_scaled', 'role_scaled', 'rank_scaled', 'rank']]

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