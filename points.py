#!/usr/bin/env python3
import pandas as pd
import employee, file_utils, dataset, report_type
import os, time
import datetime as dt
from dateutil.relativedelta import relativedelta

# Current working directory
CUR_DIR = os.getcwd()

CONFIGURATION = CUR_DIR + '/points.conf'

# Output directory - change to desired location
out_dir = CUR_DIR + '/output/points/'

# Directory to scan for data input files - change to desired location
in_dir = CUR_DIR + '/data/points/'

EXCLUDE_DIRS = ['old']

# Extensions to include in file list
EXT = (('.xls', '.xlsx', '.csv', '.htm'))

# Set pandas to display all columns
pd.set_option('display.max_columns', None)

def create_dirs():
    file_utils.create_dir(out_dir)
    file_utils.create_dir(in_dir)


def setup():
    conf = file_utils.read_conf_file(CONFIGURATION, ['in', 'out'])
    
    if 'in' in conf:
        global in_dir
        in_dir = conf['in']

    if 'out' in conf:
        global out_dir
        out_dir = conf['out']

    create_dirs()


def get_attendance_info(num_of_totals=1):
    setup()

    files = file_utils.get_files_list(directory=in_dir, extensions=EXT,
                                      abs_path=True, sub_dirs=True, exclude_dirs=EXCLUDE_DIRS)

    datasets = []
    dates = []

    start_time = time.time()
    for f in files:
        if not '/~' in f:
            datasets.append(dataset.Dataset(f))
            date = dt.datetime.strptime(''.join(c for c in f if not c.isalpha() and c not in '.:/&_ '), '%Y%m%d').date()
            dates.append(date)

    print("\nLoading all files took {} seconds.".format(time.time() - start_time))

    df_emp = employee.get_employee_info()

    df = pd.DataFrame()
    cols = []

    for i in range(num_of_totals, 0, -1):
        df_lt = [x.df for x in datasets \
            if x.df_type == report_type.Report_Type.LEAVE_TAKEN][-i]

        df_att = get_attendance_from_leave_taken(df_emp, df_lt)

        col = '_point_total_' + dt.datetime.strftime(get_start_date(dates[-i]), '%m/%d/%Y') + '_to_' + dt.datetime.strftime(dates[-i], '%m/%d/%Y')
        if i > 1:
            col = 'previous' + col
        else:
            col = 'current' + col

        cols.append(col)

        df_att['points'] = df_att['points'].fillna(0)

        df_att.rename(index=str, columns={'points' : col}, inplace=True)

        if len(cols) > 1:
            df = df[['payroll_number', cols[-2]]]
            df = pd.merge(df_att, df, how='left', on='payroll_number')
            df['change'] = df[cols[-1]] - df[cols[-2]]
        else:
            df = df_att

    if num_of_totals > 1:
        df = df[['payroll_number', 'last_name', 'first_name', 'position', 'most_recent_occurrence', 'most_recent_occurrence_type', cols[-2], cols[-1], 'change']]
    else:
        df = df[['payroll_number', 'last_name', 'first_name', 'position', 'most_recent_occurrence', 'most_recent_occurrence_type', cols[-1]]]

    df.reset_index(drop=True, inplace=True)

    return df


def get_start_date(date):
    return date - relativedelta(years=1) + relativedelta(days=1)


def get_attendance_from_leave_taken(df_main, df_att):
    df_att = df_att[['payroll_number', 'date', 'actual_leave']]
    df_att = pd.merge(df_att, df_main, how='left', on='payroll_number')

    df_att['payroll_number'].fillna(method='ffill', inplace=True)

    df_att['date'] = pd.to_datetime(df_att['date'])
    df_att['role_date'] = pd.to_datetime(df_att['role_date'])

    df_att = df_att[df_att['date'] >= df_att['role_date']]

    df_att['points'] = df_att['actual_leave'].str.split('-') \
        .str[-1].str.strip(' ')
    df_att['points'] = df_att['points'] \
        .apply(lambda x: 0.5 if x == '1/2' else x)
    df_att = df_att[df_att['points'] != 'MI']

    df_att['points'] = df_att['points'].astype(float)
    df_att = df_att[['payroll_number', 'date', 'actual_leave', 'points']]

    df_point_totals = df_att[['payroll_number', 'points']] \
        .groupby(['payroll_number']).sum().reset_index()

    df_most_recent = df_att.sort_values(by=['payroll_number', 'date'],
                     ascending=(False, False,))
    df_most_recent.drop_duplicates('payroll_number', inplace=True)

    df_most_recent = df_most_recent[['payroll_number', 'date', 'actual_leave']]

    df_point_totals = pd.merge(df_point_totals, df_most_recent, how='left', on='payroll_number')

    df = pd.merge(df_main, df_point_totals, how='left', on='payroll_number')

    df.rename(index=str, columns={'date' : 'most_recent_occurrence', 'actual_leave' : 'most_recent_occurrence_type'}, inplace=True)

    return df

if __name__ == '__main__':
    df = get_attendance_info(2)

    positions = employee.get_positions()
    positions = positions[['position', 'att_group']]

    df = pd.merge(df, positions, how='left', on='position')

    df.dropna(subset=['att_group'], inplace=True)

    for group in positions['att_group'].unique():
        df_group = df[df['att_group'] == group]

        df_group.drop(columns=['att_group'], inplace=True)

        df_group.drop_duplicates(inplace=True)

        df_group.reset_index(drop=True, inplace=True)

        filename = out_dir + pd.Timestamp.now().strftime('%Y%m%d%H%M') + '_' + group + '_points.csv'

        # Save raw file
        df_group.to_csv(filename, index=False)

        print(df_group)

    # Open output directory
    os.startfile(out_dir)
