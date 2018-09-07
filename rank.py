#!/usr/bin/env python3
import pandas as pd
import numpy as np
import os
import pathlib

# Current working directory
cur_dir = os.getcwd()

dirs = {'output_dir': cur_dir + '/output/', 
        'eval_dir': cur_dir + '/performance_reviews/',
        'demo_dir': cur_dir + '/demographics/',
        'att_dir': cur_dir + '/attendance_files/',
        'emp_dir': cur_dir + '/employee_lists/',
        'old_dir': cur_dir + '/old/'}

errors = {'no_emp_list':'No employee list files. Please place employee lists in {}'.format(dirs['emp_dir']),
          'no_att_file': 'No attendance files. Please place leave taken reports in {}'.format(dirs['att_dir']),
          'no_demo_file': 'No demographics file. Please place demographics file in {}'.format(dirs['demo_dir']),
          'no_eval_file': 'No performance review files. Please place performance review files in {}'.format(dirs['eval_dir'])}
               
# Extensions to include in file list
data_ext = (('.xls', '.xlsx', '.csv'))

# Set pandas to display all columns
pd.set_option('display.max_columns', None)


def get_files_list(directory='/', extensions='', startswith='', abs_path=False):
    """
    Retrieves a list of files for a given directory.

    Args:
        (str) directory - path of directory to find files. defaults to root directory.
        (Tuple) extensions - extensions to include. includes all extensions by default.
        (str) startswith - file name must begin with this string.
        (bool) abs_path - set to true if provided directory is an absolute path. 
                          defaults to false for relative path.
    Returns:
        files - List of files in specified directory
    """
    file_dir = ''
    if abs_path:
        file_dir = directory
    files = [file_dir + f for f in os.listdir(directory) 
            if f.startswith(startswith) and f.endswith(extensions)]
    return files


def clean_dataset(func):
    """
    Renames and formats columns.

    Args:
        (function) func - function that returns a Pandas DataFrame.
    Returns:
        df - Pandas DataFrame that has been cleaned up
    """
    def wrapper(*args, **kwargs):
        df = func(*args, **kwargs)
        # Strip whitespace from columns names and make them lowercase
        df.rename(columns=lambda x: x.strip().lower().replace(' ', '_'), inplace=True)
        df.rename(index=str, columns={'payroll_#':'payroll_number', 
                                      'employee_number':'payroll_number',
                                      'role_/_rate_effective_date':'role_date'}, inplace=True)
        # Convert payroll_number column to string datatype and add leading zeroes
        if 'payroll_number' in df.columns:
            df['payroll_number'] = df['payroll_number'].astype(str).apply('{0:0>6}'.format)
        return df
    return wrapper


@clean_dataset
def get_dataset(filename):
    """
    Retrieves dataset from specified file

    Args:
        (str) filename - file path of dataset
    Returns:
        df - Pandas DataFrame that has been loaded from file
    """
    print('\nLoading data from {}'.format(filename))
    
    if filename.endswith('.csv'):
        df = pd.read_csv(filename)
    else:
        df = pd.read_excel(filename, header=None)
        if 'Employee Settings Report' in df.iloc[0,0]:
            df = pd.read_excel(filename, header=5)
        elif 'Virtual Roster Employee' in df.iloc[0,0]:
            df = pd.read_excel(filename, header=3)
        elif 'Leave Taken' in df.iloc[0,0]:
            df = pd.read_excel(filename, header=7)
        else:
            df = pd.read_excel(filename)
    return df
    

def get_employee_info(file_prefix):
    """
    Combine employee data from multiple datasets into one dataset

    Returns:
        df_combined - Pandas DataFrame that contains all relavent employee data for ranking
    """

    employee_list_file = get_files_list(directory=dirs['emp_dir'],
                           extensions=data_ext,
                           startswith=file_prefix,
                           abs_path=True)

    attendance_file = get_files_list(directory=dirs['att_dir'],
                           extensions=data_ext,
                           startswith=file_prefix,
                           abs_path=True)

    demo_file = get_files_list(directory=dirs['demo_dir'],
                           extensions=data_ext,
                           abs_path=True)

    if not employee_list_file:
        error(errors['no_emp_list'])
    elif not attendance_file:
        error(errors['no_att_file'])
    elif not demo_file:
        error(errors['no_demo_file'])

    employee_list_file = employee_list_file[0]
    attendance_file = attendance_file[0]
    demo_file = demo_file[0]

    df_emp = get_employee_list(employee_list_file)

    df_role = get_role_dates(demo_file, df_emp)

    df_eval = get_eval_scores(df_role)

    df_att = calculate_points(attendance_file, df_role)

    df_combined = pd.merge(df_role, df_att, how='left', on='payroll_number')
    df_combined = pd.merge(df_combined, df_eval, how='left', on='payroll_number')
    
    df_combined.fillna(0, inplace=True)
    df_combined['role_date'] = df_combined['role_date']

    # Move files to old folder
    old_file_prefix = dirs['old_dir'] + file_prefix + '_' + pd.Timestamp.now().strftime('%Y%m%d%H%M')
    os.rename(employee_list_file, old_file_prefix + 'emp.' + employee_list_file.split('.')[-1])
    os.rename(attendance_file, old_file_prefix + 'att.' + attendance_file.split('.')[-1])

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
    
    att_range = np.array([12.0,11.5,11.0,10.5,10.0,9.5,9.0,8.5,8.0,7.5,7.0,6.5,6.0,5.5,5.0,4.5,4.0,3.5,3.0,2.5,2.0,1.5,1.0,0.5,0])
    att_score = np.linspace(0, 1, att_range.size)

    eval_range = np.array([0,1.0,1.2,1.4,1.6,1.8,2.0,2.2,2.4,2.6,2.8,3.0,3.2,3.4,3.6,3.8,4.0,4.2,4.4,4.6,4.8,5.0])
    eval_range = np.delete(eval_range, np.argwhere(eval_range > df['competency_score'].max()))
    eval_score = np.linspace(0, 1, eval_range.size)

    role_date_range = pd.date_range(df['role_date'].min(), df['role_date'].max(), freq='D')[::-1]
    role_date_len = len(pd.date_range(df['role_date'].min(), df['role_date'].max(), freq='D'))
    role_score = np.linspace(0, 1, role_date_len)

    df['att_score'] = df['points'].apply(lambda x: att_score[att_range == x]).astype(float)
    df['eval_score'] = df['competency_score'].apply(lambda x: eval_score[eval_range == x]).astype(float)
    df['role_score'] = df['role_date'].apply(lambda x: role_score[role_date_range==x]).astype(float)
    df['rank_score'] = df['att_score'] * att_per + df['eval_score'] * eval_per + df['role_score'] * role_per

    df_score = df.query('competency_score > 0')
    df_noscore = df.loc[(df['competency_score'] == 0) & (df['payroll_number'].str.contains('^[0-9]+'))]
    df_temp = df.loc[(df['competency_score'] == 0) & (df['payroll_number'].str.startswith('C'))]

    df_score = df_score.sort_values(by=['rank_score'], ascending=False)
    df_score.reset_index(drop=True, inplace=True)

    df_noscore = df_noscore.sort_values(by=['role_score', 'att_score', 'payroll_number'], ascending=(False, False, True))
    df_noscore.reset_index(drop=True, inplace=True)

    df_temp = df_temp.sort_values(by=['role_score', 'att_score', 'payroll_number'], ascending=(False, False, True))
    df_temp.reset_index(drop=True, inplace=True)

    df = df_score.append(df_noscore, ignore_index=True)
    df = df.append(df_temp, ignore_index=True)
    df.reset_index(drop=True, inplace=True)
    df['rank'] = df.index + 1

    return df

def get_eval_scores(df_role):
    """
    """
    df_eval = pd.DataFrame()
    files = get_files_list(directory=dirs['eval_dir'], extensions=data_ext, abs_path=True)
    if not files:
        error(errors['no_eval_file'])
    for f in files:
        df_eval = df_eval.append(get_dataset(f))
    df_eval = df_eval[['payroll_number', 'competency_score']]
    df_eval.reset_index(drop=True, inplace=True)

    # If role date is from october last year or later, set to 0
    df_eval = pd.merge(df_role, df_eval, how='left', on='payroll_number')
    year = pd.Timestamp.now().year
    eval_date = pd.Timestamp(year=year - 1, month=10, day=1)
    df_eval = df_eval[df_eval['role_date'] < eval_date]

    df_eval = df_eval[['payroll_number', 'competency_score']]

    return df_eval


def get_file_groups():
    groups = []

    for f in get_files_list(directory=dirs['emp_dir'], extensions=data_ext, abs_path=True):
        groups.append(f.split('/')[-1].split('.')[0])
    if not groups:
        error(errors['no_employee_list'])
    return groups


def get_employee_list(filepath):
    df_emp = get_dataset(filepath)
    df_emp[['last_name', 'first_name']] = df_emp['name'].apply(lambda x: pd.Series(x.split(', ')))
    df_emp = df_emp[['payroll_number', 'last_name', 'first_name']]
    return df_emp


def get_role_dates(filepath, df_emp):
    df_emp_role = get_dataset(filepath)
    df_emp_role = df_emp_role[['payroll_number', 'role_date']]
    df_combined = pd.merge(df_emp, df_emp_role, how='left', on='payroll_number')
    return df_combined


def calculate_points(filepath, df_role):
    """
    """
    df = get_dataset(filepath)

    df['payroll_number'] = df['employee_name'].str[1:7]
    df = df[['payroll_number', 'date', 'actual_leave']]
    df = pd.merge(df, df_role, how='left', on='payroll_number')
    df.fillna(method='ffill', inplace=True)
    df = df[df['date'] >= df['role_date']]
    df['points'] = df['actual_leave'].str.split('-').str[-1].str.strip(' ')
    df['points'] = df['points'].apply(lambda x: 0.5 if x == '1/2' else x)
    df = df[df['points'] != 'MI']
    df['points'] = df['points'].astype(float)
    df = df[['payroll_number', 'date', 'points']]
    df_point_totals = df[['payroll_number', 'points']].groupby(['payroll_number']).sum().reset_index()
    df_point_totals['points'] = df_point_totals['points'].apply(lambda x: 12 if x > 12 else x)

    return df_point_totals


def create_dirs():
    for key, value in dirs.items():
        pathlib.Path(value).mkdir(parents=True, exist_ok=True)


def error(err):
    print('\nCannot complete ranking: {}'.format(err))
    exit()


def main():
    create_dirs()
    file_groups = get_file_groups()

    for group in file_groups:
        df = get_employee_info(group)
        df = calculate_rank(df)
        print('\n')
        print(df)
        print('\n')
        print(df.info())
        print('\n')
        print(df.describe())

        filename = dirs['output_dir'] + group.split('/')[-1] + '_ranking_' + pd.Timestamp.now().strftime('%Y%m%d%H%M') + '.csv'

        print('\nSaving data to file: {}'.format(filename))
        df.to_csv(filename, index=False)


if __name__ == '__main__':
    main()