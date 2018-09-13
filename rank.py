#!/usr/bin/env python3
import pandas as pd
import numpy as np
import os
import pathlib
import time

# Current working directory
CURRENT_DIR = os.getcwd()

# Ouput directory - change to desired location
OUTPUT_DIR = CURRENT_DIR + '/output/'

# Directory to scan for data input files - change to desired location
SCAN_DIR = CURRENT_DIR + '/data/'

# Extensions to include in file list
EXT = (('.xls', '.xlsx', '.csv'))

# Set pandas to display all columns
pd.set_option('display.max_columns', None)


def create_dirs():
    """Create directories if they don't already exist"""
    pathlib.Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    pathlib.Path(SCAN_DIR).mkdir(parents=True, exist_ok=True)


def get_files_list(directory='/', extensions='',
                   startswith='', abs_path=False, sub_dirs=False):
    """
    Retrieves a list of files for a given directory.

    Args:
        (str) directory - path of directory to find files.
              defaults to root directory.
        (Tuple) extensions - extensions to include.
                includes all extensions by default.
        (str) startswith - file name must begin with this string.
        (bool) abs_path - set to true if provided directory is an
               absolute path. Defaults to false for relative path.
        (bool) sub_dirs - set to true to include files in sub directories.
               Defaults to false.
    Returns:
        files - List of files in specified directory
    """
    file_dir = ''
    if abs_path:
        file_dir = directory
    if sub_dirs:
        files = [os.path.join(root, f)
                 for root, dirs, files in os.walk(directory)
                 for f in files
                 if f.startswith(startswith) and f.endswith(extensions)]
    else:
        files = [file_dir + f for f in os.listdir(directory)
                 if f.startswith(startswith) and f.endswith(extensions)]
    return files


def format_dataset(df):
    """
    Renames and formats columns.

    Args:
        (pandas.DataFrame) df - Pandas DataFrame containing
                           employee information.
    Returns:
        df - Pandas DataFrame that has been cleaned up
    """
    # Strip whitespace from columns names and make them lowercase
    df.rename(columns=lambda x: x.strip().lower().replace(' ', '_'),
              inplace=True)
    df.rename(index=str,
              columns={'payroll_#': 'payroll_number',
                       'employee_number': 'payroll_number',
                       'role_/_rate_effective_date': 'role_date'},
              inplace=True)
    # Convert payroll_number column to string datatype and
    # add leading zeroes
    if 'payroll_number' in df.columns:
        df['payroll_number'] = df['payroll_number'] \
           .astype(str).apply('{0:0>6}'.format)
    return df


def identify_dataset(func):
    """
    Identifies dataset type and employee group it belongs to,
    and removes non-header rows.

    Args:
        (function) func - function that returns a Pandas DataFrame.
    Returns:
        dict('df', 'type', 'group')
            'df' - Pandas DataFrame that has been cleaned up
            'type' - string -- type of dataset: emp(employee list),
                     att(attendance points), eval(performance reviews),
                     role(role dates)
            'group' - string -- employee group the dataset belongs to.
                      returns None if not applicable
    """
    def wrapper(*args, **kwargs):
        df = func(*args, **kwargs)
        df_group = None
        first_col = df.columns[0]
        if 'Employee Settings Report' in first_col:
            header_row = 5
            df_type = 'emp'
            df_group = df.iloc[2, 1].strip(' position.').split('the ')[-1] \
                .replace(' ', '_').lower()
        elif 'Payroll #' in first_col:
            header_row = 0
            df_type = 'emp'
        elif 'Virtual Roster Employee' in first_col:
            header_row = 2
            df_type = 'role'
        elif 'Leave Taken' in first_col:
            header_row = 7
            df_type = 'att'
            df_group = df.iloc[3, 0].strip(' position.').split('the ')[-1] \
                .replace(' ', '_').lower()
        elif 'Review Sub-Status' in first_col:
            header_row = 0
            df_type = 'eval'
        else:
            header_row = 0
            df_type = 'none'

        if header_row:
            df.rename(columns=df.iloc[header_row], inplace=True)
            df = df[header_row + 1:]
            df.reset_index(drop=True, inplace=True)
        return {'df': format_dataset
    (df), 'type': df_type, 'group': df_group}
    return wrapper


@identify_dataset
def load_dataset(filepath):
    """
    Retrieves dataset from specified file

    Args:
        (str) filename - file path of dataset
    Returns:
        df - Pandas DataFrame that has been loaded from file
    """
    print('\nLoading data from {}'.format(filepath))

    if filepath.endswith('.csv'):
        df = pd.read_csv(filepath)
    else:
        df = pd.read_excel(filepath)
    return df


def combine_dfs(dfs):
    """
    Appends dataframes together.

    Args:
        (list(pandas.DataFrame)) dfs - list of Pandas DataFrames
    Returns:
        df - a single Pandas DataFrame appended together from
             given list of dataframes
    """
    df_r = pd.DataFrame()

    for df in dfs:
        df_r = df_r.append(df)

    df_r.reset_index(drop=True, inplace=True)

    return df_r


def compile_employee_info(df_emp, df_att, df_role, df_eval):
    """
    Merges provided dataset together into one comprehensive dataset including
    only employees in provided employee list

    Args:
        (pandas.DataFrame) df_emp - Pandas DataFrame containing a list of employees
        (pandas.DataFrame) df_att - Pandas DataFrame containing a list of employee
                           attendance occurrences
        (pandas.DataFrame) df_role - Pandas DataFrame containing a list of employee
                           role dates
        (pandas.DataFrame) df_eval - Pandas DataFrame containing a list of employee
                           performance scores
    Returns:
        df - a single Pandas DataFrame containing information for employees on
             the given employee list
    """

    # Split last name and first name into separate columns
    df_emp[['last_name', 'first_name']] = df_emp['name'] \
        .apply(lambda x: pd.Series(x.split(', ')))
    df_emp = df_emp[['payroll_number', 'last_name', 'first_name']]

    # Add role dates for employees
    df_role = df_role[['payroll_number', 'role_date']]
    df_combined = pd.merge(df_emp, df_role,
                           how='left', on='payroll_number')

    # Remove irrelevant columns from performance review data
    df_eval = df_eval[['payroll_number', 'competency_score']]
    df_eval.reset_index(drop=True, inplace=True)

    # If role date is from october last year or later, set to 0
    df_eval = pd.merge(df_role, df_eval, how='left', on='payroll_number')
    year = pd.Timestamp.now().year
    eval_date = pd.Timestamp(year=year - 1, month=10, day=1)
    df_eval = df_eval[df_eval['role_date'] < eval_date]

    df_eval = df_eval[['payroll_number', 'competency_score']]

    # Get payroll number
    df_att['payroll_number'] = df_att['employee_name'].str[1:7]

    # Merge datasets
    df_att = df_att[['payroll_number', 'date', 'actual_leave']]
    df_att = pd.merge(df_att, df_combined, how='left', on='payroll_number')

    # Forward fill missing data
    df_att.fillna(method='ffill', inplace=True)

    # Remove any occurrences before role start date
    df_att = df_att[df_att['date'] >= df_att['role_date']]

    # Strip point values from leave names
    df_att['points'] = df_att['actual_leave'].str.split('-') \
        .str[-1].str.strip(' ')
    df_att['points'] = df_att['points'] \
        .apply(lambda x: 0.5 if x == '1/2' else x)
    df_att = df_att[df_att['points'] != 'MI']

    # Remove irrelevant columns from attendance data
    df_att['points'] = df_att['points'].astype(float)
    df_att = df_att[['payroll_number', 'date', 'points']]

    # Get point totals for each employee
    df_point_totals = df_att[['payroll_number', 'points']] \
        .groupby(['payroll_number']).sum().reset_index()

    # Set point maximum to 12
    df_point_totals['points'] = df_point_totals['points'] \
        .apply(lambda x: 12 if x > 12 else x)

    # Merge datasets together
    df_combined = pd.merge(df_combined, df_point_totals,
                           how='left', on='payroll_number')
    df_combined = pd.merge(df_combined, df_eval,
                           how='left', on='payroll_number')

    # Fill in missing data with 0
    df_combined.fillna(0, inplace=True)
    # Ensure role dates are datetime data type
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

    # Lookup index of values from appropriate scale
    df['att_score'] = df['points'] \
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


def main():
    create_dirs()

    files = get_files_list(directory=SCAN_DIR, extensions=EXT,
                           abs_path=True, sub_dirs=True)

    emp_datasets = []
    att_datasets = []
    df_role = pd.DataFrame()
    df_evals = []
    groups = []

    for f in files:
        dataset = load_dataset(f)
        dataset_type = dataset['type']
        df = dataset['df']

        if dataset_type == 'role':
            df_role = df
        elif dataset_type == 'eval':
            df_evals.append(df)
        elif dataset_type == 'emp':
            emp_datasets.append(dataset)
        elif dataset_type == 'att':
            att_datasets.append(dataset)

        group = dataset['group']

        if group and group not in groups:
            groups.append(group)

    df_eval = combine_dfs(df_evals)

    for group in groups:
        print('\n\n\nCalculating ranking for {}\n'.format(group))
        start_time = time.time()

        df_emp = [x for x in emp_datasets if x['group'] == group][0]['df']
        df_att = [x for x in att_datasets if x['group'] == group][0]['df']

        df = compile_employee_info(df_emp, df_att, df_role, df_eval)
        df = calculate_rank(df)

        # Reorder columns
        df = df[['payroll_number', 'last_name', 'first_name',
                 'competency_score', 'points', 'role_date', 'eval_score',
                 'att_score', 'role_score', 'rank_score', 'rank']]

        print(df)
        print('\n')
        print(df.info())
        print('\n')
        print(df.describe())

        # Save to file
        filename = OUTPUT_DIR + group + '_ranking_' + \
            pd.Timestamp.now().strftime('%Y%m%d%H%M') + '.csv'

        print('\nSaving data to file: {}'.format(filename))
        df.to_csv(filename, index=False)

        print("\nThis took {} seconds.".format(time.time() - start_time))

    # Open output directory
    os.startfile(OUTPUT_DIR)


if __name__ == '__main__':
    main()
