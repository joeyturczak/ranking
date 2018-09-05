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
# Evaluation directory
eval_dir = data_dir + 'eval/'
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
        (bool) abs_path - set to true if provided directory is an absolute path. 
                          defaults to false for relative path.
    Returns:
        files - List of files in specified directory
    """
    file_dir = ''
    if abs_path:
        file_dir = directory
    files = [file_dir + f for f in os.listdir(directory) 
            if (file_dir + f).startswith(startswith) and f.endswith(extensions)]
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
        df.rename(index=str, columns={'payroll_#':'payroll_number', 
                                      'employee_number':'payroll_number',
                                      'role_/_rate_effective_date':'role_date'}, inplace=True)
        # Convert payroll_number column to string datatype and add leading zeroes
        df['payroll_number'] = df['payroll_number'].astype(str).apply('{0:0>6}'.format)
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
    elif 'emp' in filename:
        df = pd.read_excel(filename, header=5)
    elif 'demo' in filename:
        df = pd.read_excel(filename, header=3)
    else:
        df = pd.read_excel(filename)
    return df


# def get_employee_data():
#     """
#     Combine employee data from multiple datasets into one dataset

#     Returns:
#         df_combined - Pandas DataFrame that contains all relavent employee data for ranking
    
#     TODO create separate functions for each dataset
#          group files that start with the same characters and run ranking for each
#     """
#     for f in get_files_list(directory=data_dir, extensions=data_ext, abs_path=True):
#         if 'att' in f:
#             df_att = get_dataset(f)
#             df_att = df_att[['payroll_number', 'points']]
#             df_att['points'] = df_att['points'].apply(lambda x: 12 if x > 12 else x)
#         elif 'emp' in f:
#             df_emp = get_dataset(f)
#             df_emp = df_emp[['payroll_number', 'name']]
#         elif 'eval' in f:
#             df_eval = get_dataset(f)
#             df_eval = df_eval[['payroll_number', 'competency_score']]
#         elif 'demo' in f:
#             df_role = get_dataset(f)
#             df_role = df_role[['payroll_number', 'role_date']]

#     df_combined = pd.merge(df_emp, df_att, how='left', on='payroll_number')
#     df_combined = pd.merge(df_combined, df_eval, how='left', on='payroll_number')
#     df_combined = pd.merge(df_combined, df_role, how='left', on='payroll_number')
    
#     df_combined.fillna(0, inplace=True)
#     df_combined['role_date'] = pd.to_datetime(df_combined['role_date'])

#     return df_combined


def get_employee_info(file_prefix):
    """
    Combine employee data from multiple datasets into one dataset

    Returns:
        df_combined - Pandas DataFrame that contains all relavent employee data for ranking
    
    TODO create separate functions for each dataset
         group files that start with the same characters and run ranking for each
    """
    df_eval = get_eval_scores()
    for f in get_files_list(directory=data_dir, extensions=data_ext, startswith=file_prefix, abs_path=True):
        if 'att' in f:
            df_att = get_dataset(f)
            df_att = df_att[['payroll_number', 'points']]
            df_att['points'] = df_att['points'].apply(lambda x: 12 if x > 12 else x)
        elif 'emp' in f:
            df_emp = get_dataset(f)
            df_emp = df_emp[['payroll_number', 'name']]
        # elif 'eval' in f:
        #     df_eval = get_dataset(f)
        #     df_eval = df_eval[['payroll_number', 'competency_score']]
        # elif 'demo' in f:
        #     df_role = get_dataset(f)
        #     df_role = df_role[['payroll_number', 'role_date']]
    
    df_role = get_role_dates(df_emp)
    df_role = df_role[['payroll_number', 'role_date']]

    df_combined = pd.merge(df_emp, df_att, how='left', on='payroll_number')
    df_combined = pd.merge(df_combined, df_eval, how='left', on='payroll_number')
    df_combined = pd.merge(df_combined, df_role, how='left', on='payroll_number')
    
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

    df_noscore = df_noscore.sort_values(by=['role_score', 'att_score', 'payroll_number'], ascending=False)
    df_noscore.reset_index(drop=True, inplace=True)

    df_temp = df_temp.sort_values(by=['role_score', 'att_score', 'payroll_number'], ascending=False)
    df_temp.reset_index(drop=True, inplace=True)

    df = df_score.append(df_noscore, ignore_index=True)
    df = df.append(df_temp, ignore_index=True)
    df.reset_index(drop=True, inplace=True)
    df['rank'] = df.index + 1

    return df

def get_eval_scores():
    """
    """
    df_eval = pd.DataFrame()
    for f in get_files_list(directory=eval_dir, extensions=data_ext, abs_path=True):
        df_eval = df_eval.append(get_dataset(f))
    df_eval = df_eval[['payroll_number', 'competency_score']]
    df_eval.reset_index(drop=True, inplace=True)
    return df_eval


def get_file_groups():
    groups = []

    for f in get_files_list(directory=data_dir, extensions=data_ext, abs_path=True):
        if 'demo' not in f:
            groups.append(f.split('-')[0])
    groups = set(groups)
    return groups


def get_employee_list(file_prefix):
    filepath = file_prefix + '-employee_list.xlsx'
    df_emp = get_dataset(filepath)
    df_emp = df_emp[['payroll_number', 'name']]
    return df_emp


def get_role_dates(df_emp):
    filepath = data_dir + 'demo.xlsx'
    df_role = get_dataset(filepath)
    df_role = df_role[['payroll_number', 'role_date']]
    df_combined = pd.merge(df_emp, df_role, how='left', on='payroll_number')
    return df_combined


def calculate_points(file_prefix):
    """TODO
    discover which report is being used
    convert each report into standardized dataset
    calculate points
    return points dataframe
    """
    filepath = file_prefix + '-attendance.xlsx'
    df_points = get_dataset(filepath)

    # Calculate points

    # df_points = df_points[['payroll_number', 'points']]
    # df_points['points'] = df_points['points'].apply(lambda x: 12 if x > 12 else x)

    return df_points


def main():
    # df_eval = get_eval_scores()

    file_groups = get_file_groups()

    for group in file_groups:
        df = get_employee_info(group)
        print(df.head())

        # df_emp = get_employee_list(group)
        # df_emp_role = get_role_dates(df_emp)
        # df_att = calculate_points(group)
    # df = get_employee_data()
    # df = calculate_rank(df)

    # print('\n')
    # print(df)
    # print('\n')
    # print(df.info())
    # print('\n')
    # print(df.describe())

    # filename = output_dir + 'ranking_' + dt.datetime.now().strftime('%Y%m%d_%H%M') + '.csv'

    # print('\nSaving data to file: {}'.format(filename))
    # df.to_csv(filename, index=False)


if __name__ == '__main__':
    main()