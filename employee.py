#!/usr/bin/env python3
import pandas as pd
import file_utils, dataset, report_type
import os, time

# Current working directory
CUR_DIR = os.getcwd()

CONFIGURATION = CUR_DIR + '/employee.conf'

# Output directory - change to desired location
out_dir = CUR_DIR + '/output/rank/'

# Directory to scan for data input files - change to desired location
in_dir = CUR_DIR + '/data/rank/'

EXCLUDE_DIRS = ['old']

# Extensions to include in file list
EXT = (('.xls', '.xlsx', '.csv', '.htm'))

# Set pandas to display all columns
pd.set_option('display.max_columns', None)


def create_dirs():
    file_utils.create_dir(out_dir)
    file_utils.create_dir(in_dir)

def setup():
    conf = file_utils.read_conf_file(CONFIGURATION, ['in'])
    
    if 'in' in conf:
        global in_dir
        in_dir = conf['in']

    create_dirs()

def get_employee_info():
    files = file_utils.get_files_list(directory=in_dir, extensions=EXT,
                                      abs_path=True, sub_dirs=True, exclude_dirs=EXCLUDE_DIRS)

    datasets = []

    start_time = time.time()
    for f in files:
        if not '/~' in f:
            datasets.append(dataset.Dataset(f))

    print("\nLoading all files took {} seconds.".format(time.time() - start_time))

    df_demo = [x.df for x in datasets \
        if x.df_type == report_type.Report_Type.DEMOGRAPHICS][0]
    
    df_demo = df_demo[['payroll_number', 'last_name', 'first_name', 'classification', 'role_date']]

    print(df_demo)

if __name__ == '__main__':
    setup()

    get_employee_info()
