#!/usr/bin/env python3
import pandas as pd
import file_utils, df_utils, dataset, report_type, group
import os, time
from functools import reduce

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

pos_to_remove = ['Bussers - IP Busser', 
                 'Cashiers & Hosts - IP F&B Cashier', 
                 'Cashiers & Hosts - IP F&B Cashier/Host(ess)', 
                 'Cashiers & Hosts - IP Host/Hostess, F&B', 
                 'Cashiers & Hosts - IP F&B Lead Cashier', 
                 'IP Bars - IP Bar Back', 
                 'IP Bars - IP Bartender', 
                 'Food Servers - IP Food Server', 
                 'Food Servers - IP Lead Food Server', 
                 'z. Imported Positions - IP Inventory Control Stock', 
                 'Big Mo\' Cafe - Big Mo\'s Cart Attendant', 
                 'Culinary - IP Expediter', 
                 'IP Bars - IP Lead Bartender',
                 'IP Bars - IP Mixologist',
                 'Property-Wide Imported Positions - AV Tech I',
                 'Property-Wide Imported Positions - Office Administrator',
                 'Property-Wide Imported Positions - Admin. Asst.',
                 'Property-Wide Imported Positions - Retail Cashier',
                 'Property-Wide Imported Positions - Inventory Ctrl. Clerk',
                 'Property-Wide Imported Positions - Spv., Admin']

# move this to points module
groups = [group.Group('baker', ['Bakery - Baker', 'Bakery - Lead Baker'])]

def create_dirs():
    file_utils.create_dir(out_dir)
    file_utils.create_dir(in_dir)


def setup():
    conf = get_configuration(['in', 'out'])
    
    if 'in' in conf:
        global in_dir
        in_dir = conf['in']

    if 'out' in conf:
        global out_dir
        out_dir = conf['out']

    create_dirs()


def get_configuration(keys=[]):
    conf = {}
    conf_file = file_utils.read_conf_file(CONFIGURATION, keys)
    for key in keys:
        if key in conf_file:
            conf[key] = conf_file[key]

    return conf


def add_groups(df, groups):
    for group in groups:
        df['group'] = df['position'].apply(lambda x: group.name if x in group.positions else '')

    return df


def format_position(df):
    repls = ()
    for pos in pos_to_remove:
        repls = ((pos, ''),) + repls
    df['position'] = df['position'].apply(lambda x: reduce(lambda a, kv: a.replace(*kv), repls, x))

    return df


def get_employee_info():
    setup()

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

    tomorrow = pd.to_datetime('today') + pd.DateOffset()

    df_demo['termination_date'].fillna(tomorrow, inplace=True)

    df_demo = df_demo[df_demo['termination_date'] >= tomorrow]
    
    df_demo = df_demo[['payroll_number', 'last_name', 'first_name', 'classification', 'role_date']]

    df_perf = df_utils.append_dfs([x.df for x in datasets \
        if x.df_type == report_type.Report_Type.PERFORMANCE])

    df_perf['competency_year'] = df_perf['review_title'].str[:4]

    df_perf = df_perf[['payroll_number', 'competency_score', 'competency_year']]

    df = pd.merge(df_demo, df_perf, how='left', on='payroll_number')

    df_roles = [x.df for x in datasets \
        if x.df_type == report_type.Report_Type.EMPLOYEE_LIST][0]
    
    df_roles = df_roles[['payroll_number', 'position', 'skill']]

    df = pd.merge(df, df_roles, how='left', on='payroll_number')

    df['position'].fillna('', inplace=True)

    df = format_position(df)

    df = df.sort_values(by=['position', 'last_name', 'first_name'],
                     ascending=(True, True, True))
    
    return df

if __name__ == '__main__':
    df = get_employee_info()

    filename = out_dir + pd.Timestamp.now().strftime('%Y%m%d%H%M')

    print(df)

    # Save raw file
    df.to_csv(filename + '_employee_data.csv', index=False)

    # list of positions to exclude
    # list of positions to separate by skill
    # list of skills to exclude?

    # ranking can draw from all employees, or employees from list

    # create attendance points module that draws from here

    # create ranking module that draws from points module

    # create fml module that loads roles from here, corrects names and file numbers
