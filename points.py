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

# move this to points module
groups = {'bakery' : ['Bakery - Baker',
                     'Bakery - Lead Baker'],
          'beverage' : ['Bartender - Bartender',
                        'Bartender - Bartender Service',
                        'Beverage - Bar Back',
                        'Beverage - Beverage Cart Server',
                        'Beverage - Beverage Cashier',
                        'Beverage - Beverage Control Stock',
                        'Beverage - Beverage Service Ambassador',
                        'Beverage - Beverage Service Lead',
                        'Beverage - Beverage Utility Clerk',
                        'Beverage - Host(ess) AIG',
                        'Cocktail Servers - Cocktail Server',
                        'Cocktail Servers - Server Marketing Model',
                        'Poker Kitchen - Lead Poker Server',
                        'Poker Kitchen - Poker Busser',
                        'Poker Kitchen - Poker Food Server'],
          'bigmo' : ['Big Mo\' Cafe - Big Mo\'s Cashier',
                     'Big Mo\' Cafe - Big Mo\'s Lead Cashier'],
          'buffet' : ['Buffet - Buffet Busser',
                      'Buffet - Buffet Cashier/Host',
                      'Buffet - Buffet Lead Cashier',
                      'Buffet - Buffet Lead Server',
                      'Buffet - Buffet Server'],
          'chingon' : ['Chingon Kitchen - Chingon Busser',
                       'Chingon Kitchen - Chingon Cashier',
                       'Chingon Kitchen - Chingon Lead Cashier'],
          'culinary' : ['Cook 1 - Cook 1',
                        'Culinary - Cook 2',
                        'Culinary - Cook 3',
                        'Culinary - Lead Cook',
                        'Culinary - Rock n\' Brew Expo'],
          'foodcourt' : ['Food Court - Food Court Bussers',
                         'Food Court - Food Court Cashier',
                         'Food Court - Food Court Lead Cashier'],
          'jbq' : ['JBQ - JBQ Busser',
                   'JBQ - JBQ Cashier'],
          'pines' : ['Pines Steakhouse - Pines Bartender',
                     'Pines Steakhouse - Pines Busser',
                     'Pines Steakhouse - Pines Food Runners',
                     'Pines Steakhouse - Pines Food Server',
                     'Pines Steakhouse - Pines Host/Hostess',
                     'Pines Steakhouse - Pines Lead Server'],
          'rock&brews' : ['Rock & Brews Staff - Rock \'n Brew Bartender',
                          'Rock & Brews Staff - Rock \'n Brew Busser',
                          'Rock & Brews Staff - Rock \'n Brew Cashier/Host(ess)',
                          'Rock & Brews Staff - Rock \'n Brew Food Server',
                          'Rock & Brews Staff - Rock \'n Brew Lead Server',
                          'Rock & Brews Staff - Rock n\' Brew Bar Back',
                          'Rock & Brews Staff - Rock n\' Brew Lead Bartender'],
          'stewarding' : ['Stewarding - Inventory Control Stock',
                          'Stewarding - Lead Steward',
                          'Stewarding - Utility Worker'],
          'tdr' : ['Team Dining Room - TDR Busser',
                   'Team Dining Room - TDR Cashier',
                   'Team Dining Room - TDR Lead Cashier'],
          'tsport' : ['Bussing Assistants - Bussing Assistant',
                      'Bussing Assistants - Charter Bus Asst.',
                      'Limo Drivers - Limo Driver',
                      'Shuttle Drivers - Shuttle Driver',
                      'Transportation - Admin. Assistant',
                      'Valet - Valet Attendant',
                      'Valet - Valet Cashier']}

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


def get_attendance_info(df_emp):
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
            # date = dt.datetime.strftime(date, '%m/%d/%Y')
            dates.append(date)
    
    df_att_lt_old = pd.DataFrame()
    df_att_lt_old = [x.df for x in datasets \
        if x.df_type == report_type.Report_Type.LEAVE_TAKEN][-2]

    df_att_lt_new = pd.DataFrame()
    df_att_lt_new = [x.df for x in datasets \
        if x.df_type == report_type.Report_Type.LEAVE_TAKEN][-1]

    print("\nLoading all files took {} seconds.".format(time.time() - start_time))

    previous_col = 'previous_point_total_' + dt.datetime.strftime(get_start_date(dates[-2]), '%m/%d/%Y') + '_to_' + dt.datetime.strftime(dates[-2], '%m/%d/%Y')
    current_col = 'current_point_total_' + dt.datetime.strftime(get_start_date(dates[-1]), '%m/%d/%Y') + '_to_' + dt.datetime.strftime(dates[-1], '%m/%d/%Y')

    df_old = get_attendance_from_leave_taken(df_emp, df_att_lt_old)
    df_old.rename(index=str, columns={'points' : previous_col}, inplace=True)
    df_old = df_old[['payroll_number', previous_col]]
    df_new = get_attendance_from_leave_taken(df_emp, df_att_lt_new)
    df_new.rename(index=str, columns={'points' : current_col}, inplace=True)

    df = pd.merge(df_new, df_old, how='left', on='payroll_number')

    df[[current_col, previous_col]] = df[[current_col, previous_col]].fillna(0)

    df['change'] = df[current_col] - df[previous_col]

    df = df[['payroll_number', 'last_name', 'first_name', 'position', 'group', 'most_recent_occurrence', 'most_recent_occurrence_type', previous_col, current_col, 'change']]

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
    df_emp = employee.add_groups_by_position(employee.get_employee_info(), groups)

    df_emp.dropna(subset=['group'], inplace=True)

    df = get_attendance_info(df_emp)

    for group in groups:
        df_group = df[df['group'] == group]

        df_group.drop(columns=['group'], inplace=True)

        filename = out_dir + pd.Timestamp.now().strftime('%Y%m%d%H%M') + '_' + group + '_points'

        # Save raw file
        df_group.to_csv(filename + '_attendance.csv', index=False)

        print(df_group)