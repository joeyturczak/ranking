#!/usr/bin/env python3
import file_utils, df_utils
import pandas as pd
import numpy as np


class Dataset:
    DF_TYPES = {'employee_list': 1, 'attendance': 2, 'performance': 3,  'role_date': 4}

    # Columns unique to datasets
    COLUMNS = {}
    COLUMNS['employee_list'] = ['Payroll #', 'Name']
    COLUMNS['attendance'] = ['Actual Leave']
    COLUMNS['performance'] = ['Competency Score']
    COLUMNS['role_date'] = ['Role / Rate Effective Date']

    renamed_cols = {'payroll_#': 'payroll_number',
                    'employee_number': 'payroll_number',
                    'employee_id': 'payroll_number',
                    'role_/_rate_effective_date': 'role_date'}

    def __init__(self, filepath):
        self.filepath = filepath
        self.filetype = filepath.split('.')[-1]

        self.df_group = None
        self.df_type = None

        self.df = df_utils.load_data(filepath)
        
        self._identify_data()

        self._format_dataset()

    
    def _identify_data(self):
        header_row = 0
        if self.filetype == 'htm':
            df_index = 1
            for i, df in enumerate(self.df):
                for key, value in self.COLUMNS.items():
                    row, col = np.where(df.values == value[0])
                    if len(row) != 0 or set(value).issubset(df.columns):
                        self.df_type = self.DF_TYPES[key]
                        df_index = i

            if self.df_type == self.DF_TYPES['employee_list']:
                self.df_group = self._get_group()


            self.df = self.df[df_index]
        else:
            for key, value in self.COLUMNS.items():
                row, col = np.where(self.df.values == value[0])
                if len(row) != 0 or set(value).issubset(self.df.columns):
                    self.df_type = self.DF_TYPES[key]
                    if len(row) != 0:
                        header_row = row[0]

            if self.df_type == self.DF_TYPES['employee_list']:
                self.df_group = self._get_group()

        if header_row:
            self.df.rename(columns=self.df.iloc[header_row], inplace=True)
            self.df = self.df[header_row + 1:]
            self.df.reset_index(drop=True, inplace=True)
            

    
    def _get_group(self):
        if self.filetype == 'htm':
            group = self.df[0].iloc[0, 1]
        else:
            group = self.df.iloc[2, 1]

        group_type = ''

        if 'position' in group:
            group_type = 'position'
        elif 'department' in group:
            group_type = 'department'

        if not group_type:
            print('\nGroup name not found for: {}'.format(self.filepath))
            print(self.df.head())
            group = input('\nPlease enter group name: ')
        else:
            group = group.split('.')[0].strip(' {}'.format(group_type)) \
                .split('the ')[-1].replace(' ', '_').replace('/', '').lower()

        return group
        

    def _format_dataset(self):
        self.df = df_utils.normalize_columns(self.df)

        self.df.rename(index=str, columns=self.renamed_cols, inplace=True)

        if 'payroll_number' in self.df.columns:
            self.df['payroll_number'] = self.df['payroll_number'] \
            .astype(str).apply('{0:0>6}'.format)

        if 'role_date' in self.df.columns:
            self.df['role_date'] = pd.to_datetime(self.df['role_date'])


def format_employee_list(df):
    df[['last_name', 'first_name']] = df['name'] \
        .apply(lambda x: pd.Series(x.split(', ')))
    df = df[['payroll_number', 'last_name', 'first_name']]

    return df


def add_role_dates(df_emp, df_role):
    df_role = df_role[['payroll_number', 'role_date']]
    df = pd.merge(df_emp, df_role, how='left', on='payroll_number')

    return df


def add_performance(df_main, df_perf):
    df_perf = df_perf[['payroll_number', 'competency_score']]
    df_perf.reset_index(drop=True, inplace=True)

    df_perf = pd.merge(df_main, df_perf, how='left', on='payroll_number')
    year = pd.Timestamp.now().year
    perf_date = pd.Timestamp(year=year - 1, month=10, day=1)
    df_perf = df_perf[df_perf['role_date'] < perf_date]

    df_perf = df_perf[['payroll_number', 'competency_score']]

    df = pd.merge(df_main, df_perf, how='left', on='payroll_number')

    return df


def add_attendance(df_main, df_att):
    df_att['payroll_number'] = df_att['employee_name'].str[1:7]

    df_att = df_att[['payroll_number', 'date', 'actual_leave']]
    df_att = pd.merge(df_att, df_main, how='left', on='payroll_number')

    df_att.fillna(method='ffill', inplace=True)

    df_att['date'] = pd.to_datetime(df_att['date'])
    df_att['role_date'] = pd.to_datetime(df_att['role_date'])

    df_att = df_att[df_att['date'] >= df_att['role_date']]

    df_att['points'] = df_att['actual_leave'].str.split('-') \
        .str[-1].str.strip(' ')
    df_att['points'] = df_att['points'] \
        .apply(lambda x: 0.5 if x == '1/2' else x)
    df_att = df_att[df_att['points'] != 'MI']

    df_att['points'] = df_att['points'].astype(float)
    df_att = df_att[['payroll_number', 'date', 'points']]

    df_point_totals = df_att[['payroll_number', 'points']] \
        .groupby(['payroll_number']).sum().reset_index()

    df = pd.merge(df_main, df_point_totals, how='left', on='payroll_number')

    return df


def get_employee_data(df_emp, df_att, df_role, df_perf):

    df = format_employee_list(df_emp)
    df = add_role_dates(df, df_role)
    df = add_performance(df, df_perf)
    df = add_attendance(df, df_att)

    df['role_date'].fillna(pd.Timestamp(0), inplace=True)
    df.fillna(0, inplace=True)
    df['role_date'] = pd.to_datetime(df['role_date'])

    return df