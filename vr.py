#!/usr/bin/env python3
import file_utils, df_utils
import pandas as pd


class Dataset:
    EMPLOYEE_LIST = 'employee_list'
    ATTENDANCE = 'attendance'
    PERFORMANCE = 'performance'
    ROLE_DATE = 'role_date'

    emp_htm_cols = 'only includes employees'
    att_htm_cols = 'report is sorted'
    emp_report = 'Employee Settings Report'
    role_report = 'Virtual Roster Employee'
    att_report = 'Leave Taken'
    role_cols = ['Payroll Number', 'Role / Rate Effective Date']
    perf_cols = ['Employee Number', 'Competency Score']
    att_cols = ['Employee Number', 'Actual Leave']
    emp_cols = ['Payroll #', 'Name']

    renamed_cols = {'payroll_#': 'payroll_number',
                    'employee_number': 'payroll_number',
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
            first_col = self.df[0].columns[0]
            if self.emp_htm_cols in first_col:
                self.df_type = self.EMPLOYEE_LIST
                self.df_group = self._get_group()
            elif self.att_htm_cols in first_col:
                self.df_type = self.ATTENDANCE
            
            self.df = self.df[1]
        else:
            first_col = self.df.columns[0]
            if self.emp_report in first_col:
                header_row = 5
                self.df_type = self.EMPLOYEE_LIST
                self.df_group = self._get_group()
            elif set(self.emp_cols).issubset(self.df.columns):
                self.df_type = self.EMPLOYEE_LIST
            elif self.att_report in first_col:
                header_row = 7
                self.df_type = self.ATTENDANCE
            elif set(self.att_cols).issubset(self.df.columns):
                self.df_type = self.ATTENDANCE
            elif self.role_report in first_col:
                header_row = 2
                self.df_type = self.ROLE_DATE
            elif set(self.role_cols).issubset(self.df.columns):
                self.df_type = self.ROLE_DATE
            elif set(self.perf_cols).issubset(self.df.columns):
                self.df_type = self.PERFORMANCE

        if header_row:
            self.df.rename(columns=self.df.iloc[header_row], inplace=True)
            self.df = self.df[header_row + 1:]
            self.df.reset_index(drop=True, inplace=True)
            

    
    def _get_group(self):
        if self.filetype == 'htm':
            group = self.df[0].iloc[0, 1]
        else:
            group = self.df.iloc[2, 1]

        if 'position' in group:
            group_type = 'position'
        elif 'department' in group:
            group_type = 'department'

        group = group.split('.')[0].strip(' {}'.format(group_type)) \
            .split('the ')[-1].replace(' ', '_').lower()

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