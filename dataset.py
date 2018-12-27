#!/usr/bin/env python3
import df_utils as du
import report_type as rt
import pandas as pd
import numpy as np

class Dataset:
    HTM_EXT = 'htm'
    
    reports = [rt.Report_Type(rt.Report_Type.EMPLOYEE_LIST, ['Payroll #', 'Name']),
               rt.Report_Type(rt.Report_Type.LEAVE_TAKEN, ['Actual Leave']),
               rt.Report_Type(rt.Report_Type.LEAVE_ENT, ['TEST Attendance Points']),
               rt.Report_Type(rt.Report_Type.PERFORMANCE, ['Competency Score']),
               rt.Report_Type(rt.Report_Type.DEMOGRAPHICS, ['Role / Rate Effective Date'])]

    renamed_cols = {'payroll_#': 'payroll_number',
                    'employee_number': 'payroll_number',
                    'employee_id': 'payroll_number',
                    'role_/_rate_effective_date': 'role_date',
                    'name': 'employee_name',
                    'test_attendance_points': 'points',
                    'roles': 'position',
                    'full_time_/_part_time': 'classification'}

    def __init__(self, filepath):
        self.filepath = filepath
        self.filetype = filepath.split('.')[-1]

        self.df_group = None
        self.df_type = None

        self.df = du.load_data(filepath)
        
        self._identify_data()

        self._format_dataset()

    
    def _identify_data(self):
        header_row = 0
        if self.filetype == self.HTM_EXT:
            df_index = 1
            for i, df in enumerate(self.df):
                for report in self.reports:
                    row, col = np.where(df.values == report.key_cols[0])
                    if len(row) != 0 or set(report.key_cols).issubset(df.columns):
                        self.df_type = report.name
                        df_index = i

            self.df = self.df[df_index]
        else:
            for report in self.reports:
                row, col = np.where(self.df.values == report.key_cols[0])
                if len(row) != 0 or set(report.key_cols).issubset(self.df.columns):
                    self.df_type = report.name
                    if len(row) != 0:
                        header_row = row[0]

        if header_row:
            self.df.rename(columns=self.df.iloc[header_row], inplace=True)
            self.df = self.df[header_row + 1:]
            self.df.reset_index(drop=True, inplace=True)
        

    def _format_dataset(self):
        self.df = du.normalize_columns(self.df)

        self.df.rename(index=str, columns=self.renamed_cols, inplace=True)

        if 'payroll_number' in self.df.columns:
            self.df['payroll_number'] = self.df['payroll_number'] \
            .astype(str).apply('{0:0>6}'.format)
        else:
            self.df['payroll_number'] = self.df['employee_name'].str[1:7]

        if 'role_date' in self.df.columns:
            self.df['role_date'] = pd.to_datetime(self.df['role_date'])
