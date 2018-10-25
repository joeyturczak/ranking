import pandas as pd
import os
import file_utils, df_utils
import datetime as dt
from operator import itemgetter

# Current working directory
CURRENT_DIR = os.getcwd()

# Output directory - change to desired location
OUTPUT_DIR = CURRENT_DIR + '/output/fml/'

# Directory to scan for data input files - change to desired location
SCAN_DIR = CURRENT_DIR + '/data/fml/'

FML_TYPES = ['blocks', 'intermittent']

def create_dirs():
    for fml_type in FML_TYPES:
        file_utils.create_dir(OUTPUT_DIR)
        file_utils.create_dir(SCAN_DIR + fml_type + '/')


def read_data(filename):
    df = pd.read_excel(filename)
    df = df.rename(columns={' Start Date': 'Start Date', 'Expected  End Date': 'Expected End Date'})
    if 'Comments' in df.columns:
        df['Comments'].fillna('', inplace=True)
        df['Comments'] = df['Comments'].astype(str)

    df.drop(df.columns[df.columns.str.contains('Unnamed', case=False)], axis=1, inplace=True)
    return df


if __name__ == '__main__':
    create_dirs()

    for fml_type in FML_TYPES:
        scan_dir = SCAN_DIR + fml_type + '/'
        departments = file_utils.get_subdirectories(scan_dir)

        for department in departments:
            department_dir = scan_dir + department + '/' 
            files = file_utils.get_files_list(department_dir)
            file_dates = []
            for f in files:
                if not f.startswith('~'):
                    date = dt.datetime.strptime(''.join(c for c in f if not c.isalpha() and c not in '&_ '), '%m.%d.%y.').date()
                    date = dt.datetime.strftime(date, '%Y%m%d')
                    file_dates.append({'filename': f, 'date': date})

            file_dates = sorted(file_dates, key=itemgetter('date'))

            if len(file_dates) > 1:
                dep_out_dir = OUTPUT_DIR + department + '/' + fml_type + '/'
                file_utils.create_dir(dep_out_dir)
                for index, f in enumerate(file_dates, 1):
                    if index < len(file_dates):
                        old_date = f['date']
                        new_date = file_dates[index]['date']
                        
                        filename = dep_out_dir + department + '_' + old_date + '_to_' + new_date + '_diff.csv'
                        if not os.path.isfile(filename):
                            old_df = read_data(department_dir + f['filename'])
                            new_df = read_data(department_dir + file_dates[index]['filename'])
                            
                            diff_df = df_utils.df_diff(old_df, new_df)
                            diff_df.to_csv(filename, index=False)

                            print('\nSaving to file {}'.format(filename))