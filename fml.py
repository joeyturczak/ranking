import pandas as pd
import os
import file_utils, df_utils
import datetime as dt
from operator import itemgetter

# Current working directory
CURRENT_DIR = os.getcwd()

# Directories for input and output -- change to desired locations
FML_TYPES = [{'type': 'block',
              'output_dir': CURRENT_DIR + '/output/fml/blocks/',
              'scan_dir': CURRENT_DIR + '/data/fml/blocks/'},
             {'type': 'intermittent',
              'output_dir': CURRENT_DIR + '/output/fml/intermittent/',
              'scan_dir': CURRENT_DIR + '/data/fml/intermittent/'}]

def create_dirs():
    for fml_type in FML_TYPES:
        file_utils.create_dir(fml_type['output_dir'])
        file_utils.create_dir(fml_type['scan_dir'])


def read_data(filename):
    df = pd.read_excel(filename)
    df = df.rename(columns={' Start Date': 'Start Date', 'Expected  End Date': 'Expected End Date'})
    df['Comments'].fillna('', inplace=True)
    df['Comments'] = df['Comments'].astype(str)
    return df


if __name__ == '__main__':
    create_dirs()

    for fml_type in FML_TYPES:
        departments = file_utils.get_subdirectories(fml_type['scan_dir'])

        for department in departments:
            department_dir = fml_type['scan_dir'] + department + '/' 
            files = file_utils.get_files_list(department_dir)
            file_dates = []
            for f in files:
                date = dt.datetime.strptime(''.join(c for c in f if not c.isalpha() and c not in '&_ '), '%m.%d.%y.').date()
                date = dt.datetime.strftime(date, '%Y%m%d')
                file_dates.append({'filename': f, 'date': date})

            file_dates = sorted(file_dates, key=itemgetter('date'))

            if len(file_dates) > 1:
                for index, f in enumerate(file_dates, 1):
                    if index < len(file_dates):
                        old_date = f['date']
                        new_date = file_dates[index]['date']
                        dep_out_dir = fml_type['output_dir'] + department + '/'
                        file_utils.create_dir(dep_out_dir)
                        filename = dep_out_dir + department + '_' + old_date + '_to_' + new_date + '_diff.csv'
                        if not os.path.isfile(filename):
                            old_df = read_data(department_dir + f['filename'])
                            new_df = read_data(department_dir + file_dates[index]['filename'])
                            
                            diff_df = df_utils.df_diff(old_df, new_df)
                            diff_df.drop(diff_df.columns[diff_df.columns.str.contains('Unnamed', case=False)], axis=1, inplace=True)
                            diff_df.to_csv(filename, index=False)

                            print('\nSaving to file {}'.format(filename))