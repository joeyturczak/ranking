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

EXCLUDE_DIRS = ['old']

FML_TYPES = ['blocks', 'intermittent']

def create_dirs():
    for fml_type in FML_TYPES:
        file_utils.create_dir(OUTPUT_DIR)
        file_utils.create_dir(SCAN_DIR + fml_type + '/')


def read_data(filename):
    df = pd.read_excel(filename)
    df.drop(df.columns[df.columns.str.contains('Unnamed', case=False)], axis=1, inplace=True)

    df = df.rename(columns={' Start date': 'Start date', 'Expected  End date': 'Expected End date'})
    if 'Comments' in df.columns:
        df['Comments'].fillna('', inplace=True)
        df['Comments'] = df['Comments'].astype(str)

    df = df_utils.normalize_columns(df)
    # TODO Column names
    df[''].apply(lambda x: x.astype(str).str.lower())
    return df


if __name__ == '__main__':
    create_dirs()

    for fml_type in FML_TYPES:
        scan_dir = SCAN_DIR + fml_type + '/'
        departments = file_utils.get_subdirectories(scan_dir)

        for department in departments:
            department_dir = scan_dir + department + '/' 
            files = file_utils.get_files_list(department_dir, exclude_dirs=EXCLUDE_DIRS)
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
                filename = dep_out_dir + department + '_comprehensive.csv'

                comp_df = pd.DataFrame()
                for index, f in enumerate(file_dates, 1):
                    df = read_data(department_dir + f['filename'])
                    df['date'] = f['date']

                    comp_df = comp_df.append(df)

                first_df = comp_df.sort_values(by=['date'], ascending=True)
                first_df.reset_index(drop=True, inplace=True)
                first_df = first_df.rename(index=str, columns={'date': 'first_appearance'})
                first_df = first_df.drop_duplicates(subset=[col for col in first_df.columns if col not in ['first_appearance']])
                last_df = comp_df.sort_values(by=['date'], ascending=False)
                last_df.reset_index(drop=True, inplace=True)
                last_df = last_df.rename(index=str, columns={'date': 'last_appearance'})
                last_df = last_df.drop_duplicates(subset=[col for col in last_df.columns if col not in ['last_appearance']])

                comp_df = pd.merge(first_df, last_df)
                comp_df = comp_df.sort_values(by=['first_appearance'], ascending=False)

                # TODO reorganize columns

                comp_df.to_csv(filename, index=False)

                print('\nSaving to file {}'.format(filename))
