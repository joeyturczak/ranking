#!/usr/bin/env python3
import pandas as pd


def load_data(filepath):
    """
    Retrieves dataset from specified file

    Args:
        (str) filename - file path of dataset
    Returns:
        df - Pandas DataFrame that has been loaded from file
    """
    print('\nLoading data from {}'.format(filepath))

    if filepath.endswith('.csv'):
        df = pd.read_csv(filepath)
    elif filepath.endswith('.htm'):
        df = pd.read_html(filepath, header=0)
    else:
        df = pd.read_excel(filepath)
    return df


def append_dfs(dfs):
    """
    Appends dataframes together.

    Args:
        (list(pandas.DataFrame)) dfs - list of Pandas DataFrames
    Returns:
        df - a single Pandas DataFrame appended together from
             given list of dataframes
    """
    df_r = pd.DataFrame()

    for df in dfs:
        df_r = df_r.append(df, sort=False)

    df_r.reset_index(drop=True, inplace=True)

    return df_r

def df_diff(df1, df2, columns=[]):
    if not columns:
        columns = df2.columns.tolist()
    df1['exist'] = 'exist'
    df = pd.merge(df2, df1, on=columns, how='left')
    df = df[df['exist'].isnull()].drop('exist', axis=1)
    df1.drop(columns=['exist'], inplace=True)
    return df


def normalize_columns(df):
    df = df.loc[:, df.columns.notnull()]
    df.rename(columns=lambda x: x.strip().lower().replace(' ', '_'),
              inplace=True)

    return df