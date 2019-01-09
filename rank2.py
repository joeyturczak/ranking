#!/usr/bin/env python3
import pandas as pd
import file_utils
import os

# Current working directory
CUR_DIR = os.getcwd()

CONFIGURATION = CUR_DIR + '/rank2.conf'

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
    conf = file_utils.read_conf_file(CONFIGURATION, ['in', 'out'])
    
    if 'in' in conf:
        global in_dir
        in_dir = conf['in']

    if 'out' in conf:
        global out_dir
        out_dir = conf['out']

    create_dirs()

if __name__ == '__main__':
    setup()
