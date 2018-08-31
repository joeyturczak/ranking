import pandas as pd
import tkinter as tk
from tkinter import filedialog
import os

root = tk.Tk()
root.withdraw()
current_directory = os.getcwd()

def clean_dataset(df):
    return df

def get_file_path():
    file_path = filedialog.askopenfilename(initialdir = current_directory, 
                                       title = 'Select file', 
                                       filetypes = (('All files', '*.xls;*.xlsx;*.csv;*.htm'), 
                                                    ('Excel files', '*.xls;*.xlsx'), 
                                                    ('CSV files', '*.csv'), 
                                                    ('HTM files', '*.htm')))
    print('\nSelected file: {}'.format(file_path))
    return file_path

if __name__ == '__main__':
    employee_list_file = get_file_path()
