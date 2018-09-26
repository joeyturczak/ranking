#!/usr/bin/env python3
import pathlib, os


def create_dir(directory):
    pathlib.Path(directory).mkdir(parents=True, exist_ok=True)


def get_files_list(directory='/', extensions='',
                   startswith='', abs_path=False, sub_dirs=False, exclude_dirs=[]):
    """
    Retrieves a list of files for a given directory.

    Args:
        (str) directory - path of directory to find files.
              defaults to root directory.
        (Tuple) extensions - extensions to include.
                includes all extensions by default.
        (str) startswith - file name must begin with this string.
        (bool) abs_path - set to true if provided directory is an
               absolute path. Defaults to false for relative path.
        (bool) sub_dirs - set to true to include files in sub directories.
               Defaults to false.
        (list) exclude_dirs - list of directories to exclude. This only applies if sub_dirs is True
    Returns:
        files - List of files in specified directory
    """
    file_dir = ''
    if abs_path:
        file_dir = directory
    if sub_dirs:
        files = [os.path.join(root, f)
                 for root, dirs, files in os.walk(directory)
                 for f in files
                 if f.startswith(startswith) and f.endswith(extensions)]
        
        files[:] = [f for f in files if os.path.basename(os.path.dirname(f)) not in exclude_dirs]
    else:
        files = [file_dir + f for f in os.listdir(directory)
                 if f.startswith(startswith) and f.endswith(extensions)]
    return files


def get_subdirectories(directory):
    return [name for name in os.listdir(directory) \
            if os.path.isdir(os.path.join(directory, name))]
