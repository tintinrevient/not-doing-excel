import os
import pandas as pd
import re
import glob
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import config
import logging

logger=logging.getLogger(__name__)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

fh = logging.FileHandler('logs/prepare.log')
fh.setLevel(logging.ERROR)
fh.setFormatter(formatter)

logger.addHandler(fh)


def _get_skiprows(sheet_name: str) -> int:
    if sheet_name in ['Summary Data', 'Non Responders']:
        skiprows = 0
    elif sheet_name in ['Full Question Mapping']:
        skiprows = 3
    else:
        skiprows = 1

    return skiprows


def map_one_sheet_to_one_csv(file_name: str, sheet_name: str, sheet_idx: int = 0, total_size: int = 1):
    try:
        print(f'Preparing the sheet {sheet_idx + 1}/{total_size}: \t{sheet_name} \tfrom the file: \t{file_name}')

        # ensure the first row => pandas header
        skiprows = _get_skiprows(sheet_name=sheet_name)
        df = pd.read_excel(io=file_name, engine='openpyxl', sheet_name=sheet_name, skiprows=skiprows)

        df.rename(
            columns={
                x: x[x.rindex(' - ')+1:].strip() if ' - ' in x else x.strip() for x in df.columns.values.tolist()
            },
            inplace=True,
        )

        df.to_csv(f'{base_output_dir}/{sheet_name}.csv', mode='x', encoding='utf-8', header=True, index=False)

        print(f'Generated {sheet_idx + 1}/{total_size}: \t{sheet_name}.csv \tin the folder: \t{base_output_dir}')

    except Exception as ex:
        logger.error(f'While processing {sheet_name} of {file_name}, an exception occurs: {ex}')


def prepare_one_file(file_name: str):
    try:
        xl = pd.ExcelFile(path_or_buffer=file_name, engine='openpyxl')

        # ignore non-data sheets
        sheet_names = list(set(xl.sheet_names) - set(ignored_sheet_names))
        total_size = len(sheet_names)

        # flavour 1: for debug purpose
        # for idx, sheet_name in enumerate(sorted(sheet_names)):
        #     map_one_sheet_to_one_csv(file_name=file_name, sheet_name=sheet_name, idx=idx, total_size=total_size)

        # flavour 2: for multi-thread execution
        with ThreadPoolExecutor(max_workers=n_threads) as executor:
            futures = [executor.submit(map_one_sheet_to_one_csv, file_name, sheet_name, sheet_idx, total_size) for sheet_idx, sheet_name in enumerate(sorted(sheet_names))]

            for future in as_completed(futures):
                print(future)

    except Exception as ex:
        logger.error(f'While processing {file_name}, an exception occurs: {ex}')


if __name__ == "__main__":
    # time the execution
    start_time = time.monotonic()

    # load configs
    config_map = config.load('config/config.yml')
    print(config_map)

    base_input_dir = config_map['prepare']['base_input_dir']
    base_output_dir = config_map['prepare']['base_output_dir']
    ignored_sheet_names = config_map['prepare']['ignored_sheet_names']
    n_threads = config_map['prepare']['n_threads']

    # clean up all the data under the folder 'data/prepared'
    for file in glob.glob(f'{base_output_dir}/*.csv', recursive=True):
        os.remove(file)

    # prepare the data
    xlsx_file_names = sorted(glob.glob(f'{base_input_dir}/*.xlsx', recursive=True))
    for file_name in xlsx_file_names:
        prepare_one_file(file_name=file_name)

    end_time = time.monotonic()
    print(f'Total execution time: {int((end_time - start_time)/60)} min')
