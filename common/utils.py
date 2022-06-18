import pandas as pd
import json
import os
import pipeline.paths
from datetime import datetime, date

# Files


def generate_filename(path, *argv):
    date = datetime.utcnow().strftime("%Y%m%d")
    args = '_'.join(argv)
    return f'{path}/{date}_{args}'


def get_most_recent_file(path):
    """ Returns the most recent file, where files in folder are named 'YYYYMMDD_filename.ext' """
    names = [i[8:] for i in os.listdir(path)]
    dates = [datetime.strptime(i[:8], '%Y%m%d')
             for i in os.listdir(path) if i[:8].isdigit()]
    pivot = datetime.utcnow()
    latest_date = min(dates, key=lambda x: abs(x - pivot))
    return f'{latest_date.strftime("%Y%m%d")}{names[0]}'


def save_dictionary(dictionary, path):
    f = open(path, 'w', encoding='utf-8')
    f.write(json.dumps(dictionary))
    f.close()


def open_dictionary(path):
    with open(path, encoding='utf-8') as json_file:
        return json.load(json_file)


def update_index(update_type, country_name, hospital_name=None, debug_mode=False):
    if debug_mode:
        return
    index_path = pipeline.paths.INDEX_PATH
    index = open_dictionary(index_path)
    date = datetime.utcnow().strftime("%Y%m%d")
    if update_type == 'raw':
        index[country_name][update_type][hospital_name] = date
    else:
        index[country_name][update_type] = date
    save_dictionary(index, index_path)

def save_export(file_name, df): 
    date_string = date.today().strftime("%m%d%y")
    folder_path = f'pipeline/data/exports/{date_string}'

    if not os.path.isdir(folder_path):
        os.mkdir(folder_path)
    path = folder_path + '/' + file_name + f'_{date_string}'

    df.to_json(f'{path}.json', orient='records')
    df.to_csv(f'{path}.csv')
