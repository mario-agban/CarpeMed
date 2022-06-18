import pytest
import os
import pandas as pd
from pipeline.common.utils import open_dictionary


def test_column_names():
    """ Format: schema is correct. """
    columns = set(open_dictionary(
        'pipeline/resources/schema.json')['doctor_fields'])
    path = 'pipeline/data'
    countries = os.listdir(path)
    for country in countries:
        if country in ('index.json', '.DS_Store'):
            continue
        files = os.listdir(f'{path}/{country}')

        for f in files:
            if 'clean' not in f:
                continue
            elif f[-4:] == 'json':
                df = pd.read_json(f'{path}/{country}/{f}')
                assert set(df.columns) == columns
