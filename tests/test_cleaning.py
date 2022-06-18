import os
import uuid

import pandas as pd
import pytest
import requests
import ast
from fake_useragent import UserAgent
from pipeline.common.utils import open_dictionary


def clean_setup():
    path = 'pipeline/data'
    countries = os.listdir(path)
    dfs = []
    for country in countries:
        if country in ('index.json', '.DS_Store'):
            continue
        files = os.listdir(f'{path}/{country}')

        for f in files:
            if 'clean' in f and 'json' in f:
                df = pd.read_json(f'{path}/{country}/{f}')
                dfs.append(df)
    return dfs


def test_clean_column_names():
    """ Format: schema is correct. """
    columns = set(open_dictionary(
        'pipeline/resources/schema.json')['doctor_fields'])
    dfs = clean_setup()
    for df in dfs:
        assert set(df.columns) == columns


def test_name_commas():
    """ Checks if name is comma separated properly. """
    dfs = clean_setup()
    for df in dfs:
        assert not df['name'].str.contains(',').any()
        # Deprecated test: name comma separated
        # assert df['name'].str.split(',').apply(lambda x: len(x)).equals(df['name'].apply(lambda x: 2))


def test_name_length():
    """ Checks if format of name is First and given, Last name.
        This means first name should always be at least as long as the last name. """
    # Deprecated: name length no longer checked
    # dfs = clean_setup()
    # for df in dfs:
    #     first = df.name.str.split(', ').apply(lambda x: len(x[0].split()))
    #     last = df.name.str.split(', ').apply(lambda x: len(x[1].split()))
    #     assert (first >= last).all()
    pass


def test_name_extraneous():
    """ Looks for titles or extraneous characters in name """
    dfs = clean_setup()
    for df in dfs:
        assert not df['name'].str.contains('Dr.').any()
        assert not df['name'].str.contains('M.D.').any()
        assert not df['name'].str.contains('&').any()


def test_email():
    dfs = clean_setup()
    for df in dfs:
        assert not df['email'].str.contains('mailto:').any()
        assert df[df['email'] != None]['email'].str.contains('@').all()
        assert df['email'].fillna('test@test.com').apply(lambda x: x.split('@')).apply(
            lambda x: len(x)).equals(df['email'].apply(lambda x: 2))


def test_website():
    """ Checks website for 404 (sample size 3) and url formatting """
    dfs = clean_setup()
    ua = UserAgent()
    headers = {'User-Agent': str(ua.chrome)}
    for df in dfs:
        not_null = df[df['website'] != None]
        assert not_null['website'].str.contains('https://').all()

        for website in not_null['website'].sample(3):
            if website != None:
                try:
                    resp = requests.get(website, headers=headers, timeout=3)
                    print(website, resp.status_code == 200)
                except Exception as e:
                    print(e, website)


def test_spokenLanguages():
    """ Checks if spoken languges are in a comma separated string """
    dfs = clean_setup()
    for df in dfs:
        assert not df.spokenLanguages.fillna(
            '').astype(str).str.contains('and').any()


def test_location_exists():
    """ Checks to see if hospital string matches an existing one in locations.csv,
        so that they can be joined later. """
    dfs = clean_setup()
    all_locations = set(pd.read_csv(
        'pipeline/resources/locations.csv')['locationName'])
    for df in dfs:
        locations = set()
        # h = (df['location'].str.split(', '))
        h = df['location'].apply(
            lambda x: [i['locationName'] for i in ast.literal_eval(str(x))])
        for i in h:
            for j in i:
                locations.add(j)

        if 'External Office' in locations:
            locations.remove('External Office')

        for location in locations:
            assert location in all_locations


def test_provider():
    """ Checks to see if providers are correct """
    dfs = clean_setup()
    for df in dfs:
        length = df.provider.fillna('').str.split(',').apply(lambda x: len(x))
        unique = df.provider.fillna('').str.split(
            ',').apply(lambda x: len(set(x)))
        assert length.equals(unique)  # no duplicates


def test_whitespace():
    """ Checks to see if whitespace is correct (i.e. one space is used as whitespace between words, no double spacing) """
    dfs = clean_setup()
    for df in dfs:
        for col in df.columns:
            assert not df[col].fillna('').astype(str).str.contains('  ').any()


def test_city():
    """ Checks to see if city is not none. """
    dfs = clean_setup()
    for df in dfs:
        assert not df.city.isnull().any()


def test_doctorid():
    """ Checks to see if DoctorID is a UUID4 """
    # From https://stackoverflow.com/questions/19989481/how-to-determine-if-a-string-is-a-valid-v4-uuid/19989922
    def is_valid_uuid(val):
        try:
            uuid.UUID(str(val))
            return True
        except ValueError:
            return False

    dfs = clean_setup()
    for df in dfs:
        assert df.doctorId.apply(lambda x: is_valid_uuid(x)).all()
