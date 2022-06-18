import os
import re

import pandas as pd


class Cleaner:
    def __init__(self):
        self._cleaner = DefaultCleaner()

    def set_cleaner(self, cleaner):
        self._cleaner = cleaner

    def clean(self):
        return self._cleaner.clean()


class DefaultCleaner:
    def __init__(self):
        pass

    def clean(self):
        pass


class CleanerUtils:
    def __init__(self):
        pass

    def combine_raw_files(self, country) -> pd.DataFrame:
        path = f'pipeline/data/{country}'
        raw_files = [f'{path}/{f}' for f in os.listdir(path) if 'raw' in f and "json" in f]
        return pd.concat([pd.read_json(f, orient='records') for f in raw_files])

    def clean_name(self, df):
        """ Cleans name and comma separates first and last. Add patterns to be dropped in name_pattern. """
        name_pattern = r'(^Dr. )|(\sa\s)|(Dra. )|(DRA. )|(Lic. )|(DR. )|(LIC. )|(Drag. )|(Y )'
        df['name'] = (
            df['name'].str.replace(name_pattern, '').str.title().str.strip().apply(lambda x: '' if x == None else x))
        first_name = (
            df['name'].str.split(' ').apply(lambda x: ' '.join(x[:len(x)//2])))
        last_name = (
            df['name'].str.split(' ').apply(lambda x: ' '.join(x[len(x)//2:])))
        return first_name + ', ' + last_name

    def clean_education(self, df):
        def dict_from_string(s):
            try:
                date = ', '.join(re.findall(r'\d{4}', s))
            except:
                date = None

            try:
                issuer = ', '.join(re.findall(r'University[\w\s]*', s))
            except:
                issuer = None

            title = s

            return {
                "date": date,
                "title": title,
                "issuer": issuer
            }
        return (
            df['education']
            .str.replace(r'(\n)|(\xa0)', '')
            .str.strip().str.split('|')
            .apply(lambda x: [i.strip() for i in x] if x is not None else x)
            .apply(lambda x: [dict_from_string(i) for i in x] if x is not None else x)
        )

    def clean_locations(self, df):

        hospitals = pd.read_csv('pipeline/resources/locations.csv')
        hospitals['hoursOperation'] = hospitals.hoursOperation.fillna('None').apply(
            lambda x: eval(x)).apply(lambda x: ', '.join(x) if type(x) == list else x)

        inline_hospitals = hospitals[['locationName', 'location', 'city',
                                      'state', 'country', 'zipCode', 'phoneNumber', 'latitude', 'longitude']]
        inline_hospitals.columns = ['locationName', 'location', 'city',
                                    'state', 'country', 'zipCode', 'phoneNumber', 'latitude', 'longitude']

        inline_hospital_list = inline_hospitals.to_dict(orient='records')

        d = {}
        for h in inline_hospital_list:
            hospital = h['locationName']
            d[hospital] = h
