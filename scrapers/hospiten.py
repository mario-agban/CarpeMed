import json

import numpy as np
import pandas as pd
import requests
import tqdm
import pipeline.common.utils
from bs4 import BeautifulSoup
from pipeline.common.translator import Translator


class Hospiten:
    def __init__(self, metadata: dict):
        self.metadata = metadata
        self.module_id = metadata['module_id']
        self.module_id = metadata['module_id']
        self.page_size = metadata['page_size']
        self.tab_id = metadata['tab_id']
        self.center_list = metadata['center_list']
        self.headers = {
            'TabId': self.tab_id,
            'Content-Type': 'application/json; charset=UTF-8',
            'ModuleId': self.module_id,
        }

    def scrape(self):
        url = 'https://hospiten.com/en/API/Hospiten/Professional/GetProfessional'
        doctor_jsons = []
        doctor_ids = self._get_doctor_ids()
        for doctor_id in tqdm.tqdm(doctor_ids, f'Scraping {self.metadata["hospital_name"]} doctors'):
            payload = {
                "ModuleId": int(self.module_id),
                "ProfessionalId": doctor_id,
                "Culture": "en-US",
                "UserLocation": None
            }
            resp = requests.post(url, headers=self.headers,
                                 data=json.dumps(payload))
            try:
                doctor_json = resp.json()
                doctor_jsons.append(doctor_json)
            except Exception as e:
                print(e)
        df = pd.json_normalize(i['Professional'] for i in doctor_jsons)
        df = self._format_hospiten(df)

        if self.metadata['language'] != 'en':
            translator = Translator(df, self.metadata['hospital_short_name'])
            df = translator.translate()

        return df

    def _get_doctor_ids(self):
        """ POST request to get doctor ids from module_id (hospital). """
        url = 'https://hospiten.com/en/API/Hospiten/Professional/GetProfessionals'
        data = {
            "ModuleId": int(self.module_id),
            "Culture": "en-US",
            "PageSize": int(self.page_size),
            "CurrentPage": 1,
            "SortColumn": "",
            "SortOrder": "ASC",
            "CountryList": [],
            "CenterList": [{"Id": int(self.center_list)}],
            "SpecialtyList": []
        }
        resp = requests.post(
            url, headers=self.headers, data=json.dumps(data))
        self.status_code = resp.status_code
        doctor_ids = [i['ProfessionalId']
                      for i in resp.json()['Professionals']]
        return doctor_ids

    def _format_hospiten(self, df):
        """ Format json data from hospiten POST request. """
        df = df[['Name', 'LastName', 'Specialties', 'DetailUrl',
                 'EducationInfo', 'ImageUrl', 'MemberOfSocieties']]
        df = df.rename(columns={'DetailUrl': 'website', 'Specialties': 'provider',
                                'EducationInfo': 'education', 'ImageUrl': 'photoUrl', 'MemberOfSocieties': 'additionalInformation'})

        df['name'] = df['LastName'] + ' ' + df['Name']
        df['provider'] = df['provider'].apply(
            lambda x: x[0]['Name'] if len(x) > 0 else None)

        def clean_html(x): return BeautifulSoup(x, features='lxml').get_text()
        df['education'] = (
            df['education'].apply(clean_html)
            .str.replace(r'\t|\n', ' ').str.replace(r'\s+', ' ').str.strip())
        df['additionalInformation'] = (
            df['additionalInformation'] .apply(clean_html)
            .str.replace(r'\t|\n', ' ').str.replace(r'\s+', ' ').str.strip())

        df['location'] = self.metadata['hospital_name']
        df['city'] = self.metadata['city']
        df['country'] = self.metadata['country']
        df['confirmedHours'] = False

        df = df.drop(['Name', 'LastName'], axis=1)
        col = df.pop('name')
        df.insert(0, col.name, col)

        header_list = pipeline.common.utils.open_dictionary(
            'pipeline/resources/schema.json')['doctor_fields']
        df = df.reindex(columns=header_list)
        df = df.replace(np.nan, None, regex=True)
        df = df.where(pd.notnull(df), None)
        return df
