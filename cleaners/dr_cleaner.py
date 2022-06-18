import json
import math
import re
import uuid

import pandas as pd
import pipeline.common.utils as utils
from pipeline.cleaners.cleaner import CleanerUtils


class DominicanRepublicCleaner:
    def __init__(self):
        self.country = 'dr'
        self.cleaner = CleanerUtils()

    def clean(self):
        """ Main function to add cleaning functionality.
            Either create a custom cleaning function in this class,
            or use a cleaning util function from the CleanerUtils class. """

        df = self.cleaner.combine_raw_files(self.country)
        df = self._initial_formatting(df)

        # clean columns
        df['name'] = self._clean_name(df)

        df = df[~df['name'].str.contains('#Value!,')]
        df = df[df['name'] != ', ']

        df['provider'] = self._clean_provider(df)
        df['spokenLanguages'] = self._clean_language(df)
        # df['location'] = self._clean_location(df)
        df['email'] = self._clean_email(df)
        df['education'] = self._clean_education(df)
        df['otherActivities'] = self._clean_otherActivities(df)
        df['photoUrl'] = self._clean_photoUrl(df)
        # df['city'] = self._clean_city(df)
        df['doctorId'] = self._clean_doctorId(df)
        df['additionalInformation'] = self._clean_additionalInformation(df)

        def replace_whitespace(x):
            if type(x) == str:
                return x.replace(r'\s+', ' ')
            else:
                return x
        df = df.applymap(lambda x: replace_whitespace(x))

        df = self._additional_fix(df)
        return df

    def _initial_formatting(self, df):
        df = df.reset_index().drop(['index'], axis=1)
        df = df.where(pd.notnull(df), None)
        df = df.drop_duplicates(subset='name')
        return df.reset_index().drop(['index'], axis=1)

    def _clean_provider(self, df):
        providers = utils.open_dictionary(
            'pipeline/resources/providers_remapper.json')

        prov = (
            df.provider
            .str.replace(r'\n|\r', '')
            .str.replace(',', '|')
            .str.title().str.split('|')
            .apply(lambda x: x if x is not None else [])
            .apply(lambda x: [i.strip() for i in x])
            .apply(lambda x: [i for i in x if i not in (None, 'And', '', ' ')])
            .apply(lambda x: [i.replace(r' ', '') for i in x])
            .apply(lambda x: [i if i is not None else [''] for i in x])
            .apply(lambda x: [providers[i] if i in providers else i for i in x])
            .apply(lambda x: [i for i in x if i is not None])
            .apply(lambda x: [i.split(', ') for i in x])
            .apply(lambda x: [j for i in x for j in i])
            .apply(lambda x: sorted(set(x)))
            .apply(lambda x: ', '.join((x)))
        )
        return prov.str.replace('  ', ' ')

    def _clean_language(self, df):
        df['spokenLanguages'] = df['spokenLanguages'].str.replace("and", ",").str.replace('  ', ' ').str.replace(
            '.', '').str.split(',').apply(lambda x: ', '.join(i.strip() for i in x) if x is not None else x)
        df['spokenLanguages'] = (
            df.spokenLanguages.fillna('Spanish')
        )
        return df.spokenLanguages

    def _clean_description(self, df):
        return (df['description'].str.replace(r'\n', '').str.strip()
                .str.replace(u'\xa0', ' ').str.split('|')
                .apply(lambda x: ', '.join(x) if x is not None else x)).str.replace('  ', ' ')

    def _clean_email(self, df):
        return (
            df['email']
            .astype(str).str.replace(r"( )|(')", "")
            .str.replace('mailto:', '')
            .str.replace('Malto:', '')
            .str.replace('Email:', '')
            .str.replace('\t', '')
            .str.replace('\n', '')
            .str.replace('हबिब्गिनेचो@होत्मैल.कॉम', '')
            .str.replace(' जैमेक्लेइमन@याहू.कॉम', '')
            .str.replace('की_क्चल@याहू.कॉम', '')
            .str.replace('डॉ.रहलमिर@जीमेल.कॉम', '')
            .str.replace(' चर्मसोफ़@होत्मैल.कॉम', '')
            .str.replace('चर्मसोफ़@होत्मैल.कॉम', '')
            .str.replace('जुअन्सावेद्र@याहू.कॉम', '')
            .str.replace('जैमेक्लेइमन@याहू.कॉम', '')
            .str.strip()
            .str.split('|').apply(lambda x: ', '.join(x) if type(x) == list else x)
            .apply(lambda x: x if len(x.split('@')) == 2 and '' not in x.split('@') else None)
        ).str.replace(' ', '')

    # TODO: Pending -- need to find out right schema
    def _clean_education(self, df):
        #  def dict_from_string(s):
        #      try:
        #          date = ', '.join(re.findall(r'\d{4}', s))
        #      except:
        #          date = None

        #      try:
        #          issuer = ', '.join(re.findall(r'University[\w\s]*', s))
        #      except:
        #          issuer = None

        #      title = s

        #      return {
        #          "date": date,
        #          "title": title,
        #          "issuer": issuer
        #      }
        #  return (
        #      df['degrees']
        #      .str.replace(r'(\n)|(\xa0)', '')
        #      .str.strip().str.split('|')
        #      .apply(lambda x: [i.strip() for i in x] if x is not None else x)
        #      .apply(lambda x: [dict_from_string(i) for i in x] if x is not None else x)
        #  )
        return (
            df['education'].str.strip()
            .str.replace('|', ',')
            .str.replace('\r\n', '')
            .apply(lambda x: re.sub('\s+', ' ', x) if type(x) == str else x))

    def _clean_additionalInformation(self, df):
        return (
            df['additionalInformation']
            .fillna('')
            .str.strip()
            .str.replace('|', ',')
            .str.replace('\r\n', '')
            .str.replace('  ', ' ')
            .apply(lambda x: re.sub('\s+', ' ', x) if type(x) == str else x)
            .str.replace(r'\n', '')
            .str.replace(r'â', '').str.replace(r'?', '')
            .str.strip().str.split('|')
            .replace({'': None})
            .apply(lambda x: ', '.join(x) if type(x) == list else x).apply(lambda x: re.sub('\s+', ' ', x) if type(x) == str else x)
            .str.replace('  ', ' '))

    # DELETE
    # def _clean_certifications(self, df):
       # return (
        # df.certifications.fillna('')
        # .apply(lambda x: ', '.join(x))
        # .replace({'': None}))

    # DELETE
    # def _clean_specialCourses(self, df):
        # return (
        # df['specialCourses'].str.replace(r'\n', '')
        # .str.replace(r'â', '').str.replace(r'?', '')
        # .str.strip().str.split('|')
        # .apply(lambda x: ', '.join(x) if type(x) == list else x)).apply(lambda x: re.sub('\s+', ' ', x) if type(x) == str else x)

    def _clean_otherActivities(self, df):
        return(
            df['otherActivities']
            .str.replace('\s+', ' ', regex=True)
            .str.strip()
            .str.split('|')
            .apply(lambda x: ', '.join([i.strip() for i in x]) if x is not None else x)
        )
        # return df['otherActivities'].str.replace('[', "").replace(']', "").replace('\'', "").str.replace('  ', ' ')

    def _clean_location(self, df):
        hospitals_remapper = utils.open_dictionary(
            'pipeline/resources/hospitals_remapper.json')
        location = (
            df.location.fillna('')
            .replace('\t|\n', '', regex=True)
            .str.split('|')
            .apply(lambda x: [i.strip() for i in x])
            .apply(lambda x: [hospitals_remapper[i] for i in x if i in hospitals_remapper])
            .apply(lambda x: ', '.join(x))
            .replace('', None)
        )
        return location.str.replace('  ', ' ')

    def _clean_city(self, df):
        locations = utils.open_dictionary('pipeline/resources/locations.json')
        df['city'] = (
            df['location'].str.split(', ')
            .apply(lambda x: x[0])
            .apply(lambda x: locations[x]['city']))
        return df.city

    def _clean_photoUrl(self, df):
        return (
            df.photoUrl.fillna('')
            .str.split(' ')
            .apply(lambda x: x[0])
            .str.strip('.')
            .replace('https://www.hospitalesangeles.com/directorios/images/medicos/nofoto.gif', None)
            .replace('https://hospiten.com/DesktopModules/Hospiten/Images/default-professional-image.png', None)
            .replace({'': None}))

    def _clean_name(self, df):
        name_pattern = r'(^Dr. )|(\sa\s)|(Dra. )|(DRA. )|(Lic. )|(DR. )|(LIC. )|(Drag. )|(Y )'
        df['name'] = (
            df['name'].str.replace(name_pattern, '')
            .str.title().str.strip()
            .apply(lambda x: '' if x == None else x)
            .str.replace(',', '')
            .str.replace('Dr.', '')
            .str.replace('M.D.', '')
            .str.replace('&', '')
            .str.replace('Under ', '')
            .str.replace('Where ', '')
            .str.replace('  ', ' '))

        first_name = (
            df['name'].astype(str).str.split(' ')
            .apply(lambda x: ' '.join(x[:math.ceil(len(x)/2)])))

        last_name = (
            df['name'].astype(str).str.split(' ')
            .apply(lambda x: ' '.join(x[math.ceil(len(x)/2):])))

        return (first_name + ' ' + last_name).str.replace('  ', ' ')

    def _clean_doctorId(self, df):
        uuids = 'pipeline/resources/uuids.json'
        d = utils.open_dictionary(uuids)
        updated = False

        for name in df.name:
            if name not in d:
                d[name] = str(uuid.uuid4())
                updated = True

        df.doctorId = df.name.apply(lambda x: d[x])
        if updated:
            utils.save_dictionary(d, uuids)

        return df.doctorId

    def _additional_fix(self, df):

        hospitals = pd.read_csv('pipeline/resources/locations.csv')
        df = df.where(pd.notnull(df), None)
        ken = df[df.doctorId.apply(lambda x: x == None)]
        df = df[df.doctorId.apply(lambda x: x != None)]

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

        df['phoneNumber'] = (df.location
                             .astype(str)
                             .str.split(', ')
                             .apply(lambda x: [str(d[i]['phoneNumber']) for i in x if i in d])
                             .apply(lambda x: ', '.join(x))
                             )

        df['location'] = (df.location
                          .str.split(', ')
                          .apply(lambda x: [[d[i] if i in d else x for i in x][0]])
                          )

        ken['location'] = (ken.location
                           .apply(lambda x: [{
                               'locationName': 'External Office',
                               'location': x,
                               'city': None,
                               'state': None,
                               'country': 'Dominican Republic',
                               'zipCode': None,
                               'phoneNumber': None,
                               'latitude': None,
                               'longitude': None,
                           }])
                           )
        df = pd.concat([df, ken])

        def _clean_education(df):
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

        df['education'] = _clean_education(df)

        return df
