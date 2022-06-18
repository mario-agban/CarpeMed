from os import environ
import re
import tqdm
import requests
import json
import uuid
import pandas as pd
from pipeline.common.utils import open_dictionary, save_dictionary, save_export
from pipeline.common import utils

class Locations:
    def __init__(self, path):
        self.path = path

    def aggregate_locations(self):
        """ This function will read the locations from locations metadata (path) and save the data to locations.json """
        try:
            location_metadata = open_dictionary(self.path)
        except Exception as e:
            print(e)
            return
        hospitals_dict = {}

        for country in location_metadata:
            country_dict = {}
            for provider_category in location_metadata[country]:
                for hospital_name in tqdm.tqdm(location_metadata[country][provider_category], desc=f'{country} {provider_category}'):
                    hospital_data = self.get_gmaps_data(
                        hospital_name, provider_category, country)
                    hospitals_dict[hospital_name] = hospital_data
                    country_dict[hospital_name] = hospital_data
            df = pd.json_normalize(list(country_dict.values()))
            save_export(f'{country}_hospitals_raw', df)

        # Saves as one big file, unsure if this is still needed 
        # self.save(hospitals_dict)
        # self.export_csv(hospitals_dict)

    def save(self, dictionary):
        try:
            save_dictionary(dictionary, 'pipeline/resources/locations.json')
        except Exception as e:
            print(e)

    def export_csv(self, d):
        df = pd.json_normalize(list(d.values()))
        df.to_csv('pipeline/resources/locations.csv')

    def _get_locationID(self, locationName):
        uuids = 'pipeline/resources/uuids.json'
        d = utils.open_dictionary(uuids)
        updated = False

        if locationName not in d:
            d[locationName] = str(uuid.uuid4())
            updated = True

        if updated:
            utils.save_dictionary(d, uuids)

        return d[locationName]

    def get_gmaps_data(self, location_query, provider_category, country) -> dict:
        """ This function will make the Google Geocoder API call to collect information and returns a dictionary. """

        key = environ.get('GOOGLE_API_KEY')
        country_iso_remapper = open_dictionary(
            'pipeline/resources/schema.json')['country_isos']
        location_query_with_country = f'{location_query} {country_iso_remapper[country]}'
        search_address = '+'.join(location_query_with_country.split(' '))
        resp = requests.get(
            f'https://maps.googleapis.com/maps/api/geocode/json?address={search_address}&key={key}')
        resp = resp.json()

        hospital = {
            'locationID': None,
            'locationName': location_query,
            'location': None,
            'city': None,
            'state': '',
            'country': None,
            'zipCode': '',
            'phoneNumber': None,
            'website': None,
            'latitude': None,
            'longitude': None,
            'hoursOperation': None,
            'providerCategory': provider_category,
        }

        if resp['status'] == 'ZERO_RESULTS':
            return hospital

        hospitalData = resp['results'][0]

        place_id = hospitalData['place_id']
        place_details = requests.get(
            f'https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&key={key}')
        place_details = place_details.json()

        hospital['locationID'] = self._get_locationID(hospital['locationName'])
        self.set_value(
            'phoneNumber', place_details["result"], "international_phone_number", hospital, location_query)
        self.set_value(
            'website', place_details["result"], "website", hospital, location_query)
        self.set_value(
            'longitude', hospitalData['geometry']['location'], 'lng', hospital, location_query)
        self.set_value(
            'latitude', hospitalData['geometry']['location'], 'lat', hospital, location_query)
        self.set_value('location', hospitalData,
                       'formatted_address', hospital, location_query)

        hours = requests.get(
            f'https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&key={key}&fields=opening_hours')
        hours = hours.json()
        self.set_value(
            'hoursOperation', hours["result"], "opening_hours", hospital, location_query)

        for component in hospitalData['address_components']:
            for k, v in component.items():
                if 'country' in v and hospital['country'] is None:
                    hospital['country'] = component['long_name']
                if 'locality' in v or 'sublocality' in v and hospital['city'] is None:
                    hospital['city'] = component['long_name']
        return hospital

    def set_value(self, k, v, i, hospital, location_query):
        try:
            if k == 'phoneNumber':
                hospital[k] = re.sub(r'\+|\s', '', v[i])
            elif k == 'location':
                hospital[k] = ','.join(v[i].split(',')[:])
            elif k == 'hoursOperation':
                hospital[k] = v[i]['weekday_text']
            else:
                hospital[k] = v[i]
        except Exception as e:
            print(e)
