import pytest
import os
import pandas as pd
from pipeline.common.utils import open_dictionary

HOSPITALS_PATH = 'pipeline/resources/locations.json'
HOSPITALS_METADATA_PATH = 'pipeline/resources/locations_metadata.json'
SCHEMA_PATH = 'pipeline/resources/schema.json'


def test_hospital_schema():
    """ Format: hospital schema is correct. """
    columns = set(open_dictionary(SCHEMA_PATH)['location_fields'])
    locations = open_dictionary(HOSPITALS_PATH)
    for k, v in locations.items():
        assert set(v.keys()) == columns


def test_location_fields():
    """ Test expected values of hospital fields """
    hospitals = open_dictionary(HOSPITALS_PATH)
    for k, v in hospitals.items():
        assert k == v['locationName']
        assert v['city'] not in (None, '')
        assert v['location'] not in (None, '')
        assert v['phoneNumber'].isdigit()
        assert v['state'] == ''
        assert v['zipCode'] == ''
        assert check_lat_and_lon(v['latitude'], v['longitude'])


def test_hospital_country():
    hospitals = open_dictionary(HOSPITALS_PATH)
    hospitals_metadata = open_dictionary(HOSPITALS_METADATA_PATH)
    country_isos = open_dictionary(SCHEMA_PATH)['country_isos']
    for country, hospital_list in hospitals_metadata.items():
        country = country_isos[country]
        for h in hospital_list:
            for name, data in hospitals.items():
                if name == h:
                    assert country == data['country']


def check_lat_and_lon(lat, lon):
    lat, lon = float(lat), float(lon)
    check_lat = -90 < lat < 90
    check_lon = -180 < lon < 180
    return check_lat and check_lon
