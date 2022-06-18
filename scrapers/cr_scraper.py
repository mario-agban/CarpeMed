import time
import traceback
from string import ascii_uppercase

import lxml.html
import pandas as pd
import requests
import tqdm
from fake_useragent import UserAgent
from numpy.core.defchararray import strip
from pipeline.common.translator import Translator
from pipeline.models.doctor import Doctor
from selenium import webdriver


class HospitalCima:
    def __init__(self, metadata):
        self.metadata = metadata
        self.doctor = Doctor(metadata, 'class_methods_not_in_use')
        self.doctor_information = self.doctor.doctor_information
        self.doctor_information['city'] = self.metadata['city']

    def scrape(self):
        """ This method will first gather all of the individual doctor
        pages to a list of urls, then scrape each of those pages by
        creating a Doctor object and adding to a dataframe.

        Returns: DataFrame
        """
        doctor_urls = self._get_doctor_urls(
            'https://directorio.hospitalcima.com/en/doctor')

        doctors = []
        for url in tqdm.tqdm(doctor_urls, 'Scraping doctors'):
            try:
                doctor = Doctor(self.metadata, 'static', url)
                doctors.append(doctor.extract_doctor_information())
            except Exception as e:
                print(e)
        df = pd.DataFrame(doctors)
        return df

    def _get_doctor_urls(self, base_url):
        """ Returns a list of doctors by sending a request and gathering all pages a/href() attribute xpath. """
        self.doctor_information['websites'] = base_url
        url = 'https://directorio.hospitalcima.com/en/doctor'
        ua = UserAgent()
        headers = {'User-Agent': str(ua.chrome)}
        resp = requests.get(url, headers=headers)
        tree = lxml.html.fromstring(resp.content)
        doctor_urls = ['https://directorio.hospitalcima.com' + url for url in tree.xpath(
            "//a[contains(@class, 'item')]/@href")]
        return doctor_urls


class ClinicaCatolica:
    def __init__(self, metadata):
        self.metadata = metadata
        self.doctor = Doctor(metadata, 'class_methods_not_in_use')
        self.doctor_information = self.doctor.doctor_information
        self.doctor_information['city'] = self.metadata['city']

    def scrape(self):
        """ This method will first gather all of the individual doctor
        pages to a list of urls, then scrape each of those pages by
        creating a Doctor object and adding to a dataframe.

        Returns: DataFrame
        """
        doctor_urls = self._get_doctor_urls(
            'https://directorio.hospitallacatolica.com/en/doctor')

        doctors = []
        for url in tqdm.tqdm(doctor_urls, 'Scraping doctors'):
            try:
                doctor = Doctor(self.metadata, 'static', url)
                doctors.append(doctor.extract_doctor_information())
            except Exception as e:
                print(e)
        df = pd.DataFrame(doctors)
        return df

    def _get_doctor_urls(self, base_url):
        """ Returns a list of doctors by sending a request and gathering all pages a/href() attribute xpath. """
        self.doctor_information['websites'] = base_url
        url = 'https://directorio.hospitallacatolica.com/en/doctor'
        ua = UserAgent()
        headers = {'User-Agent': str(ua.chrome)}
        resp = requests.get(url, headers=headers)
        tree = lxml.html.fromstring(resp.content)
        doctor_urls = ['https://directorio.hospitallacatolica.com' + url for url in tree.xpath(
            "//a[contains(@class, 'item')]/@href")]
        return doctor_urls


class ClinicaBiblica:
    def __init__(self, metadata):
        self.metadata = metadata
        self.doctor = Doctor(metadata, 'class_methods_not_in_use')
        self.doctor_information = self.doctor.doctor_information
        self.doctor_information['city'] = self.metadata['city']

    def scrape(self):
        """ This method will first gather all of the individual doctor
        pages to a list of urls, then scrape each of those pages by
        creating a Doctor object and adding to a dataframe.

        Returns: DataFrame
        """
        doctor_urls = self._get_doctor_urls(
            'https://www.clinicabiblica.com/en/services/medical-specialties')

        doctors = []
        for url in tqdm.tqdm(doctor_urls, 'Scraping doctors'):
            try:
                doctor = Doctor(self.metadata, 'static', url)
                doctors.append(doctor.extract_doctor_information())
            except Exception as e:
                print(e)
        df = pd.DataFrame(doctors)
        return df

    def _get_doctor_urls(self, base_url):
        """ Returns a list of doctors by iterating through a specialty page. """
        self.doctor_information['websites'] = base_url

        url = 'https://www.clinicabiblica.com/en/services/medical-specialties'
        ua = UserAgent()
        headers = {'User-Agent': str(ua.chrome)}
        resp = requests.get(url, headers=headers)
        tree = lxml.html.fromstring(resp.content)
        specialty_urls = ['https://www.clinicabiblica.com' + url for url in tree.xpath(
            "//div[contains(@class, 'itemHeader')]/h3/a/@href")]

        doctor_urls = []
        for url in tqdm.tqdm(specialty_urls):
            resp = requests.get(url)
            tree = lxml.html.fromstring(resp.content)
            doctors_in_specialty = [f'https://www.clinicabiblica.com{d}' for d in tree.xpath(
                "//a[contains(text(), 'More information')]/@href")]
            doctor_urls += doctors_in_specialty
        return doctor_urls


class HospitalMetropolitano:
    def __init__(self, metadata):
        self.metadata = metadata
