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


class HospitalAngeles:
    def __init__(self, metadata: dict):
        self.metadata = metadata

    def scrape(self):
        """ Strategy method scrape """
        doctor_urls = self._get_doctor_urls(
            'https://hospitalesangeles.com/indice_directorio.php?letra=')

        doctors = []
        for url in tqdm.tqdm(doctor_urls, 'Scraping doctors'):
            try:
                doctor = Doctor(self.metadata, 'static', url)
                doctors.append(doctor.extract_doctor_information())
            except Exception as e:
                print(e)
        df = pd.DataFrame(doctors)
        translator = Translator(df, self.metadata['hospital_short_name'])
        df = translator.translate()
        return df

    def _get_doctor_urls(self, base_url):
        """ Gets list of doctors by iterating through alphabet pages """
        alpha_pages = []
        for c in tqdm.tqdm(ascii_uppercase, 'Collecting doctor urls'):
            try:
                resp = requests.get(base_url + c)
                tree = lxml.html.fromstring(resp.content)
                doctors = [
                    i.replace(' ', '%20')[10:] for i in tree.xpath('//div[contains(@class, "nombre")]/a/@href')
                    if i != '#']
                alpha_pages += [
                    f'https://hospitalesangeles.com/paginaprofesional.php?{doctor.strip()}' for doctor in doctors]
            except Exception as e:
                print(e)
        return alpha_pages


class AmerimedHospital:
    def __init__(self, metadata):
        self.metadata = metadata
        self.doctor = Doctor(metadata, 'class_methods_not_in_use')
        self.doctor_information = self.doctor.doctor_information
        self.doctor_information['city'] = self.metadata['city']

    def scrape(self):
        url = 'https://www.amerimedcancun.com/directorio-medico.php?_pagi_pg=3&_pagi_pg='
        df = self._extract_doctor_pages(url, 4)
        translator = Translator(df, self.metadata['hospital_short_name'])
        df = translator.translate()
        return df

    def _extract_doctor_pages(self, url, num_pages):
        self.doctor_information['website'] = url
        for page in tqdm.tqdm(range(1, num_pages+1), 'Scraping doctors'):
            try:
                resp = requests.get(
                    url+str(page), headers={"User-Agent": "XY"})
                tree = lxml.html.fromstring(resp.content)
                for column, xpath in self.metadata['xpaths'].items():
                    if self.doctor_information[column] != None:
                        self.doctor_information[column] += tree.xpath(xpath)
                    else:
                        self.doctor_information[column] = tree.xpath(xpath)
            except Exception as e:
                print(e)
        return pd.DataFrame(self.doctor_information)


class CentroMedico:
    def __init__(self, metadata: dict):
        self.metadata = metadata
        self.driver = webdriver.Chrome()

    def scrape(self):
        # self.driver.get(
        #     'https://centromedicoabc.com/en/encuentra-a-tu-medico/')
        # time.sleep(3)
        # selenium_elements = self.driver.find_elements_by_xpath(
        #     '//*[@id="list-doctors"]/*')
        # doctors = []
        # for i in tqdm.tqdm(range(248)):
        #     for j in range(6):
        #         doctors.append(
        #             self._selenium_centro_medico_extract_doctor_information(selenium_elements[i*6+j]))
        #     self.driver.find_element_by_xpath(
        #         '//*[@id="page-navi"]/a[252]').click()  # goes to next page
        # df = pd.DataFrame(doctors)
        # translator = Translator(df, self.metadata['hospital_short_name'])
        # df = translator.translate()
        return pd.DataFrame()

    def _selenium_centro_medico_extract_doctor_information(self, selenium_element):
        selenium_element.find_element_by_tag_name('button').click()

        doctor = Doctor(self.metadata, 'selenium_centromedico')
        for column, xpath in self.metadata['xpaths'].items():
            if doctor.doctor_information[column] not in (None, "Location"):
                doctor.doctor_information[column] += self._add_element(
                    selenium_element, xpath)
            else:
                doctor.doctor_information[column] = self._add_element(
                    selenium_element, xpath)
        return doctor.doctor_information

    def _add_element(self, doctor, xpath, element_num=0):
        try:
            element_list = doctor.find_elements_by_xpath(xpath)
            return element_list[element_num].text
        except Exception as e:
            traceback.print_exc()
            return ''


class AngelesHealth:
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
            'https://www.angeleshealth.com/doctors-surgeons-angeles-hospital-tijuana/')

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
        url = 'https://www.angeleshealth.com/doctors-surgeons-angeles-hospital-tijuana/'
        ua = UserAgent()
        headers = {'User-Agent': str(ua.chrome)}
        resp = requests.get(url, headers=headers)
        tree = lxml.html.fromstring(resp.content)
        doctor_urls = [url for url in tree.xpath(
            '//a/img/parent::a/@href')[1:-1]]
        return doctor_urls


class MedicaSur:
    def __init__(self, metadata):
        self.metadata = metadata
        self.doctor = Doctor(metadata, 'class_methods_not_in_use')
        self.doctor_information = self.doctor.doctor_information
        self.doctor_information['city'] = self.metadata['city']

    def scrape(self):
        url = 'https://info.healthtravelmexico.com/medical-services/our-physicians.html'
        resp = requests.get(url)
        tree = lxml.html.fromstring(resp.content)
        for column, xpath in self.metadata['xpaths'].items():
            self.doctor_information[column] = tree.xpath(xpath)
            if column == 'additionalInformation' or column == 'otherActivities':
                self.doctor_information[column] = []
                for name in self.doctor_information['name']:
                    new_xpath = xpath.replace('NAME', name)
                    values = tree.xpath(new_xpath)
                    values = list(map(lambda x: " ".join(
                        x.split()).replace("\n", ""), values))
                    self.doctor_information[column].append(values)

        df = pd.DataFrame(self.doctor_information)
        df['otherActivities'] = df['otherActivities'].fillna(
            '').apply(lambda x: '|'.join(x))
        df['additionalInformation'] = df['additionalInformation'].fillna(
            '').apply(lambda x: '|'.join(x))
        return df
