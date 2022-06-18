import lxml.html
import pandas as pd
import requests
import tqdm
from fake_useragent import UserAgent
from pipeline.common.translator import Translator
from pipeline.models.doctor import Doctor


class ClinicaUnionMedicaDelNorte():
    def __init__(self, metadata):
        self.metadata = metadata
      
    def scrape(self):
        """ This method will first gather all of the individual doctor
        pages to a list of urls, then scrape each of those pages by
        creating a Doctor object and adding to a dataframe.

        Returns: DataFrame
        """
        url = 'https://clinicaunionmedica.com/medicos/'
        ua = UserAgent()
        headers = {'User-Agent': str(ua.chrome)}
        resp = requests.get(url, headers=headers)
        tree = lxml.html.fromstring(resp.content)
        specialty_urls = [url for url in tree.xpath(
            "//div[contains(@class, 'feature-btn')]/a/@href")]

        information = []
        for url in tqdm.tqdm(specialty_urls, 'Scraping doctors'):
            resp = requests.get(url, headers=headers)
            specialtyTree = lxml.html.fromstring(resp.content)
            doctorCells = specialtyTree.xpath("//div[contains(@class, 'em-team')]")
            for cell in doctorCells:
                doctor = Doctor(self.metadata, 'class_methods_not_in_use')
                doctor_information = doctor.doctor_information
                doctor_information['city'] = self.metadata['city']
                for column, xpath in self.metadata["xpaths"].items():
                    doctor_information[column] = "|".join(cell.xpath("." + xpath))
                information.append(doctor_information)
        df = pd.DataFrame(information)
        translator = Translator(df, self.metadata['hospital_short_name'])
        df = translator.translate()
        return df



