import lxml.html
import requests
from fake_useragent import UserAgent


class Doctor:
    def __init__(self, metadata, strategy, *argv):
        self.metadata = metadata
        self.strategy = strategy
        self.argv = ''.join(argv)

        self.doctor_information = {
            'doctorId': None,  # TODO: generate random uuid4
            'name': None,
            'provider': None,
            'phoneNumber': None,
            'location': metadata['hospital_name'],
            'description': None,
            'spokenLanguages': None,
            'website': None,
            'email': None,
            'hoursOperation': None,
            'hoursOperationObj': None,
            'confirmedHours': False,
            'ratings': None,
            'rating': None,
            'education': None,
            'additionalInformation': None,
            'teleHealth': None,
            'city': None,
            'country': metadata['country'],
            'photoUrl': None,
            'otherActivities': None,
            'alternativeMedicine': None
        }

    def extract_doctor_information(self):
        if self.strategy == 'static':
            return self._static_page_extract_doctor_information(self.metadata, self.argv)
        else:
            return self.doctor_information

    def _static_page_extract_doctor_information(self, metadata, doctor_url):
        """ Calls get_xpath for each field and creates a DataFrame row for doctor/provider scraped. """

        self._set_doctor_information('website', doctor_url)
        self._set_doctor_information('city', self.metadata['city'])
        ua = UserAgent()
        headers = {'User-Agent': str(ua.chrome)}
        resp = requests.get(self.argv, headers=headers)
        tree = lxml.html.fromstring(resp.content)
  
        for column, xpath in metadata['xpaths'].items():
            self.doctor_information[column] = self._get_xpath(tree, xpath)

        return self.doctor_information

    def _set_doctor_information(self, field, value):
        try:
            self.doctor_information[field] = value
        except Exception as e:
            print(e)

    def _get_xpath(self, tree, path):
        """ Tries to find field information provided xpath. Returns items delimited by '|'. """
        try:
            return '|'.join(tree.xpath(path))
        except:
            return None
