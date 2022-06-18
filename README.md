# Data-Pipeline-v2

## Setup

To set up the first time, make sure you have Anaconda installed and set up an environment:

`conda create -n carpemed python=3.9`

`conda activate carpemed`

`git clone https://github.com/CarpeMedTech/data-pipeline-v2`

`cd Data-Pipeline-v2 && pip install -r requirements.txt`

# Archived Pipeline v1 Documentation

For more information about the previous technical specifications or approach, read v1 pipeline README.

- [Previous Data Pipeline GitHub Link](https://github.com/CarpeMedTech/Data-Pipeline)
# Important Documents (Access via CarpeMed Drive)

These Google Docs contain important information regarding the data acquisition approach and schema. Technical information regarding the pipeline is in `README.md`

- [CM Country Provider Supply Approach](https://docs.google.com/document/d/15mUoUPcRUc-38FAN2YO-K7TSnmmf03DH1QZR_s0WGuM/edit)
- [2021 v2 Changes](https://docs.google.com/document/d/1_r_qGu19nyswETrvbeBAL57o73mp8LoKTgMiynfdXeg/edit)
- [Schema v2 - Design](https://docs.google.com/document/d/1eHyl1-Z0DLHyST4V5Dhf-Lcn9kJ1MTGuSZNwBIGkftI/edit#)
- [DS Onboarding](https://docs.google.com/presentation/d/1cunxR31GYUs2dc8aVXGTJrAaI4YVCEoU6LI-BmWgFOk/edit#slide=id.p)
# Starter Code Instructions

## __Hospital Website Scraper__

- Checkout to `mx-scraper-starter` branch to get started.

### Input files:
- `scrapers/mx_scraper.py` this is where classes for each website will be added.
- `__main__.py` this shouldn't need any changes, but you can see the flow of how the scrapers get called starting on line 20
- `paths.py` this is where the hospital will be added in the `scrapers` portion of the dictionary, so that the main function knows to call the class from `mx_scraper.py`
- `hospitals_metadata/country.json`, this is where the metadata for each website is defined, including the xpaths that are used for gathering information
- `models/doctor.py` - this is what is constructed for each instance of a doctor, and is used to represent the information that is gathered from each xpath, and is later converted to data that can be put in a dataframe

### Outputs:
- `data/mx/mx_raw_hospital.json` this file gets saved from `__main__.py` after being scraped using the necessary classes and functions.

### Angeles Health
#### https://www.angeleshealth.com/doctors-surgeons-angeles-hospital-tijuana/
- Start by identifying the way a website has its information, and figure out if there is one doctor per page, or many
- For Angeles Health, there is one doctor per page, so the first step is to get the link of each doctor's individual page
- Start by filling out the metadata for the hospital, including any defaults
- Take a look at the skeleton code for the class in `mx_scrapers.py` and those two functions should be the ones necessary to gather the information.
- Creating a doctor instance for each doctor will help in the `scrape()` function and

### Medica Sur
#### https://info.healthtravelmexico.com/medical-services/our-physicians.html
- This one will be a little more tricky since all of the doctors will be on the same page.
- As a starting point, take a look at `AmerimedHospital` to see the structure of that site and how it is done, and try something similar.

### General Tips:
- Using a jupyter notebook on the side to see the requests and responses, and manipulate the data to get the right xpaths can be easier, and then putting it into the metadata.json file after.

## __Costa Rica Scrapers__ _(Added: 2021-0-15)_

Related files:
- `scrapers/cr_scraper.py`
- `resources/hospitals_metadata/costarica.json`
- `resources/locations_metadata.json`
- `data/cr`
- `data/index.json`

### Hospital CIMA 
#### https://directorio.hospitalcima.com/en/doctor 
- This one seems pretty easy - it follows the structure of a doctor per page
- Workflow similar to Angeles Health
- First get individual doctor page URLS then run scraper

### Clinica Catolica 
#### https://directorio.hospitallacatolica.com/en/doctor 
- Same as CIMA

### Clinica Biblica 
#### https://www.clinicabiblica.com/en/services/medical-specialties 
- This one is more difficult - follows a doctor per page workflow
- Start by iterating through the medical specialty pages
- i.e. https://www.clinicabiblica.com/en/services/medical-specialties/923-allergology then https://www.clinicabiblica.com/en/medical-specialists/162-allergology/794-mario-alberto-martinez-alfonso
- Repeat this for all provdier medical provider types

### Hospital Metropolitano 
#### https://directorio.metropolitanocr.com/
- Uses AJAX/XHR, I will do this one
- Let me know if you would be interested in working on this one, though, since it involves working with websites' API

## __Locations Scraper__

- Checkout to `locations-scraper` branch to get started.
- Run `python -m pipeline -l` to use location scraper.
- Make sure to cache results in `locations.json` so queries are not re-run.
- Make sure you have `.env` in top level directory for Google API key.
- Open `locations.json` with `open_dictionary`, check to see if location exists, if not then API call, then `save_dictionary`, else move onto next value.
- Make sure to handle cases where API json response returns None

### Inputs:
- `locations_metadata.json`, dictionary of countries (key) and corresponding list of hospitals/locations/google maps query term (values).
- `locations.py`, which is called from main.

### Outputs:
- `locations.json` Dictionary where key is country, list of hospital names/google maps query terms
- Each location should match:
```
hospital = {
    'hospitalName': hospitalName,
    'streetAddress': streetAddress,
    'city': city,
    'state': state,
    'country': country,
    'zipCode': zipCode,
    'phoneNumber': phoneNumber,
    'website': website,
    'latitude': latitude,
    'longitude': longitude
}
```

### Example
Input: `locations_metadata.json`
```
{"mx": ["Hospiten Cancun"]}
```

run `python -m pipeline -l`

Output: `locations.json`
```
{
    "Hospiten Cancun": {
        "hospitalName": "Hospiten Cancun,
        "streetAddress": "Av. Bonampak 10, Zona Hotelera",
        "city": "Cancun",
        "state": "Quintana Roo",
        "country": "Mexico",
        "zipCode": "77500",
        "phoneNumber": "529988813700",
        "website": "https://hospiten.com/en/hospitals-and-centers/hospiten-cancun",
        "latitude": "21.13837",
        "longitude": "-86.82384"
    }
}
```

### Help
- API Documentation: https://developers.google.com/maps/documentation/geocoding/start

## __Cleaning__

- Checkout to `mx-cleaner` to get started.

### Inputs:
- Scraped JSON data from `data/country/raw` is combined with `CleanerUtils` function.
- This input dataframe will have cleaning functions applied to it.
- `mx_cleaner.py` has a `MexicoCleaner` class, where the `clean` function is called by `main`
- To clean columns, either call generic function from the `CleanerUtils` class (e.g. `self.cleaner.combine_raw_files(self.country)`), or call a function from within the specific country's class (e.g. `self._clean_name(df)`)
- So when writing cleaning functions, if it is general, add it to the `CleanerUtils` class. If it is country specific, add it to the specific country cleaning class.

### Outputs:
- Clean dataset, in both JSON in CSV format, in the country's data folder.

### Resources and Tips:
- Use a juypyter notebook with the raw dataframe to test out cleaning functions.
- Make sure to __print out and examine all the rows to make sure each entry__ has clean data.
- Make good use of Regex, pandas apply functions, and built in pandas functions.
- See https://github.com/CarpeMedTech/Data-Pipeline/blob/dev/pipeline/cleaners/gr_cleaner.py for an example of a cleaning class file.
- Try to make functions generic and in the `CleanerUtils` class, and only in the country class if the data has to be cleaned in a specific way.
- Message me on Slack if you have any questions about how to clean something, or if you need help approaching something.
- Run `python -m pipeline -t` to make sure clean dataset passes tests (edit 3/31/21: currently no tests for clean data).