# Standard library imports
from base64 import encode
import random
import time
import re
from datetime import datetime, timezone
from os.path import exists
from re import S, sub
from decimal import Decimal

# Third party library imports
import requests
from bs4 import BeautifulSoup
from lxml import html
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import urllib
from dateutil import relativedelta as rd
from dateutil import parser
import pandas as pd
import numpy as np

# Local library imports
from openrent.configloader import ConfigLoader

URL_BASE = 'https://www.openrent.co.uk/'
URL_ENDPOINT = 'https://www.openrent.co.uk/properties-to-rent/'
ADVERTS_URLS_SELECTOR = 'a.pli.clearfix'
MAPS_XPATH_SELECTOR = '/html/body/div[4]/div[2]/section/div[2]/div/div/div/div/div[1]/div[5]/div/div[1]/img[1]'

class Search():
    """ This class scrapes openrent for adverts & saves data into 
        a csv file to avoid repeating notifications. 

        The search parameters for an individual search can be found in 
        the conf/search_config.yaml file.
    """

    def __init__(self, config_file, search_num, existing_data=None):
        """ Initialise the search query. Also loads historical data for avoidance
            of repeated listings

            Args:
                config_file: yaml file containing at least 1 search config params
                search_num: The number of the search as ordered within the config file
        """
        self.config = ConfigLoader(config_file, search_num)
        self.existing = existing_data
        self.params = self.config.config
        self.driver = self._get_driver()
        self.url = self._encode_url(self.params)

    def _get_driver(self):

        options = Options()
        options.add_argument('--headless')
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)

        return driver

    def _encode_url(self, params):

        term = params['term']
        term = re.sub('[^A-Za-z0-9 ]', '', term)
        term = re.sub('\s+', '-', term).lower()

        query_string = urllib.parse.urlencode(params)

        url = f'{URL_ENDPOINT}{term}?{query_string}'

        return url

    def _make_request(self, url, params=None):
        
        session = requests.session()

        response = session.get(url, params=params, headers='')

        # Raise any HTTP status errors
        response.raise_for_status()

        # Be gentle with the requests!
        time.sleep(random.randint(0, 3))

        return response

    def _pull_results(self, url):
        
        self.driver.get(url)

        pre_scroll_height = self.driver.execute_script('return document.body.scrollHeight;')

        run_time, max_run_time = 0, 1
        while True:
            iteration_start = time.time()
            # Scroll webpage, the 100 allows for a more 'aggressive' scroll
            self.driver.execute_script('window.scrollTo(0, 100*document.body.scrollHeight);')

            post_scroll_height = self.driver.execute_script('return document.body.scrollHeight;')

            scrolled = post_scroll_height != pre_scroll_height
            timed_out = run_time >= max_run_time

            if scrolled:
                run_time = 0
                pre_scroll_height = post_scroll_height
            elif (not scrolled) & (not timed_out):
                run_time += time.time() - iteration_start
            elif not scrolled & timed_out:
                break

        html = self.driver.page_source

        page = BeautifulSoup(html, features="lxml")

        self.driver.refresh()
        time.sleep(1)

        return page

    def _parse_results(self, url):
        
        page = self._pull_results(self.url)

        listings = page.select(ADVERTS_URLS_SELECTOR)

        updates = [(x.text.split(' ')[5] in ['hour', 'minutes']) for x in page.select('div.timeStamp')]
        
        listings_updates = zip(listings, updates)

        results_parsed = []

        for l, u in listings_updates:

            result = {
                "id":self._get_listing_id(l),
                "created_at":datetime.now(timezone.utc),
                "let_agreed":self._get_let_agreed(l),
                "let_agreed_at":datetime.now(timezone.utc) if self._get_let_agreed(l) else None,
                "recently_updated":u
            }

            results_parsed.append(result)
        
        results_parsed = pd.DataFrame(results_parsed)

        if self.existing is None:
            results_parsed['historical'] = ~results_parsed['recently_updated']

        results_parsed = results_parsed.reset_index(drop=True).convert_dtypes()
        
        return results_parsed
    
    def _get_listing_id(self, listing_html):
            
        for div in listing_html.find_all("div"):
            if div.get('data-listing-id'):
                listing_id = int(div.get('data-listing-id'))
                break

        return listing_id


    def _get_let_agreed(self, listing_html):
                
        let_agreed = len(listing_html.find_all("span", {"class":"let-agreed"})) == 1

        return let_agreed


    def _get_listing_details(self, listing_id):

        def _extract_bool_from_html(html_list, ind):

            html = html_list[ind].find("i")['class']
            bool = ' '.join(html)!="fa fa-times"

            return bool
        
        url = f'{URL_BASE}{listing_id}'

        self.driver.get(url)
        self.driver.implicitly_wait(2)
        
        WebDriverWait(self.driver,20).until(EC.element_to_be_clickable((By.XPATH, MAPS_XPATH_SELECTOR))).click()

        time.sleep(1.5)
        result = None
        response = self.driver.page_source

        while not result:
            result = re.search('LatLng\((.*)\);', response)

        latlong = result.group(1).split(",")
        lat = float(latlong[0])
        lng = float(latlong[1])

        soup = BeautifulSoup(response, features="lxml")

        title = soup.find_all("h1", {"class":"property-title"})[0].string

        overview = soup.find_all("table", {"class":"table table-striped intro-stats"})[0]
        bedrooms, bathrooms, max_tenants, location  = [x.string for x in overview.find_all("strong")]
        bedrooms = int(bedrooms)
        bathrooms = int(bathrooms)
        max_tenants = int(max_tenants)
        description = soup.find_all("div", {"class":"description"})[0].text

        features = soup.find_all("table", {"class":"table table-striped"})
        price_bills = features[0].find_all("td")
        deposit = float(sub(r'[^\d.]', '', price_bills[1].text))
        rent_total = float(sub(r'[^\d.]', '', price_bills[3].text))
        bills_included = _extract_bool_from_html(price_bills, 5)

        tenant_preference = features[1].find_all("td")
        student_friendly = _extract_bool_from_html(tenant_preference, 1)
        families_allowed = _extract_bool_from_html(tenant_preference, 3)
        pets_allowed = _extract_bool_from_html(tenant_preference, 5)
        smokers_allowed = _extract_bool_from_html(tenant_preference, 7)
        dss_lha_covers_rent = _extract_bool_from_html(tenant_preference, 9)

        availability = features[2].find_all("td")
        available_from = availability[1].text
        available_from_ts = datetime.now() if available_from=="Today" else parser.parse(available_from)
        available_from_ts = str(available_from_ts.date())
        minimum_tenancy = availability[3].text

        additional_features = features[3].find_all("td")
        has_garden = _extract_bool_from_html(additional_features, 1)
        has_parking = _extract_bool_from_html(additional_features, 3)
        has_fireplace = _extract_bool_from_html(additional_features, 5)
        furnished = additional_features[7].text
        epc_rating = additional_features[9].text

        try:
            transport = [x.text.replace('\r', '').replace('\n', '').strip() for x in soup.find_all("table", {"class":"table table-striped mt-1"})[0].find_all('td', text=True)]
        except:
            transport = list()

        try:
            closest_station = transport[2]
            closest_station_mins = int(transport[3].split(' ')[0])
        except:
            closest_station = ''
            closest_station_mins = None
        
        try:
            second_closest_station = transport[4]
            second_closest_station_mins = int(transport[5].split(' ')[0])
        except:
            second_closest_station = ''
            second_closest_station_mins = None

        room_only = title.split(',')[0]=='Room in a Shared House'
        rent_per_person = round(rent_total/int(bedrooms), 2) if not room_only else rent_total
        
        listing_details = {
                "title":title,
                "room_only":room_only,
                "rent_per_person":rent_per_person,
                "location":location,
                "lat":lat,
                "lng":lng,
                "bedrooms":bedrooms,
                "bathrooms":bathrooms,
                "max_tenants":max_tenants,
                "description":description,
                "deposit":deposit,
                "rent_total":rent_total,
                "bills_included":bills_included,
                "student_friendly":student_friendly,
                "families_allowed":families_allowed,
                "pets_allowed":pets_allowed,
                "smokers_allowed":smokers_allowed,
                "dss_1ha_covers_rent":dss_lha_covers_rent,
                "available_from":available_from,
                "available_from_ts":available_from_ts,
                "minimum_tenancy":minimum_tenancy,
                "has_garden":has_garden,
                "has_parking":has_parking,
                "has_fireplace":has_fireplace,
                "furnished":furnished,
                "epc_rating":epc_rating,
                "closest_station":closest_station,
                "closest_station_mins":closest_station_mins,
                "second_closest_station":second_closest_station,
                "second_closest_station_mins":second_closest_station_mins
            }
        
        return listing_details 


    def _update_records(self, new_results):
        
        if self.existing is None:
            first_run = True
            self.existing = pd.DataFrame(
                columns=list(new_results.columns)
            ).astype(new_results.dtypes.to_dict()
            ).drop('historical', axis=1)
        else:
            first_run = False
            historical_ids = self.existing['id'][self.existing['historical']]
            new_results = new_results[~new_results['id'].isin(historical_ids)]

        updated = pd.merge(self.existing, new_results, on='id', how='outer', suffixes=["_existing", "_new"])

        # Mark new listings 
        updated['new_listing'] = updated['created_at_existing'].isna()

        # Mark change in let agreed
        if not first_run:
            updated['let_agreed_since_last_run'][~updated['new_listing']] = np.where((~updated['let_agreed_new'].isna()) & (updated['let_agreed_existing'].isna()), True, False)
            updated['let_agreed_since_last_run'].fillna(value=False, inplace=True)
            updated['historical'].fillna(value=False, inplace=True)
        else:
            updated['let_agreed_since_last_run'] = False

        # Update new records
        updated['created_at'] = np.where(updated['new_listing'], updated['created_at_new'], updated['created_at_existing'])

        updated['let_agreed'] = np.where(updated['new_listing'], updated['let_agreed_new'], updated['let_agreed_existing'])

        updated['let_agreed_at'] = np.where(updated['new_listing'], updated['let_agreed_at_new'], updated['let_agreed_at_existing'])

        # Updated let agreed at values 
        updated['let_agreed_at'] = np.where((~updated['new_listing']) & (updated['let_agreed_since_last_run']), updated['let_agreed_at_new'], updated['let_agreed_at'])
        
        # Convert Dtypes
        updated = updated.convert_dtypes()

        # timestamps need setting to correct tz
        updated[[
            'created_at_existing',
            'let_agreed_at_existing',
            'created_at_new',
            'let_agreed_at_new',
            'let_agreed_at',
            'created_at',
        ]] = updated[[
            'created_at_existing',
            'let_agreed_at_existing',
            'created_at_new',
            'let_agreed_at_new',
            'let_agreed_at',
            'created_at',
        ]].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S.%f', utc=True)

        return updated
    
    def _update_new_listing_details(self, df):

        ids_to_update = list(df['id'][(df['new_listing']==True) & (df['historical']==False)])
        df = df.set_index('id')

        for id in ids_to_update:
            d = self._get_listing_details(id)
            for key in d.keys():
                if not key in df.columns:
                    df[key] = None
                df.loc[id, key] = d.get(key)
                
        return df

    def search(self):

        parsed_results = self._parse_results(self.url)

        updated_results = self._update_records(parsed_results)

        if len(updated_results[(updated_results['new_listing']==True) & (updated_results['historical']==False)])==0:
            return None

        final_results = self._update_new_listing_details(updated_results)

        # remove_cols
        final_results = final_results.loc[:,~final_results.columns.str.endswith('_existing')]
        final_results = final_results.loc[:,~final_results.columns.str.endswith('_new')]

        # Convert available_from_ts to datetime
        final_results['available_from_ts'] = final_results['available_from_ts'].apply(pd.to_datetime, format='%Y-%m-%d', utc=True)

        # Sort results
        final_results = final_results.sort_values(by=['created_at'], ascending=False)

        return final_results