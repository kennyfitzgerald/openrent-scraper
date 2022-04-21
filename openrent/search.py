# Standard library imports
import random
import time
import re
from datetime import datetime, timezone
from os.path import exists

# Third party library imports
import requests
from bs4 import BeautifulSoup
from lxml import html
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import urllib
from dateutil import relativedelta as rd
import pandas as pd
import numpy as np


# Local library imports
from openrent.configloader import ConfigLoader

config = ConfigLoader('conf/search_config.yaml', 0)

params = config.config

URL_BASE = 'https://www.openrent.co.uk/'
URL_ENDPOINT = 'https://www.openrent.co.uk/properties-to-rent/'
ADVERTS_URLS_SELECTOR = 'a.pli.clearfix'
LISTING_ID_SELECTOR = 'div.property-row-carousel.swiper'

class Search():
    """ This class scrapes openrent for adverts and saves data into 
        a csv file to avoid repeating notifications. 

        The search parameters for an individual search can be found in 
        the conf/search_config.yaml file.
    """

    def __init__(self, params):
        """ Initialise the search query. Also loads historical data for avoidance
            of repeated listings

            Args:
                params: Dictionary containg query string parameters for 
                making HTTP request
                max_age: Maximum age of listing to pull
        """
        self.params = params
        self.driver = self._get_driver()
        self.url = self._encode_url(self.params)
        self.page = self._pull_results()
        self.new_results = self._parse_results()

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

    def _pull_results(self):
        
        self.driver.get(self.url)

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
            elif not scrolled and not timed_out:
                run_time += time.time() - iteration_start
            elif not scrolled and timed_out:
                break

        html = self.driver.page_source

        page = BeautifulSoup(html)

        return page

    def _parse_results(self):

        listings = self.page.select(ADVERTS_URLS_SELECTOR)

        results_parsed = []

        for l in listings:

            result = {
                "id":self._get_listing_id(l),
                "created_at":datetime.now(timezone.utc),
                "updated_at":self._get_last_updated(l)[2],
                "updated_at_total":self._get_last_updated(l)[0],
                "updated_at_unit":self._get_last_updated(l)[1],
                "updated_at_seconds":self._get_last_updated(l)[3],
                "let_agreed":self._get_let_agreed(l)
            }

            results_parsed.append(result)
        
        results_parsed = pd.DataFrame(results_parsed)
        results_parsed = results_parsed.sort_values(by=['updated_at'], ascending=False).reset_index(drop=True).convert_dtypes()
        
        return results_parsed

    def _update_dataset(self):
        
        if exists('data/results.csv'):
            # Read existing data
            existing = pd.read_csv('data/results.csv').convert_dtypes()
        else:
            existing = pd.DataFrame(
                columns=list(new_results.columns)
            ).astype(new_results.dtypes.to_dict())

        updated = pd.merge(existing, new_results, on='id', how='outer', suffixes=["_existing", "_new"]).convert_dtypes()

        # timestamps need setting to correct tz
        updated[[
            'created_at_existing',
            'updated_at_existing',
            'created_at_new',
            'updated_at_new'
        ]] = updated[[
            'created_at_existing',
            'updated_at_existing',
            'created_at_new',
            'updated_at_new'
        ]].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S.%f')

        # Mark new listings 
        updated['new_listing'] = updated['created_at_existing'].isna()

        # Mark change in let agreed
        updated['let_agreed_ts'] = updated['let_agreed_new']==updated['let_agreed_existing']

        # Set unified created_at column (AKA first seen at)
        updated['created_at'] = np.where(updated['created_at_existing'].notna(), updated['created_at_existing'], updated['created_at_new'])

        # Check if listing has been updated

        updated.updated_at_seconds_existing.fillna(updated.updated_at_seconds_new, inplace=True)
        updated.updated_at_seconds_new.fillna(updated.updated_at_seconds_existing, inplace=True)

        updated.updated_at_total_existing.fillna(updated.updated_at_total_new, inplace=True)
        updated.updated_at_total_new.fillna(updated.updated_at_total_existing, inplace=True)
        
        updated.updated_at_unit_existing.fillna(updated.updated_at_unit_new, inplace=True)
        updated.updated_at_unit_new.fillna(updated.updated_at_unit_existing, inplace=True)

        updated['updated_at'] = np.where(
            updated['updated_at_seconds_new'] > updated['updated_at_seconds_existing'], 
            updated['updated_at_new'], 
            updated['updated_at_existing']
        )
        updated.updated_at.fillna(updated.updated_at_existing, inplace=True)

        
        updated['updated_at_total'] = np.where(
            updated['updated_at_seconds_new'] > updated['updated_at_seconds_existing'], 
            updated['updated_at_total_new'], 
            updated['updated_at_total_existing']
        )
        updated.updated_at_total.fillna(updated.updated_at_total_existing, inplace=True)

        updated['updated_at_unit'] = np.where(
            updated['updated_at_seconds_new'] > updated['updated_at_seconds_existing'], 
            updated['updated_at_unit_new'], 
            updated['updated_at_unit_existing']
        )
        updated.updated_at_unit.fillna(updated.updated_at_unit_existing, inplace=True)



        pass


    def _get_listing_id(self, listing_html):
                
        for div in listing_html.find_all("div"):
            if div.get('data-listing-id'):
                listing_id = int(div.get('data-listing-id'))
                break

        return listing_id


    def _get_last_updated(self, listing_html):

            last_updated = re.sub(" Last Updated around | ago", "", listing_html.find("div", attrs={"class":"timeStamp"}).text)

            if last_updated[len(last_updated)-1] != 's':
                last_updated = f"{last_updated}s"
            
            if last_updated[0]=='a':
                last_updated = last_updated.replace('a', '1', 1)

            parsed_s = [last_updated.split()[:2]]
            time_dict = dict((fmt,float(amount)) for amount,fmt in parsed_s)
            last_updated_ts = datetime.now() - rd.relativedelta(**time_dict)
            seconds = last_updated_ts.timestamp()

            last_updated_amount, last_updated_unit = last_updated.split(' ')
            
            return int(last_updated_amount), last_updated_unit, last_updated_ts, int(seconds)


    def _get_let_agreed(self, listing_html):
                
        let_agreed = len(listing_html.find_all("span", {"class":"let-agreed"})) == 1

        return let_agreed


search = Search(params)

new_results = search.new_results

# new_results.to_csv('data/results.csv', index=False)