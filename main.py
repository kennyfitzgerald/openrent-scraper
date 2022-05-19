# Standard library imports
import json
import os
import sys

# Third party library imports
from google.cloud import bigquery
import pandas as pd

# Local library imports
from openrent.search import Search
import openrent.bq_loader as bql
from openrent.email import Emailer

if __name__ == "__main__":

    if len(sys.argv) != 1:
        print('usage: bin/scrape <config>')
        sys.exit(0)

    # Load existing data from BigQuery

    bq_project = 'kenny-personal-projects'
    bq_dataset_id = 'openrent'
    bq_table_id = 'openrent_listings'

    bq_table_ref = f'{bq_project}.{bq_dataset_id}.{bq_table_id}'

    client = bigquery.Client()

    try:
        existing_data = bql.read_df_from_bq(bq_table_ref, client)
    except:
        existing_data=None
    
    srch = Search('conf/search_config.yaml', 0, existing_data)

    results = srch.search()
    
    if results is not None:
        bql.write_df_to_bq(results, 'schemas/openrent_listings.json', bq_table_ref, client)

        email = Emailer('conf/email_config.yaml', results)

        print(email.filtered_results)
        
        if len(email.filtered_results) != 0:
            email.send_gmail()

    else:
        print('No new results found.')