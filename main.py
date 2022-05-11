# Standard library imports
import json
import os
import sys

# Third party library imports
from google.cloud import bigquery

# Local library imports
from openrent.search import Search
import openrent.bq_loader as bql

if __name__ == "__main__":

    if len(sys.argv) != 1:
        print('usage: bin/scrape <config>')
        sys.exit(0)

    # Load existing data from BigQuery

    # bq_project = os.environ['PROJECT_ID']
    # bq_dataset_id = os.environ['DATASET_ID']
    # bq_table_id = os.environ['TABLE_ID']

    bq_project = 'kenny-personal-projects'
    bq_dataset_id = 'openrent'
    bq_table_id = 'openrent_listings'

    # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "C:/kenny-personal-projects-bq-sa.json"

    bq_table_ref = f'{bq_project}.{bq_dataset_id}.{bq_table_id}'

    client = bigquery.Client()

    try:
        existing_data = bql.read_df_from_bq(bq_table_ref, client)
    except:
        existing_data=None
    
    Search = Search('conf/search_config.yaml', 0, existing_data)

    results = Search.search()

    print(results.head())

    bql.write_df_to_bq(results, 'schemas/openrent_listings.json', bq_table_ref, client)