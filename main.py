# Standard library imports
import json
import os
import sys

# Third party library imports
from google.cloud import bigquery

# Local library imports
from openrent.search import Search
import openrent.bq_loader as bql

if __name__ == "main":

    if len(sys.argv) != 1:
        print('usage: bin/scrape <config>')
        sys.exit(0)

    # Load existing data from BigQuery

    bq_project = os.environ['PROJECT_ID']
    bq_dataset_id = os.environ['DATASET_ID']
    bq_table_id = os.environ['TABLE_ID']

    bq_table_ref = f'{bq_project}.{bq_dataset_id}.{bq_table_id}'

    client = bigquery.Client()

    existing_data = bql.read_df_from_bq(bq_table_ref, client)

    search = Search('conf/search_config.yaml', 0, existing_data)

    bql.write_df_to_bq(search.results, 'schemas/openrent_listings.json', bq_table_ref, client)