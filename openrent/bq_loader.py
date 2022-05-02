# Standard library imports

# Third party library imports

# Local library imports

import datetime

from google.cloud import bigquery
import pandas
import pytz

from openrent.configloader import ConfigLoader

class bq_loader:
    """ A class for reading and writing data from a specified BigQuery project
    """

    def __init__(config):
        self.config = ConfigLoader()
        self.project = config
        self.client = bigquery.Client()
        self.




client = bigquery.Client()

project = 'kenny-personal-projects'
dataset_id = 'openrent'
table_id = 'openrent_listings'
table_ref = f'{project}.{dataset_id}.{table_id}'

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE"
)

job = client.load_table_from_dataframe(
    x, table_id, job_config=job_config
)  # Make an API request.
job.result()  # Wait for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)

query_string = f"""
SELECT * FROM {table_id}
"""

dataframe = (
    client.query(query_string)
    .result()
    .to_dataframe()
)

print(dataframe.head())

dataset_ref = client.dataset(dataset_id, project=project)
table_ref = dataset_ref.table(table_id)
table = client.get_table(table_ref)