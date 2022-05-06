# Standard library imports
import json 

# Third party library imports
from google.cloud import bigquery

# Local library imports


def json_schema_to_list(json_file):

    bigquerySchema = []

    with open(json_file) as f:
        bigqueryColumns = json.load(f)
        for col in bigqueryColumns:
            bigquerySchema.append(bigquery.SchemaField(col['name'], col['type'], col['mode']))
    
    return bigquerySchema

def write_df_to_bq(df, schema_json_file, bq_table_ref, client):

    schema = json_schema_to_list(schema_json_file)

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE"
    )

    job = client.load_table_from_dataframe(
        df, bq_table_ref, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(bq_table_ref)  # Make an API request.
    
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), bq_table_ref
        )
    )

def read_df_from_bq(bq_table_ref, client):

    query_string = f"""
    SELECT * FROM {bq_table_ref}
    """

    df = (
        client.query(query_string)
        .result()
        .to_dataframe()
    )
    
    return df