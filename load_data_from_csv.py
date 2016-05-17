#!/usr/bin/env python

# Copyright 2015, Google, Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Command-line application that loads data into BigQuery from a CSV file in
Google Cloud Storage.
This sample is used on this page:
    https://cloud.google.com/bigquery/loading-data-into-bigquery#loaddatagcs
For more information, see the README.md under /bigquery.
"""

import json
import time
import uuid
import yaml

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

table_name = string.replace(filename, ".csv", "")
bq_import_creds = yaml.load(open('bq_import_creds.yml'))
project_id = bq_import_creds['project_id']
dataset_id = bq_import_creds['dataset_id']
source_schema = bq_import_creds['schema']
source_path = cloud_storage_dir+table_id+".csv"

# [START load_table]
def load_table(bigquery, project_id, dataset_id, table_name, source_schema,
               source_path, num_retries=5):
    """
    Starts a job to load a bigquery table from CSV
    Args:
        bigquery: an initialized and authorized bigquery client
        google-api-client object
        source_schema: a valid bigquery schema,
        see https://cloud.google.com/bigquery/docs/reference/v2/tables
        source_path: the fully qualified Google Cloud Storage location of
        the data to load into your table
    Returns: a bigquery load job, see
    https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load
    """

    # Generate a unique job_id so retries
    # don't accidentally duplicate query
    job_data = {
        'jobReference': {
            'projectId': project_id,
            'job_id': str(uuid.uuid4())
        },
        'configuration': {
            'load': {
                'sourceUris': [source_path],
                'schema': {
                    'fields': source_schema
                },
                'destinationTable': {
                    'projectId': project_id,
                    'datasetId': dataset_id,
                    'tableId': table_name
                }
            }
        }
    }

    return bigquery.jobs().insert(
        projectId=project_id,
        body=job_data).execute(num_retries=num_retries)
# [END load_table]


# [START poll_job]
def poll_job(bigquery, job):
    """Waits for a job to complete."""

    print('Waiting for job to finish...')

    request = bigquery.jobs().get(
        projectId=job['jobReference']['projectId'],
        jobId=job['jobReference']['jobId'])

    while True:
        result = request.execute(num_retries=2)

        if result['status']['state'] == 'DONE':
            if 'errorResult' in result['status']:
                raise RuntimeError(result['status']['errorResult'])
            print('Job complete.')
            return

        time.sleep(1)
# [END poll_job]


# [START run]
def main(project_id, dataset_id, table_name, schema_file, data_path,
         poll_interval, num_retries):
    # [START build_service]
    # Grab the application's default credentials from the environment.
    credentials = GoogleCredentials.get_application_default()

    # Construct the service object for interacting with the BigQuery API.
    bigquery = discovery.build('bigquery', 'v2', credentials=credentials)
    # [END build_service]

    with open(schema_file, 'r') as f:
        schema = json.load(f)

    job = load_table(
        bigquery,
        project_id,
        dataset_id,
        table_name,
        schema,
        data_path,
        num_retries)

    poll_job(bigquery, job)
# [END run]

