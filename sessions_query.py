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

import argparse
import json
import time
import uuid
import yaml

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from datetime import date, timedelta

# date_offset controls how many days prior from current date to pull report.
date_offset = 1
yesterday = (date.today() - timedelta(date_offset)).strftime('%Y%m%d')
table_id = '{}_sessions'.format(yesterday)
bq_report_creds = yaml.load(open('bq_report_creds_ytd.yml'))
project_id = bq_report_creds['project_id']
dataset_id = bq_report_creds['dataset_id']
query = bq_report_creds['sessions_query'].format(dataset_id,date_offset)
# [START async_query]
def async_query(bigquery, project_id, query, batch=False, num_retries=5):
    # Generate a unique job_id so retries
    # don't accidentally duplicate query
    job_data = {
        'jobReference': {
            'projectId': project_id,
            'job_id': str(uuid.uuid4())
        },
        'configuration': {
            'query': {
                'query': query,
                'priority': 'BATCH' if batch else 'INTERACTIVE',
                'allowLargeResults': True,
                'destinationTable': {
                    'projectId': project_id,
                    'datasetId': dataset_id,
                    'tableId': table_id
                },
            "createDisposition": "CREATE_IF_NEEDED",
            "writeDisposition": "WRITE_TRUNCATE",
            }
        }
    }
    return bigquery.jobs().insert(
        projectId=project_id,
        body=job_data).execute(num_retries=num_retries)
# [END async_query]


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
def main(project_id, query_string, batch, num_retries, interval):
    # [START build_service]
    # Grab the application's default credentials from the environment.
    credentials = GoogleCredentials.get_application_default()

    # Construct the service object for interacting with the BigQuery API.
    bigquery = discovery.build('bigquery', 'v2', credentials=credentials)
    # [END build_service]

    # Submit the job and wait for it to complete.
    query_job = async_query(
        bigquery,
        project_id,
        query_string,
        batch,
        num_retries)

    poll_job(bigquery, query_job)

    # Page through the result set and print all results.
    page_token = None
    while True:
        page = bigquery.jobs().getQueryResults(
            pageToken=page_token,
            **query_job['jobReference']).execute(num_retries=2)

        print(json.dumps(page['rows']))

        page_token = page.get('pageToken')
        if not page_token:
            break
# [END run]


# [START main]
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    # parser.add_argument('project_id', help='Your Google Cloud project ID.')
    # parser.add_argument('query', help='BigQuery SQL Query.')
    parser.add_argument(
        '-b', '--batch', help='Run query in batch mode.', action='store_true')
    parser.add_argument(
        '-r', '--num_retries',
        help='Number of times to retry in case of 500 error.',
        type=int,
        default=5)
    parser.add_argument(
        '-p', '--poll_interval',
        help='How often to poll the query for completion (seconds).',
        type=int,
        default=1)

    args = parser.parse_args()

    main(
        project_id,
        query,
        args.batch,
        args.num_retries,
        args.poll_interval)
# [END main]
