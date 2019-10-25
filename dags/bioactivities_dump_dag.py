import datetime as dt
import functools
import threading
import logging
from urllib.parse import urljoin

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# I have no idea why relative import doesn't work and this one works
# Needs some more investigation
from hooks.custom_hooks import FixedPostgresHook
from utils.bioactivities_dump_utils import (
    keys_mapping,
    get_response_with_retry,
    set_rows_to_get
)

URL = 'https://drugtargetcommons.fimm.fi'
API_PATH = '/api/data/bioactivity/'
# Even though the limit is actually higher (tested with limit 1000)
# the API documentation
# [https://drugtargetcommons.fimm.fi/static/Excell_files/Api_documentation.pdf]
# states that maximum limit is 500
LIMIT_PER_PAGE = int(Variable.get('limit_per_page', default_var=500))

KEYS_TO_RETRIEVE = (
    'compound_name',
    'pubmed_id',
    'authors',
    'target_organism',
    'target_pref_name',
    'gene_name',
    'resource_uri'
)
UNIQUE_COLUMNS = ('resource_uri',)

MAX_THREADS = int(Variable.get('max_threads', default_var=5))


def _extract_and_load(api_url: str, offset: int, number_of_rows: int,
                      number_of_threads: int,) -> None:
    """
    Function that retrieves data from api_url and loads to database.
    Extraction starts with given offset and leaps
    every `number_of_threads * LIMIT_PER_PAGE` rows.

    :param api_url: API url from which data is extracted
    :param offset: Offset to start collecting the data
    :param number_of_rows: Number of rows to extract in total by all threads.
    :param number_of_threads: Number of threads that runs the extraction
    """

    rows_to_get = set_rows_to_get(number_of_rows, offset, LIMIT_PER_PAGE)

    hook = FixedPostgresHook(
        postgres_conn_id='db_connection'
    )

    while rows_to_get > 0:
        logging.info(f"Get {rows_to_get} rows with offset: {offset}")
        response = get_response_with_retry(
            api_url,
            params={'offset': offset, 'limit': rows_to_get, 'format': 'json'},
        )

        # If there was timeout for the request even after retries, log the
        # warning and continue extraction for different offset
        if not response:
            logging.warning(
                f"Couldn't retrieve data on offset: {offset}, "
                f"with limit: {LIMIT_PER_PAGE}"
            )

            offset += LIMIT_PER_PAGE * number_of_threads
            rows_to_get = set_rows_to_get(
                number_of_rows,
                offset,
                LIMIT_PER_PAGE
            )
            continue

        json_response = response.json()

        hook.insert_rows(
            'bioactivities',
            map(
                functools.partial(keys_mapping, keys=KEYS_TO_RETRIEVE),
                json_response['bioactivities']
            ),
            KEYS_TO_RETRIEVE,
            replace=True,
            unique_columns=UNIQUE_COLUMNS
        )

        offset += LIMIT_PER_PAGE * number_of_threads
        rows_to_get = set_rows_to_get(number_of_rows, offset, LIMIT_PER_PAGE)


def extract_and_load_bioactivities_data() -> None:
    """
    Function used in DAG task.
    Gets number of rows to extract and starts extraction/loading in
    different threads
    """
    api_url = urljoin(URL, API_PATH)

    _number_of_rows = Variable.get('number_of_rows', default_var=0)
    number_of_rows = int(_number_of_rows)
    # If number_of_rows wasn't set up or it's equal to zero
    # get all possible records
    if not number_of_rows:
        response = requests.get(api_url)
        json_response = response.json()
        number_of_rows = json_response['meta']['total_count']

    threads = list()
    for i in range(MAX_THREADS):
        thread = threading.Thread(
            target=_extract_and_load,
            args=(
                api_url,
                i * LIMIT_PER_PAGE,
                number_of_rows,
                MAX_THREADS
            )
        )
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()


default_args = {
    'owner': 'Pawel',
    'start_date': dt.datetime(1993, 12, 17),
    'concurrency': 1,
    'retries': 0
}

with DAG('bioactivities_dump',
         default_args=default_args,
         schedule_interval=None,
         ) as dag:

    dummy_start = DummyOperator(
        task_id='task_started'
    )

    opr_extract = PythonOperator(
        task_id='extract_and_load_bioactivities_data',
        python_callable=extract_and_load_bioactivities_data,
    )

    dummy_finish = DummyOperator(
        task_id='task_finished'
    )

dummy_start >> opr_extract >> dummy_finish
