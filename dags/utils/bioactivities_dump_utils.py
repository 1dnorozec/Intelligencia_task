import random
from time import sleep
from typing import Dict, Iterable, Tuple, Any, Optional

import requests
from requests import Response


def keys_mapping(row: Dict, keys: Iterable[str]) -> Tuple[Optional[Any]]:
    """
    Function gets `row` values from keys provided in `keys` iterable.

    :param row: Dictionary with values
    :param keys: Keys that value we want to get
    """
    return tuple(row.get(key) for key in keys)


def get_response_with_retry(url: str, params: Dict,
                            number_of_retries: int = 3) -> Response or None:
    """
    Try to send get request.
    If response code is different than 200, retry `number_of_retries` times.
    Before each retry sleep some random time from [1, 5) set.

    :param url: URL to which we send get request
    :param params: Params passed in get request
    :param number_of_retries: Number of retries
    """
    response = requests.get(url, params=params)
    if response.status_code != 200:
        if number_of_retries == 0:
            return None
        else:
            sleep(random.uniform(1, 5))
            return get_response_with_retry(url, params, number_of_retries-1)
    return response


def set_rows_to_get(number_of_rows: int, offset: int,
                    limit_per_page: int) -> int:
    """
    Function calculates the limit for the next get requests.
    Calculation is based on current offset,
    number of rows we want to get and api limit.

    :param number_of_rows: Number of all rows we want to get
    :param offset: Offset at which extraction is currently set
    :param limit_per_page: Limit per page
    :return:
    """
    if number_of_rows - offset > limit_per_page:
        rows_to_get = limit_per_page
    else:
        rows_to_get = number_of_rows - offset
    return rows_to_get
