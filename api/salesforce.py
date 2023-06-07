import json
import time
from typing import List
from xmlrpc.client import boolean
import requests
from api.utilities import get_logger
from advisorschoice import settings

POLL_INTERVAL = 5

logger = get_logger('salesforce', 'DEBUG')


def get_access_token() -> str:
    logger.info("Getting access token...")
    host = 'https://lifeease.my.salesforce.com/services/oauth2/token'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    payload = (
        'grant_type=password'
        + f'&client_id={settings.SALESFORCE_CLIENT_ID}'
        + f'&client_secret={settings.SALESFORCE_CLIENT_SECRET}'
        + f'&username={settings.SALESFORCE_USERNAME}'
        + f'&password={settings.SALESFORCE_PASSWORD}'
    )
    response = requests.post(host, headers=headers, data=payload)
    response = response.json()
    # logger.info(response)
    logger.info(response['access_token'])
    return response['access_token']


def debug_request(response):
    logger.debug(f"response.request.url: {response.request.url}")
    logger.debug(f"response.request.headers: {response.request.headers}")
    logger.debug(f"response.request.content: {response.request.body}")
    logger.debug(f"response.status_code: {response.status_code}")
    logger.debug(f"response.content: {response.content}")

# https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_queryall.htm
def rest_query_all(query: str) -> List[dict]:
    """Uses Salesforce API to execute `query`

    Args:
        query (str): SOQL Query (e.g. "select fields(all) from Account limit 200")

    Returns:
        List[dict]: list of records from API.
    """
    
    # HEADER of requests
    HEADERS = {
        'Authorization': f'Bearer {get_access_token()}',
        'Content-Type': 'application/json',
    }
    
    records = []
    logger.debug(query)
    response = requests.get(settings.SALESFORCE_API_URL + f'queryAll/?q={query}', headers=HEADERS)
    ## get the endpoint (Used for testing API requests)
    # print(settings.SALESFORCE_API_URL + f'queryAll/?q={query}')
    logger.info(response.status_code)
    # debug_request(response)

    if response.status_code == 200:
        cnt = 0
        response_json = response.json()
        # debug_request(response)
        logger.debug(response_json.keys())
        records.extend(response_json['records'])
        # print(json.dumps(records[0], indent=4, sort_keys=True))

        while 'nextRecordsUrl' in response_json.keys():
            # logger.info('Entering while loop...')
            query_identifier = response_json['nextRecordsUrl'].split('/')[-1]
            response = requests.get(settings.SALESFORCE_API_URL + f'queryAll/{query_identifier}', headers=HEADERS)
            print(settings.SALESFORCE_API_URL + f'queryAll/{query_identifier}')
            response_json = response.json()
            records.extend(response_json['records'])
            logger.info(f"{cnt}-{len(response_json['records'])}")
            cnt += 1
    else:
        logger.warning("First query went wrong!")
        debug_request(response)

    logger.info(len(records))
    return records

def compare_string(number1: str, number2: str) -> boolean:
    if len(number1) != len(number2):
        return len(number1) < len(number2)
    
    for ch1,ch2 in zip(number1, number2):
        if ch1 != ch2:
            return ch1 < ch2

# https://developer.salesforce.com/docs/marketing/pardot/guide/object-field-reference.html
# https://developer.salesforce.com/docs/marketing/pardot/guide/version4.html
def query_pardot(query: str, id: str, access_token: str) -> List[dict]:
    """ Use Pardot API to execute a query

    Args:
        query (str): object field
        id (int): Pardot ID

    Returns:
        List[dict]: List of records returned
    """
    
    # Headers of the request
    HEADERS = {
        'Authorization': 'Bearer ' + access_token,
        'Content-Type': 'application/json',
        'Pardot-Business-Unit-Id': f'{settings.PARDOT_UNIT_ID}'
    }
    
    records = []
    # logger.debug(query)

    url = f'{settings.PARDOT_API_URL}{query}/version/4/do/query?format=json&sort_by=id&sort_order=ascending&id_greater_than={id}'
    
    response = requests.get(url, headers=HEADERS)
    # logger.info(response.status_code)

    if response.status_code == 200:
        cnt = 0
        response_json = response.json()
        # logger.debug(response_json.keys())
        if query in response_json['result']:
            records.extend(response_json['result'][query])
        # print(json.dumps(records[0], indent=4))
    else:
        logger.warning("First query went wrong!")
        debug_request(response)

    # logger.info(len(records))
    # logger.info(records[-1])
    
    # comment this part of code to generate model code if dont need to get all the records
    #  
    if len(records) > 0:
        biggest_id = str(records[-1]['id'])
        logger.info(biggest_id)
        if compare_string(id, biggest_id) == True:
            records.extend(query_pardot(query, biggest_id, access_token))
    
    # logger.info(len(records))
    return records

def query_pardot_2(query: str, id: str, access_token: str):
    """ Use Pardot API to execute a query

    Args:
        query (str): object field
        id (int): Pardot ID

    Returns:
        List[dict]: List of records returned
    """
    
    # Headers of the request
    HEADERS = {
        'Authorization': 'Bearer ' + access_token,
        'Content-Type': 'application/json',
        'Pardot-Business-Unit-Id': f'{settings.PARDOT_UNIT_ID}'
    }
    
    records = []
    # logger.debug(query)

    url = f'{settings.PARDOT_API_URL}{query}/version/4/do/query?format=json&sort_by=id&sort_order=ascending&id_greater_than={id}'
    
    response = requests.get(url, headers=HEADERS)
    # logger.info(response.status_code)

    if response.status_code == 200:
        cnt = 0
        response_json = response.json()
        # logger.debug(response_json.keys())
        if query in response_json['result']:
            records.extend(response_json['result'][query])
        # print(json.dumps(records[0], indent=4))
    else:
        logger.warning("First query went wrong!")
        debug_request(response)

    # logger.info(len(records))
    # logger.info(records[-1])
    
    # comment this part of code to generate model code if dont need to get all the records
    #  
    biggest_id = id
    if len(records) > 0:
        biggest_id = str(records[-1]['id'])
        logger.info(biggest_id)
        # if compare_string(id, biggest_id) == True:
            # records.extend(query_pardot(query, biggest_id, access_token))
    
    # logger.info(len(records))
    return (records, biggest_id)

