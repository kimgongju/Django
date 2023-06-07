from typing import List, OrderedDict
from requests.api import request
import xmltodict
import requests
import json

import advisorschoice.settings as settings
from api.utilities import generate_model_code, get_attributes, get_logger, to_plural, to_snake

logger = get_logger('smarthome', 'DEBUG')
HEADERS = {
    'Content-Type': 'application/xml',
    'regKey': settings.SMARTHOME_REGKEY,
}


def read_xml_file(file_path: str) -> str:
    with open(file_path, 'r', encoding='utf-8') as f:
        xml = f.read()

    return xml


def get_xml_attributes(xml: str) -> List[str]:
    """
    <request version='1.0'>
        <header>...</header>
        <search pagesize="100">
            <object>
                <Contact>...</Contact>
            </object>
        </search>
    </request>
    """
    xmldict = xmltodict.parse(xml)
    obj = xmldict['request']['search']['object']
    entity_name = list(obj.keys())[0]
    attributes = get_attributes([obj[entity_name]])
    return attributes


def get_model_code(entity: str) -> str:
    xml = read_xml_file(f'./api/xml/{entity}.xml')
    attributes = get_xml_attributes(xml)
    model_code = generate_model_code(entity, to_plural(to_snake(entity)), attributes, primary_key='id', parent_class='TimeStamp')
    return model_code


def get_records(entity: str) -> List[dict]:
    """
    Read XML request file with name `entity` and send it as requests to SmartOffice to get records.

    Example response from SmartOffice's API:
    
    <response version="1.0">
        <header/>
        <search total="1" searchid="Mg==" more="true" pagesize="1" page="0">
            <Contact _type="obj" id="Contact.60667.43662944">
            </Contact>
        </search>
    </response>

    Args:
        entity (str): SmartOffice entity name.

    Returns:
        List[dict]: list of records as Python dictionary.
    """
    records, more, page = [], 'true', 0
    request_xml = read_xml_file(f'./api/xml/{entity}.xml')
    while more == 'true':
        response = requests.get(settings.SMARTHOME_URL, data=request_xml, headers=HEADERS)

        if response.status_code != 200:
            logger.warning(f"An error occurred: {response.status_code=}, stopping early")
            break
            
        response_dict = xmltodict.parse(response.text)
        # logger.info(json.dumps(response_dict, indent=4))
        response_search = response_dict['response']['search']

        # After first request, add searchid to xml request for pagination.
        # https://sidevkit.ez-data.com/Main/SearchOperation#Pagination
        if len(records) == 0:
            request_dict = xmltodict.parse(request_xml)
            request_dict['request']['search']['@searchid'] = response_search['@searchid']
            
            request_with_id = xmltodict.unparse(request_dict, pretty=True)
            request_xml = request_with_id

        new_records = response_search[entity]
        
        # Single instance returned
        if isinstance(new_records, dict) or isinstance(new_records, OrderedDict):
            new_records = [new_records]
            
        logger.info(f"Found {len(new_records)}")
        records.extend(new_records)
        more = response_search['@more']
        logger.info(f"{more=} {len(records)=}")
    return records
