from dataclasses import replace
import logging
import sys
from typing import Any, Dict, List, Tuple
import json
from api import utilities
from advisorschoice import settings
from dateutil import tz

from bdb import Breakpoint
from itertools import count
import json
# from tokenize import String
from xmlrpc.client import boolean
from matplotlib.font_manager import json_dump
import requests
import logging
import os
import re
import sys
import time
import urllib
from datetime import datetime
# import datetime
import threading
import hubspot
from asyncio.log import logger
from logging import Logger
from datetime import timezone
from typing import Any, Dict, List, Tuple
from django.db import transaction, IntegrityError
from django.http import HttpRequest, HttpResponse, JsonResponse
from rest_framework import status
from psycopg2 import Error
from advisorschoice import settings
from api import models
from django.http.request import HttpRequest
from django.http.response import HttpResponse


HS_API_VALUE = settings.HS_API_VALUE
HS_API_KEY = settings.HS_API_KEY
PARAMS = { settings.HS_API_KEY: settings.HS_API_VALUE }
HS_BASE_URL = settings.HS_BASE_URL
RETRY_INTERVAL = 2000
HEADERS = { 'Content-Type': 'application/json' }


# https://developers.hubspot.com/docs/api/crm/deals
def get_object_properties(
    object_name: str, filter_keywords: List[str], except_names: List[str],
    exclude_archived: bool = False, exclude_read_only: bool = False,
) -> List[Tuple[str, str]]:
    """
    Example:
    >>> properties = get_object_properties('companies', [], [], exclude_archived=True, exclude_read_only=True)
    >>> for name, label in properties:
    >>>     print(name, label)

    Args:
        object_name (str): [description]
        filter_keywords (List[str]): [description]
        except_names (List[str]): [description]
        exclude_archived (bool, optional): [description]. Defaults to False.
        exclude_read_only (bool, optional): [description]. Defaults to False.

    Returns:
        List[Tuple[str, str]]: Returns properties' name and label
    """
    response: requests.Response = requests.get(
        HS_BASE_URL + f'/crm/v3/properties/{object_name}',
        params={
            HS_API_KEY: HS_API_VALUE,
        }
    )

    if response.status_code == 200:
        response_json: Dict[Any, Any] = response.json()
        results = response_json['results']
        logger.debug(results[0].keys())
        
        if exclude_archived:
            results = [x for x in results if ('archived' not in x) or ('archived' in x and x['archived'] is False)]
            
        if exclude_read_only:
            results = [x for x in results if x['modificationMetadata']['readOnlyValue'] is False]
        
        results = [[x['name'], x['label']] for x in results]
    else:
        return []

    # Filter properties
    for kw in filter_keywords:
        results = [x for x in results if (kw not in x['name']) or (x['name'] in except_names)]
    
    logger.info(f"{len(results)=}")
    return results

def get_logger(name: str, level: str) -> logging.Logger:
    logger = logging.getLogger(name)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '%(filename)s:%(lineno)s-%(funcName)20s-%(levelname)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)
    return logger

logger = get_logger('hubspot', 'DEBUG')

def clean_name(name: str) -> str:
    if name is None:
        return name

    for i in range(len(name)):
        if i == 0 or name[i - 1] == ' ':
            name = name[:i] + name[i].upper() + name[i + 1:]
        else:
            name = name[:i] + name[i].lower() + name[i + 1:]

    return name  

def clean_phone(phone: str) -> str:
    count = 0
    res = "+1 ("

    if phone is None:
        return phone
    
    for ch in phone:
        if ch >= '0' and ch <= '9':
            count = count + 1
    
    if count != 10:
        return phone
    
    count = 0
    for ch in phone:
        if ch >= '0' and ch <= '9':
            res = res + ch
            count = count + 1
            if count == 3:
                res = res + ") "
            if count == 6:
                res = res + "-"

    return res 


def map_properties(obj, source_attrs: List[str], dest_attrs: List[str]) -> dict:
    res: dict = {}

    for (source_attr, dest_attr) in zip(source_attrs, dest_attrs):
        if dest_attr == 'ip_region' or dest_attr == 'ip_city' or dest_attr == 'notes_next_activity_date':
            continue

        # if dest_attr == 'last_activity_visitor_activity_created_at' or dest_attr == 'last_activity_at' or dest_attr == 'crm_last_sync' or dest_attr == 'notes_next_activity_date':
        #     continue
        
        res[dest_attr] = getattr(obj, source_attr)

        if dest_attr == 'dealstage':
            temp_dict = {'Proof': '18020193', 'Proposal Viewed': '18020194', 'Future Opportunity': '18020195', 'Deep Discovery': '18020196', 'Closed Won': '18020197', 'Near Loss': '18020198', 'Prospecting': '18020199', 'Stalled': '18020200', 'Sold': '18020201', 'Pre-Prospecting': '18020202', 'Proposal Sent': '18020203', 'Proposal Expired': '18020204', 'Business Case': '18020205', 'Discovery': '18020206', 'Demo Run': '18020207', 'Near Win': '18020208', 'Demo Set': '18020209', 'Closed Lost': 'closedlost'}
            res[dest_attr] = temp_dict[res[dest_attr]]

        if dest_attr == 'closedate':
            time_string = res[dest_attr]
            time_string = time_string[:10]
            res[dest_attr] = time_string

        if dest_attr == 'created_at' or dest_attr == 'updated_at' or dest_attr == 'last_activity_visitor_activity_created_at' or dest_attr == 'last_activity_at' or dest_attr == 'crm_last_sync':
            time_str = str(res[dest_attr])
            res[dest_attr] = date_to_timestamp(time_str.replace('-', '/'), '%Y/%m/%d')

        if dest_attr == 'firstname' or dest_attr == 'lastname':
            new_name = clean_name(name = res[dest_attr])
            res[dest_attr] = new_name   

        if dest_attr == 'phone' or dest_attr == 'mobilephone':
            new_phone = clean_phone(phone = res[dest_attr])
            res[dest_attr] = new_phone
        
        if dest_attr == 'domain':
            new_domain = extract_company_domain(email = getattr(obj, source_attr))
            res[dest_attr] = new_domain
        
    res['created_from_api'] = 'yes'

    return res


def extract_company_domain(email) -> str:
    # Extract email domain. Hubspot can detect company based on email
    if email is not None and email != "":
        company_domain = email.split('@')[-1]
        common_domains = ['gmail', 'outlook', 'hotmail', 'yahoo', 'aol', 'yandex']
        if any([company_domain in x for x in common_domains]):
            company_domain = ''
    else:
        company_domain = ''

    return company_domain

def create_deal():
    records: List[models.Opportunity] = models.Opportunity.objects.filter(at_hubspot='no')
    logger.info(f"Found {len(records)=}")

    source_attributes = ['id', 'name', 'value', 'probability', 'type', 'stage', 'status', 'closed_at', 'created_at', 'updated_at', 'campaign_id', 'campaign_name', 'prospects_prospect_id', 'prospects_prospect_first_name', 'prospects_prospect_last_name', 'prospects_prospect_email', 'prospects_prospect_company', 'campaign_crm_fid']

    dest_attributes = ['pardot_opportunity_id', 'dealname', 'amount', 'probability', 'dealtype', 'dealstage', 'deal_status', 'closedate', 'created_at', 'updated_at', 'pardot_campaign_id', 'pardot_campaign_name', 'pardot_prospect_id', 'contact_first_name', 'contact_last_name', 'contact_email', 'company_name', 'pardot_campaign_crm_fid']

    count = 0
    for row in records:
        if count == 5:
            break
        id_prospect = row.prospects_prospect_id
        records1: List[models.Prospect] = models.Prospect.objects.filter(id=id_prospect)
        if len(records1) == 0:
            continue

        properties = map_properties(
            obj = row,
            source_attrs = source_attributes,
            dest_attrs = dest_attributes
        )
        try:
            response = requests.post(
                url = f'https://api.hubapi.com/crm/v3/objects/deals?hapikey={HS_API_VALUE}',
                headers = {'Content-Type': 'application/json'},
                data = json.dumps({"properties": properties})
            )
            if response.status_code == 201:
                count += 1
                print(count)
                response = response.json()
                logger.info(f"{row.id=} deal created on Hubspot")
                models.Opportunity.objects.filter(id = row.id).update(at_hubspot=response['id'])
                create_contact_company_using_id_and_associate(prospect_id=row.prospects_prospect_id, deal_id=response['id'])
            else:
                logger.warning("Create deal failed")
                logger.warning(response.text)
        except Exception as e:
            logger.warning(e)

    return JsonResponse({"status": status.HTTP_200_OK})

def create_contact_company_using_id_and_associate(prospect_id: str, deal_id: str):
    records: List[models.Prospect] = models.Prospect.objects.filter(id=prospect_id)
    logger.info(f"Found {len(records)=} prospect")

    ok_contact = 0
    id_contact = ""
    ok_company = 0
    id_company = ""


    # create contact
    for row in records:
        print(type(row))
        source_attributes = ['id', 'grade', 'Time_in_App_Last_7', 'last_activity_visitor_activity_list_email_id', 'Win_Loss_Stage', 'Subscription_Start_Date', 'assigned_to_user_first_name', 'is_reviewed', 'Interest_Level', 'Projects_Created_Yesterday', 'STACK_UAG_Member', 'assigned_to_user_role', 'last_activity_visitor_activity_visitor_id', 'Qualification', 'Next_Event_Date', 'state', 'country', 'STACK_Created_Date', 'Completed_Takeoffs_Last_30', 'On_Demand_Demo', 'last_activity_visitor_activity_campaign_id', 'Language_from_Pendo', 'Next_Activity_Date', 'Subscription', 'prospect_account_id', 'SalesLoft_Next_Step_Contact', 'Lead_Notes', 'Custom_Projects_Created_Last_30', 'Full_Name', 'Sub_Segment_Assigned', 'Other_Reasons_UAB', 'Most_Excited_to_Share_About_STACK', 'website', 'crm_owner_fid', 'Qualification_Reason', 'Referral_Contact_Link', 'Active_User', 'email', 'Training_Webinar_2', 'Partner_Lead', 'Completed_Takeoffs_Last_7', 'campaign_id', 'Projects_Deleted_Last_30', 'StackUser_ID', 'assigned_to_user_email', 'Web_Browser', 'NPS_Score', 'Qualification_Notes', 'fax', 'first_name', 'NPS_Date', 'city', 'updated_at', 'created_at', 'Training_Complete', 'Days_Active_Last_30_Days', 'UAB_status', 'SC_Industry', 'Free_Subscription_Date', 'job_title', 'company', 'Mktg_Group', 'last_activity_at', 'SA_Industry', 'Operating_System', 'Subscription_Type', 'Primary_Reason_For_Joining', 'Training_Webinar_1', 'Contact_Owner_Calendly', 'phone', 'salesforce_fid', 'IP_City', 'crm_url', 'IP_Region', 'crm_last_sync', 'MKTG_Source', 'Referral_Link', 'Training_1', 'score', 'last_name', 'Minutes_on_Site_Last_30_Days', 'Demo_Completed', 'Qualification_Sub_Status', 'Subscription_End_Date', 'Subscription_Status', 'Referral_Contact_Name', 'last_activity_visitor_activity_created_at', 'recent_interaction', 'Referral_Contact_Email', 'Sub_Segment_Group', 'zip', 'crm_contact_fid', 'opted_out', 'notes', 'Number_of_Estimators_Demo_Form', 'Avg_Project_Value_ROI_Calc', 'Language_Preference_Demo_Form', 'Role', 'last_activity_visitor_activity_landing_page_id', 'address_two', 'crm_account_fid', 'Lofted', 'SalesLoft_Next_Step_Lead', 'password', 'last_activity_visitor_activity_form_id', 'is_do_not_call', 'Last_Demonstration_Date', 'annual_revenue', 'is_do_not_email', 'last_activity_visitor_activity_visitor_page_view_id', 'Trades', 'Hours_Bid_ROI_Calc', 'Additional_Bids_Month_ROI_Calc']
        
        dest_attributes = ['id', 'grade', 'time_in_app_last_7', 'last_activity_visitor_activity_list_email_id', 'win_loss_stage', 'subscription_start_date', 'assigned_to_user_first_name', 'is_reviewed', 'interest_level', 'projects_created_yesterday', 'stack_uag_member', 'assigned_to_user_role', 'last_activity_visitor_activity_visitor_id', 'qualification', 'next_event_date', 'state', 'country', 'stack_created_date', 'completed_takeoffs_last_30', 'on_demand_demo', 'last_activity_visitor_activity_campaign_id', 'language_from_pendo', 'notes_next_activity_date', 'subscription', 'prospect_account_id', 'salesloft_next_step_contact', 'lead_notes', 'custom_projects_created_last_30', 'full_name', 'sub_segment_assigned', 'other_reasons_uab', 'most_excited_to_share_about_stack', 'website', 'crm_owner_fid', 'qualification_reason', 'referral_contact_link', 'active_user', 'email', 'training_webinar_2', 'partner_lead', 'completed_takeoffs_last_7', 'campaign_id', 'projects_deleted_last_30', 'stackuser_id', 'assigned_to_user_email', 'web_browser', 'nps_score', 'qualification_notes', 'fax', 'firstname', 'nps_date', 'city', 'updated_at', 'created_at', 'training_complete', 'days_active_last_30_days', 'uab_status', 'sc_industry', 'free_subscription_date', 'jobtitle', 'company', 'mktg_group', 'last_activity_at', 'sa_industry', 'operating_system', 'subscription_type', 'primary_reason_for_joining', 'training_webinar_1', 'contact_owner_calendly', 'phone', 'salesforce_fid', 'ip_city', 'crm_url', 'ip_region', 'crm_last_sync', 'mktg_source', 'referral_link', 'training_1', 'score', 'lastname', 'minutes_on_site_last_30_days', 'demo_completed', 'qualification_sub_status', 'subscription_end_date', 'subscription_status', 'referral_contact_name', 'last_activity_visitor_activity_created_at', 'recent_interaction', 'referral_contact_email', 'sub_segment_group', 'zip', 'crm_contact_fid', 'opted_out', 'notes', 'number_of_estimators_demo_form', 'avg_project_value_roi_calc', 'language_preference_demo_form', 'role__c', 'last_activity_visitor_activity_landing_page_id', 'address_two', 'crm_account_fid', 'lofted__c', 'salesloft_next_step_lead', 'password', 'last_activity_visitor_activity_form_id', 'is_do_not_call', 'last_demonstration_date__c', 'annual_revenue', 'is_do_not_email', 'last_activity_visitor_activity_visitor_page_view_id', 'trades', 'hours_bid_roi_calc', 'additional_bids_month_roi_calc']
        
        contact_id = contact_exists(getattr(row, 'email', None))
        if contact_id:
            id_contact = contact_id
            ok_contact = 1
            logger.info(f"{id_contact} contact existed on Hubspot")
            update_object('contacts', contact_id, {
                'created_from_api': 'yes',
            })
            continue

        properties = map_properties(
            obj = row,
            source_attrs = source_attributes,
            dest_attrs = dest_attributes
        )
        try:
            response = requests.post(
                url = f'https://api.hubapi.com/crm/v3/objects/contacts?hapikey={HS_API_VALUE}',
                headers = {'Content-Type': 'application/json'},
                data = json.dumps({"properties": properties})
            )
            if response.status_code == 201:
                response = response.json()
                logger.info(f"{row.id=} contact created on Hubspot")
                id_contact = response['id']
                ok_contact = 1
            else:
                logger.warning("Create contact failed")
                logger.warning(response.text)
        except Exception as e:
            logger.warning(e)

    # create company
    for row in records:
        source_attributes = ['Won_Opportunities_New_Sale', 'campaign_name', 'Win_Loss_Analysis', 'Stack_Leadsource', 'last_activity_visitor_activity_campaign_name', 'STACK_User_Role', 'SA_Annual_Revenue', 'Projects_Deleted_Last_7', 'last_activity_visitor_activity_email_template_id', 'Segment_Assigned', 'last_activity_visitor_activity_id', 'industry', 'Company_Annual_Revenue', 'last_activity_visitor_activity_campaign_crm_fid', 'last_activity_visitor_activity_custom_redirect_id', 'company', 'Consent_Provided', 'Bids_Month_ROI_Calc', 'SA_Number_of_Users', 'assigned_to_user_account', 'Description', 'assigned_to_user_job_title', 'Projects_Created_Last_7', 'source', 'last_activity_visitor_activity_prospect_id', 'Company_Trades', 'address_one', 'last_activity_visitor_activity_details', 'Last_Session_Date', 'Subscription_Quantity', 'Projects_Deleted_Yesterday', 'STACK_Company_ID', 'Last_Visit_from_Pendo', 'assigned_to_user_last_name', 'Time_in_App_Yesterday', 'assigned_to_user_id', 'last_activity_visitor_activity_type_name', 'Lost_Opportunities_New_Sale', 'Trade_Market']
        dest_attributes = ['won_opportunities_new_sale', 'campaign_name', 'win_loss_analysis', 'stack_leadsource', 'last_activity_visitor_activity_campaign_name', 'stack_user_role', 'sa_annual_revenue', 'projects_deleted_last_7', 'last_activity_visitor_activity_email_template_id', 'segment_assigned', 'last_activity_visitor_activity_id', 'industry', 'company_annual_revenue', 'last_activity_visitor_activity_campaign_crm_fid', 'last_activity_visitor_activity_custom_redirect_id', 'name', 'consent_provided', 'bids_month_roi_calc', 'sa_number_of_users', 'assigned_to_user_account', 'description', 'assigned_to_user_job_title', 'projects_created_last_7', 'source', 'last_activity_visitor_activity_prospect_id', 'company_trades', 'address', 'last_activity_visitor_activity_details', 'last_session_date', 'subscription_quantity', 'projects_deleted_yesterday', 'stack_id__c', 'last_visit_from_pendo', 'assigned_to_user_last_name', 'time_in_app_yesterday', 'assigned_to_user_id', 'last_activity_visitor_activity_type_name', 'lost_opportunities_new_sale', 'trade_market']
        
        company_id = company_exists(getattr(row, 'company', None))
        if contact_id:
            id_company = company_id
            ok_company = 1
            logger.info(f"{id_company} company existed on Hubspot")
            update_object('companies', company_id, {
                'created_from_api': 'yes',
            })
            continue

        properties = map_properties(
            obj = row,
            source_attrs = source_attributes,
            dest_attrs = dest_attributes
        )
        try:
            response = requests.post(
                url = f'https://api.hubapi.com/crm/v3/objects/companies?hapikey={HS_API_VALUE}',
                headers = {'Content-Type': 'application/json'},
                data = json.dumps({"properties": properties})
            )
            if response.status_code == 201:
                response = response.json()
                logger.info(f"{row.id=} company created on Hubspot")
                id_company = response['id']
                ok_company = 1
            else:
                logger.warning("Create company failed")
                logger.warning(response.text)
        except Exception as e:
            logger.warning(e)
    
    if ok_company == 1 and ok_contact == 1:
        for row in records:
            mix_id = id_contact + "," + id_company
            models.Prospect.objects.filter(id = row.id).update(at_hubspot=mix_id)
    
    if id_contact != "":
        try:
            response = requests.put(
                url = f'https://api.hubapi.com/crm/v3/objects/deals/{deal_id}/associations/contacts/{id_contact}/deal_to_contact?hapikey={HS_API_VALUE}',
                headers = {'Content-Type': 'application/json'},
            )
            if response.status_code == 200:
                response = response.json()
                logger.info("Associated deal to contact on Hubspot")
            else:
                logger.warning("Associate deal to contact failed")
                logger.warning(response.text)
        except Exception as e:
            logger.warning(e)

    if id_company != "":
        try:
            response = requests.put(
                url = f'https://api.hubapi.com/crm/v3/objects/deals/{deal_id}/associations/companies/{id_company}/deal_to_company?hapikey={HS_API_VALUE}',
                headers = {'Content-Type': 'application/json'},
            )
            if response.status_code == 200:
                response = response.json()
                logger.info("Associated deal to company on Hubspot")
            else:
                logger.warning("Associate deal to company failed")
                logger.warning(response.text)
        except Exception as e:
            logger.warning(e)

    return JsonResponse({"status prospect": status.HTTP_200_OK})


def create_contact_properties():
    from Contact_Hubspot_Properties import properties_list1
    custom_properties = properties_list1
    logger.info(len(custom_properties))
    count = 0
    for property in custom_properties:
        try:
            response = requests.post(
                url='https://api.hubapi.com/crm/v3/properties/contacts?hapikey=e67afaa6-f08a-49c3-bdc9-7f4862473bf2',
                headers={'Content-Type': 'application/json'},
                data=json.dumps(property)
            )
            if response.status_code == 201:
                count += 1
                logger.info(f"Property {property['name']} was added to hubspot!")
                logger.info(f"Property {count}")
            else:
                logger.warning(f"Property {property['name']} added failed")
                logger.warning(response.text)
        except Exception as e:
            logger.warning(e)

    return JsonResponse({"status prospect": status.HTTP_200_OK})

def create_company_properties():
    from Companies_Hubspot_Properties import properties_list2
    custom_properties = properties_list2
    logger.info(len(custom_properties))
    count = 0
    for property in custom_properties:
        try:
            response = requests.post(
                url='https://api.hubapi.com/crm/v3/properties/companies?hapikey=e67afaa6-f08a-49c3-bdc9-7f4862473bf2',
                headers={'Content-Type': 'application/json'},
                data=json.dumps(property)
            )
            if response.status_code == 201:
                count += 1
                logger.info(f"Property {property['name']} was added to hubspot!")
                logger.info(f"Property {count}")
            else:
                logger.warning(f"Property {property['name']} added failed")
                logger.warning(response.text)
        except Exception as e:
            logger.warning(e)

    return JsonResponse({"status prospect": status.HTTP_200_OK})

def get_pipeline_id():
    stage: dict = {}
    response = requests.get(
        url = f'https://api.hubapi.com/crm/v3/pipelines/deals/default/stages?archived=false&hapikey={settings.HS_API_VALUE}'
    )
    if response.status_code == 200:
        response = response.json()
        # logger.info(json.dumps(response, indent=4))
        for data in response["results"]:
            stage[data["label"]] = data["id"]
    else:
        logger.info("FAILLLLLLL")
        logger.info(response.text)

    print(stage)

def contact_exists(email: str, firstname: str = '', lastname: str='', phone: str='') -> bool:
    if not email:
        return False
    
    search_url = HS_BASE_URL + '/crm/v3/objects/contacts/search'
    headers = {}
    headers['Content-Type']= 'application/json'
    data = json.dumps({
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "email",
                        "operator": "EQ",
                        "value": email,
                    }
                ]
            }
        ]
    })
    response = requests.post(data=data, url=search_url, headers=headers, params=PARAMS)
    response_json = response.json()
    try:
        contact_id = response_json['results'][0]['id']
        return contact_id
    except Exception as e:
        print(f"{email}: {e}")
        return False


def company_exists(company_name: str) -> bool:
    search_url = HS_BASE_URL + '/crm/v3/objects/companies/search'
    headers = {}
    headers['Content-Type']= 'application/json'
    data = json.dumps({
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "name",
                        "operator": "EQ",
                        "value": company_name
                    }
                ]
            }
        ]
    })
    try:
        response = requests.post(data=data, url=search_url, headers=headers, params=PARAMS)
        company_id = response.json()['results'][0]['id']
        return company_id
    except Exception as e:
        logger.warning(f"{company_name}: {e}")
        return False

def date_to_timestamp(date: str, date_format: str) -> str:
    try:
        date = date.replace('-', '/')[:len("yyyy/mm/dd")].rstrip()
        element = datetime.strptime(date, date_format)
        timestamp = datetime.timestamp(element)
        timestamp = str(int(int(str(timestamp).replace('.','')+'00') + 2.52e+7))
        return timestamp
    except ValueError:
        return None

def update_object(object_name: str, object_id: str, properties: dict) -> bool:
    url = f"{HS_BASE_URL}/crm/v3/objects/{object_name}/{object_id}"
    
    response: requests.Response = requests.patch(
        url,
        headers=HEADERS,
        params=PARAMS,
        data=json.dumps({"properties": properties})
    )
    
    if response.status_code == 200:
        print(f"Updated {object_name=} {object_id=} successfully")
        return True
    else:
        print(f"{response.status_code=} {response.reason=} {response.text=}")
        print(f"Failed {object_name=}")
        return False