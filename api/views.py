import os
import csv
import sys
import json
import time
import urllib
import logging
import operator
import datetime
import traceback
from typing import List

# import xlsxwriter
from rest_framework import status
from django.apps import apps
from django.db import models
from django.db import transaction
from django.db import IntegrityError
from django.db.utils import ProgrammingError
from django.http import JsonResponse
from django.http.response import HttpResponse
from django.http.request import HttpRequest

from api import hubspot as hs_utils
from .models import *
import api.smarthome as smarthome
from advisorschoice import settings
from .salesforce import query_pardot_2, rest_query_all, query_pardot, get_access_token
from .utilities import add_key, bulk_sync, generate_long_model_code, generate_model_code, get_attributes, get_first_attributes, get_logger, timestamp_to_date, bulk_sync
from .utilities import *

# Logger
BATCH_SIZE = 5000
logger = get_logger('views', 'DEBUG')


def test_receive_request(request: HttpRequest) -> HttpResponse:
    logger.info(request)

    return JsonResponse({"status": status.HTTP_200_OK})


def save_smarthome_contacts(request: HttpRequest) -> HttpResponse:
    """Save SmartOffice Contact entity.

    Args:
        request (HttpRequest): nothing.

    Returns:
        HttpResponse: [description]
    """
    records = smarthome.get_records('Contact')
    
    attributes = [field.name for field in Contact._meta.get_fields() if '__' not in field.name]
    logger.info(f"{attributes=}")
        
    obj_list = []
    for record in records:
        db_params = add_key(
            record,
            obj_params={},
            attributes=attributes,
            pref='',
            custom_names={},
            date_attributes=[],
        )
        db_params['id'] = record['@id']
        
        obj_list.append(Contact(**db_params))
        
    bulk_sync(Contact, obj_list=obj_list)
    return JsonResponse({"status": status.HTTP_200_OK})


def save_advisorschoice_agency(request: HttpRequest) -> HttpResponse:
    """Save SmartOffice Contact entity.

    Args:
        request (HttpRequest): nothing.

    Returns:
        HttpResponse: [description]
    """
    records = smarthome.get_records('Agent')
    
    attributes = [field.name for field in Agency._meta.get_fields() if '__' not in field.name]
    logger.info(f"{attributes=}")
        
    obj_list = []
    for record in records:
        db_params = add_key(
            record,
            obj_params={},
            attributes=attributes,
            pref='',
            custom_names={},
            date_attributes=[],
        )
        db_params['id'] = record['@id']
        obj_list.append(Agency(**db_params))
        
    bulk_sync(Agency, obj_list=obj_list)
    return JsonResponse({"status": status.HTTP_200_OK})

def save_advisorschoice_policy(request: HttpRequest) -> HttpResponse:
    """Save SmartOffice Contact entity.

    Args:
        request (HttpRequest): nothing.

    Returns:
        HttpResponse: [description]
    """
    # model_code = smarthome.get_model_code('Policy')
    # print(model_code)
    # return
    records = smarthome.get_records('Policy')
    
    attributes = [field.name for field in Policy._meta.get_fields() if '__' not in field.name]
    logger.info(f"{attributes=}")
        
    obj_list = []
    
    for record in records:
        db_params = add_key(
            record,
            obj_params={},
            attributes=attributes,
            pref='',
            custom_names={},
            date_attributes=[],
        )
        db_params['id'] = record['@id']
        obj_list.append(Policy(**db_params))
    temp = True
    
    # for obj1 in range(len(obj_list)):
    #     for obj2 in range(obj1 + 1, len(obj_list)):
    #         # if obj_list[obj1].id == 'Policy.60667.343' and obj_list[obj2].id == 'Policy.60667.343':
    #         if obj_list[obj1].id == obj_list[obj2].id:
    #             # for field in obj_list[obj1]
    #             print(obj_list[obj1].id)
    #             temp = False
    #             # print("obj1: " + obj1 + "    obj2: " + obj2)
    #     if temp == False:
    #         break
    # attributes=['created_at', 'updated_at', 'id', 'CreatedOn', 'ModifedOn', 'Contact_EmployerName', 'Contact_LastName', 'Contact_FirstName', 'Contact_Name', 'Contact_FirstLastName', 'Contact_FirstMiddleLastName', 'Contact_FullName']
                
    print("000000000000000000000000000")
    bulk_sync(Policy, obj_list=obj_list)
    return JsonResponse({"status": status.HTTP_200_OK})


def bulk_create(model: models.Model, obj_list: List[models.Model]):
    retry = True
    while retry:
        try:
            logger.info(f"Processed {len(obj_list)} records")
            num = model.objects.bulk_create(obj_list, batch_size=BATCH_SIZE, ignore_conflicts=True)
            logger.info(f"Bulk_created {len(num)} records")
            retry = False
        except Exception as e:
            print(e)
            print("Bulk_create error, retrying...")


def save_accounts(request):
    # # Code used to get raw attributes and models with cleaned attributes
    records = rest_query_all(f"select+fields(all)+from+Account+limit+1")
    # attributes = get_attributes(records)
    
    # # Get raw attributes from API excluding attributes_type and attributes_url
    # attributes.remove("attributes_type")
    # attributes.remove("attributes_url")
    # logger.info(f"{attributes=}")

    # # Print models with cleaned attributes (e.g. "$name" -> "_name", "a__c" -> "a_c")
    # for i in range(len(attributes)):
    #     attributes[i] = attributes[i].replace('__', '_')
    # print(generate_long_model_code(
    #     model_name='Account',
    #     table_name='salesforce_accounts',
    #     attributes=attributes,
    #     primary_key='Id',
    #     parent_class='TimeStamp',
    # ))
    
    # This part is used to go from raw attributes to model's parameter
    attributes = ['Id', 'IsDeleted', 'MasterRecordId', 'Name', 'RecordTypeId', 'ParentId', 'BillingStreet', 'BillingCity', 'BillingState', 
    'BillingPostalCode', 'BillingCountry', 'BillingLatitude', 'BillingLongitude', 'BillingGeocodeAccuracy', 'BillingAddress_city', 'BillingAddress_country', 'BillingAddress_geocodeAccuracy', 'BillingAddress_latitude', 'BillingAddress_longitude', 'BillingAddress_postalCode', 'BillingAddress_state', 'BillingAddress_street', 'ShippingStreet', 'ShippingCity', 'ShippingState', 'ShippingPostalCode', 'ShippingCountry', 'ShippingLatitude', 'ShippingLongitude', 'ShippingGeocodeAccuracy', 'ShippingAddress', 'Phone', 'Fax', 'AccountNumber', 'Website', 'PhotoUrl', 'Sic', 'Industry', 'AnnualRevenue', 'NumberOfEmployees', 'Ownership', 'TickerSymbol', 'Description', 'Rating', 'Site', 'OwnerId', 'CreatedDate', 'CreatedById', 'LastModifiedDate', 'LastModifiedById', 'SystemModstamp', 'LastActivityDate', 'LastViewedDate', 'LastReferencedDate', 'Jigsaw', 'JigsawCompanyId', 'AccountSource', 'SicDesc', 'Compensation__c', 'AgencyID__c', 'Email__c', 'Last_Modified_SmartOffice__c', 'Out_of_Sync__c', 'SmartOffice_ID__c', 'TaxID__c', 'LifeEase_Demo_Presentation__c', 'SmartOfficeAccountType__c', 'AcceptsPDFOfPolicy__c', 'Sandbox_EXTID__c', 'AllowsRefsTo3rdPartyServiceProvider__c', 'AllowsStaffToParticipateInLoyaltyProgram__c', 'RequiresOwnCollateralAssignmentForm__c', 'RequiresPhysicalPolicy__c', 'RequiresPolicyVerificationForm__c', 'RequiresProofOfPaidPremium__c', 'LendingInstituteFor__c', 'ltnadptn__Active__c', 'SmartOfficeID__c']
    first_attributes = get_first_attributes(records)
    first_attributes.remove("attributes")
    # print("OKOKOKOKOKOKOK")
    # print(first_attributes)
    records = rest_query_all(f"select {','.join(first_attributes)} from Account")
    
    # for record in records:
    #     print(json.dumps(record, indent=4))

    custom_names = {}
    for attr in attributes:
        custom_names[attr] = attr.replace('__', '_')
        
    obj_list = []
    
    for record in records:
        db_params = add_key(
            record,
            obj_params={},
            attributes=attributes,
            pref="",
            custom_names=custom_names,
            date_attributes={},
        )
        
        obj_list.append(Account(**db_params))
    
    # try to use bulk_sync instead of bulk_create
    try:
        logger.info(f"Finished {len(obj_list)}")
        bulk_create(Account, obj_list)
        # bulk_sync(Account, obj_list=obj_list)
    except Exception as e:
        logger.warning(e)
        logger.warning("Bulk_create error ...")
        # logger.warning("Bulk_sync error ...")
        
    return JsonResponse({"status": status.HTTP_200_OK})


def save_salesforce_contacts(request):
    # # Code used to get raw attributes and models with cleaned attributes
    records = rest_query_all(f"select+fields(all)+from+Contact+limit+1")
    # attributes = get_attributes(records)
    
    # # Get raw attributes from API excluding attributes_type and attributes_url
    # attributes.remove("attributes_type")
    # attributes.remove("attributes_url")
    # logger.info(f"{attributes=}")

    # # Print models with cleaned attributes (e.g. "$name" -> "_name", "a__c" -> "a_c")
    # for i in range(len(attributes)):
    #     attributes[i] = attributes[i].replace('__', '_')
    # print(generate_long_model_code  (
    #     model_name='Salesforce_Contact',
    #     table_name='salesforce_contacts',
    #     attributes=attributes,
    #     primary_key='Id',
    #     parent_class='TimeStamp',
    # ))
    
    # This part is used to go from raw attributes to model's parameter
    attributes = ['Id', 'IsDeleted', 'MasterRecordId', 'AccountId', 'LastName', 'FirstName', 'Salutation', 'Name', 'RecordTypeId', 'OtherStreet', 'OtherCity', 'OtherState', 'OtherPostalCode', 'OtherCountry', 'OtherLatitude', 'OtherLongitude', 'OtherGeocodeAccuracy', 'OtherAddress_city', 'OtherAddress_country', 'OtherAddress_geocodeAccuracy', 'OtherAddress_latitude', 'OtherAddress_longitude', 'OtherAddress_postalCode', 'OtherAddress_state', 'OtherAddress_street', 'MailingStreet', 'MailingCity', 'MailingState', 'MailingPostalCode', 'MailingCountry', 'MailingLatitude', 'MailingLongitude', 'MailingGeocodeAccuracy', 'MailingAddress_city', 'MailingAddress_country', 'MailingAddress_geocodeAccuracy', 'MailingAddress_latitude', 'MailingAddress_longitude', 'MailingAddress_postalCode', 'MailingAddress_state', 'MailingAddress_street', 'Phone', 'Fax', 'MobilePhone', 'HomePhone', 'OtherPhone', 'AssistantPhone', 'ReportsToId', 'Email', 'Title', 'Department', 'AssistantName', 'LeadSource', 'Birthdate', 'Description', 'OwnerId', 'HasOptedOutOfEmail', 'HasOptedOutOfFax', 'DoNotCall', 'CreatedDate', 'CreatedById', 'LastModifiedDate', 'LastModifiedById', 'SystemModstamp', 'LastActivityDate', 'LastCURequestDate', 'LastCUUpdateDate', 'LastViewedDate', 'LastReferencedDate', 'EmailBouncedReason', 'EmailBouncedDate', 'IsEmailBounced', 'PhotoUrl', 'Jigsaw', 'JigsawContactId', 'IndividualId', 'SSN__c', 'Gender__c', 'Occupation__c', 'JobTitle__c', 'Marital__c', 'Salary__c', 'Tobacco__c', 'TaxID__c', 'URL__c', 'IM_ID__c', 'Suffix__c', 'Advisor_CRD__c', 'AgencyID__c', 'Agent__c', 'Appointment_Source__c', 'Assistant_DOB__c', 'Assistant_Email__c', 'Assistant_Name__c', 'Assistant_Phone__c', 'BrokerDealer__c', 'Broker_Dealer_CRD__c', 'Broker_Dealer_Name__c', 'BusinessRecord__c', 'Business_City__c', 'Business_Office_Phone__c', 'Business_State__c', 'Business_Street_1__c', 'Business_Zip__c', 'CRD__c', 'CFP__c', 'Channel__c', 'CFA__c', 'ChFC__c', 'CIC__c', 'Comments_Notes__c', 'Compensation__c', 'ContactSubType__c', 'Credentials__c', 'Direct_Business_Phone__c', 'Fax_ID__c', 'Home_Phone_ID__c', 'pi__Needs_Score_Synced__c', 'How_Started_in_Business__c', 'Insurance__c', 'pi__Pardot_Last_Scored_At__c', 'Lead_Source_Description__c', 'Lead_Status_Value__c', 'Lead_Status__c', 'Life_License_More_Than_5_Years__c', 'Life_Licensed__c', 'Line_of_Business__c', 'List_Source__c', 
    'Log_a_Call__c', 'Mailing_ID__c', 'Mobile_ID__c', 'Mobile_Phone__c', 'Newsletter_Subscription__c', 'OBA_Insurance__c', 'OB_Tax_Planning__c', 'Office_Direct_Phone__c', 'Other_Address_ID__c', 'Agent_Id__c', 'Personal_Email__c', 'Personal_Financial_Specialist__c', 'Phone_ID__c', 'Preferred_Name__c', 'PrimaryAgent__c', 'Prior_Broker_Dealer_Name__c', 'RIA__c', 'Registered_with_Appointease__c', 'Rep_Website__c', 'Retirement_Plans__c', 'Scheduled_Tasks__c', 'Securities_Licensed__c', 'Securities_Production_Level__c', 'Securities_Registration__c', 'SmartOffice_ID__c', 'Total_AUM__c', 'Total_Years_Licensed__c', 'WebAddress_ID__c', 'Agent_Type__c', 'LegAdvID__c', 'Birthdate__c', 'Home_Address_Street_1__c', 'Home_Address_Street_2__c', 'Home_City__c', 'Home_State__c', 'Home_ZIp__c', 'Business_Street_2__c', 'Broker_Dealer__c', 'SmartOffice_XML__c', 'pi__campaign__c', 'pi__comments__c', 'pi__conversion_date__c', 'pi__conversion_object_name__c', 'pi__conversion_object_type__c', 'pi__created_date__c', 'pi__first_activity__c', 'pi__first_search_term__c', 'pi__first_search_type__c', 'pi__first_touch_url__c', 'pi__grade__c', 'pi__last_activity__c', 'pi__notes__c', 'pi__score__c', 'pi__url__c', 'pi__utm_campaign__c', 'pi__utm_content__c', 'pi__utm_medium__c', 'pi__utm_source__c', 'pi__utm_term__c', 'SyncSSN__c', 'rh2__Currency_Test__c', 'rh2__Describe__c', 'rh2__Formula_Test__c', 'rh2__Integer_Test__c', 'Sync_SmartOffice__c', 'Type__c', 'Designations__c', 'AccountID__c', 'TobaccoUser__c', 'Borrower_Referral_Fee__c', 'Hot_List__c', 'Company_Name__c', 'Role__c', 'LifeEase_Demo_Presentation__c', 'Agent_Type_Name__c', 'Service_Provider_Role__c', 'Phone_Extension__c', 'MailingAdditionalLine2__c', 'Material_Status__c', 'Rating__c', 'MailingAdditionalLine3__c', 'Business_Office_Phone_Extension__c', 'OtherAdditionalLine2__c', 'UpDate__c', 'SubType__c', 'Service_Provider_Type__c', 'pi__pardot_hard_bounced__c', 'PrimaryAgentID__c', 'Referred_By__c', 'ClientCompany__c', 'DeletedInSO__c', 'Business_Street__c', 'Case_Manager__c', 'Greeting__c', 'Home_Phone__c', 
    'Middle_Name__c', 'Review_Date__c', 'Status__c', 'Supervisor__c', 'of_Children__c', 'SmartOffice_Created_Date__c', 'SmartOffice_Modified_Date__c', 'Assistant_Email_ID__c', 
    'Email_ID__c', 'OtherPhone_Id__c', 'URL_ID__c', 'AEP__c', 'AIF__c', 'CEBS__c', 'CPA__c', 'CLU__c', 'CRPC__c', 'JD__c', 'MBA__c', 'LLM__c', 'Professional_Plan_Consultant_PPC__c', 'OtherAdditionalLine3__c', 'NoPolicies__c', 'Marketing_Mgr__c', 'Age__c', 'LifeEaseLMT__c', 'NotifyOnNewPushToSO__c', 'LE_BeneficiariesCoverage__c', 'LE_CalculatorUsed__c', 'LE_Debt__c', 'LE_ExistingInsurance__c', 'LE_InitialCoverage__c', 'LE_Mortgage__c', 'LE_ProfileId__c', 'LE_Savings__c', 'LE_Type__c', 'LE_UserId__c', 'rcsfl__SMS_Number__c', 'CaseSafeId__c', 'ClientInPardot__c', 'PardotProspectId__c', 'ClientTZOffset__c', 'ClientTime__c', 'LMT_UpdateFromSO__c', 'RecordTypeName__c', 'RecordTypeDevName__c', 'OfficeCode__c', 'Sub_Region_Pardot__c', 'LE_Sex__c', 'PRA_Count__c', 'Referral_Checkbox_PB__c', 'SmartOfficeID__c', 'LE_Created__c', 'DocumentRequested__c', 'LE_LenderId__c', 'LE_NumberOfChildren__c', 'LE_OwnerInsured__c', 'LE_Persona__c', 'LE_AnemiaCholesterol__c', 'Company_Lead__c', 'Reconciliation__c', 'do_merge_temp__c', 'LE_CurrentCoverageIssuedYear__c', 'LE_CurrentCoverageInsuranceType__c', 'MergeIndex__c', 'Account_SmartOfficeID__c', 'IfAcctSOidISContactSOid__c', 'dont_merge__c', 'GET__c', 'POST__c', 'LE_AnnualIncome__c', 'LE_AppliedCoverageCarrierApplied__c', 'LE_AppliedCoverageDateOfApplic__c', 'LE_AppliedCoverageOutcome__c', 'LE_AppliedCoverageThreeYears__c', 'LE_BrainMuscleNervousDisorder__c', 'LE_CancerTumor__c', 'LE_ChargesPendingDetails__c', 'LE_ChargesPending__c', 'LE_CoverageToReplace__c', 'LE_CurrentCovToBeReplaced__c', 'Gender_WS__c', 'LE_CurrentCoverageInsurance__c', 'LE_CurrentCoverageIssueDate__c', 'LE_CurrentCoverageOwner__c', 'LE_CurrentCoveragePolicyNumber__c', 'LE_CurrentCoveragePolicy__c', 'LE_CurrentOccupation__c', 'LE_CurrentlyHaveCoverage__c', 'LE_DepressionAnxiety__c', 'LE_Diabetes__c', 'LE_DriversLicenseNumber__c', 'LE_DriversLicenseRevDets__c', 'LE_DriversLicenseRevoked__c', 'LE_FamilyDiagnosed__c', 'LE_HangGliding__c', 'LE_HeartDisorder__c', 'LE_HouseholdIncome__c', 'LE_JointBoneDisorder__c', 'LE_KidneyDisorder__c', 
    'LE_LawfulCitizenDetails__c', 'LE_LawfulCitizen__c', 'LE_LungRespDisorder__c', 'LE_MiddleName__c', 'LE_MotorRacing__c', 'LE_MountainClimbing__c', 'LE_MovingViolDets__c', 'LE_MovingViolations__c', 'LE_NoneCondition__c', 'LE_NoneExtremeSport__c', 'LE_OtherConditionDets__c', 'LE_OtherCondition__c', 'LE_OtherExtremeSportDets__c', 'LE_OtherExtremeSport__c', 'LE_PhysicianAddress1__c', 'LE_PhysicianAddress2__c', 'LE_PhysicianCity__c', 'LE_PhysicianDateLastSeen__c', 'LE_PhysicianLastName__c', 'LE_PhysicianPhoneNumber__c', 'LE_PhysicianReasonVisit__c', 'LE_PhysicianState__c', 'LE_PhysicianZipCode__c', 'LE_PilotingAirplane__c', 'LE_PlansToTravelDetails__c', 'LE_PlansToTravel__c', 'LE_ReceivedCounselingDetails__c', 'LE_ReceivedCounseling__c', 'LE_RegularPhysician__c', 'LE_ScubaDiving__c', 'LE_SkyDiving__c', 'LE_SocialSecurityNumber__c', 'LE_StateCountryBorn__c', 'LE_StateDriversLicense__c', 'LE_StomachGastroDisorder__c', 'LE_StreetAddress2__c', 'LE_UsedCannabisDetails__c', 'LE_UsedCannabis__c', 'LE_ValidDriversLicense__c', 'LE_MinimumLoanAmount__c', 'LE_MaximumLoanAmount__c', 'LE_FormOfLender__c', 'LE_OtherFormOfLender__c', 'LE_OtherFormsOfSBALoans__c', 'LE_FirmUniqueValueProposition__c', 'LE_FormsOfLoans__c', 'LE_FormsOfSBALoans__c', 'LE_LimitedToStateRegion__c', 'LE_PreferredIndustries__c', 'Height__c', 'Weight__c', 'Campaign__c', 'First_Visit__c', 'Keywords__c', 'Last_Visit__c', 'Segmentation_Pardot__c', 'Medium__c', 'LE_ApproximateLoanCloseDate__c', 'LE_FullNameLender__c', 'LE_KeyContact__c', 'LE_LenderEmail__c', 'LE_LenderPhoneNumber__c', 'Pardot_Force_Sync__c', 'Source__c', 'AdGroup__c', 'Analytics_Source__c', 'AvgPosition__c', 'GCLID__c', 'CMPID__c', 'LE_CurrentCoverageFaceAmount__c']

    first_attributes = get_first_attributes(records)
    first_attributes.remove("attributes")
    # print("OKOKOKOKOKOKOK")
    # print(first_attributes)
    records = rest_query_all(f"select {','.join(first_attributes)} from Contact")
    
    # for record in records:
    #     print(json.dumps(record, indent=4))

    custom_names = {}
    for attr in attributes:
        custom_names[attr] = attr.replace('__', '_')
        
    obj_list = []
    
    for record in records:
        db_params = add_key(
            record,
            obj_params={},
            attributes=attributes,
            pref="",
            custom_names=custom_names,
            date_attributes={},
        )
        
        obj_list.append(Salesforce_Contact(**db_params))
    
    # try to use bulk_sync instead of bulk_create
    try:
        logger.info(f"Finished {len(obj_list)}")
        bulk_create(Salesforce_Contact, obj_list)
        # bulk_sync(Salesforce_Contact, obj_list=obj_list)
    except Exception as e:
        logger.warning(e)
        logger.warning("Bulk_create error ...")
        # logger.warning("Bulk_sync error ...")
        
    return JsonResponse({"status": status.HTTP_200_OK})


def save_opportunities(request):
    # # Code used to get raw attributes and models with cleaned attributes
    records = rest_query_all(f"select+fields(all)+from++Opportunity+limit+1")
    # attributes = get_attributes(records)
    
    # Get raw attributes from API excluding attributes_type and attributes_url
    # attributes.remove("attributes_type")
    # attributes.remove("attributes_url")
    # logger.info(f"{attributes=}")

    # # Print models with cleaned attributes (e.g. "$name" -> "_name", "a__c" -> "a_c")
    # for i in range(len(attributes)):
    #     attributes[i] = attributes[i].replace('__', '_')
    # print(generate_long_model_code(
    #     model_name='Opportunity',
    #     table_name='salesforce_opportunities',
    #     attributes=attributes,
    #     primary_key='Id',
    #     parent_class='TimeStamp',
    # ))
    
    # This part is used to go from raw attributes to model's parameter
    attributes = ['Id', 'IsDeleted', 'AccountId', 'RecordTypeId', 'IsPrivate', 'Name', 'Description', 'StageName', 'Amount', 'Probability', 'ExpectedRevenue', 'TotalOpportunityQuantity', 'CloseDate', 'Type', 'NextStep', 'LeadSource', 'IsClosed', 'IsWon', 'ForecastCategory', 'ForecastCategoryName', 'CampaignId', 'HasOpportunityLineItem', 'Pricebook2Id', 'OwnerId', 'Territory2Id', 'IsExcludedFromTerritory2Filter', 'CreatedDate', 'CreatedById', 'LastModifiedDate', 'LastModifiedById', 'SystemModstamp', 'LastActivityDate', 'PushCount', 'LastStageChangeDate', 'FiscalQuarter', 'FiscalYear', 'Fiscal', 'ContactId', 'LastViewedDate', 'LastReferencedDate', 
    'ContractId', 'HasOpenActivity', 'HasOverdueTask', 'LastAmountChangedHistoryId', 'LastCloseDateChangedHistoryId', 'Advisor__c', 'Annualized_Premium__c', 'BrokerDealer2__c', 'Carrier_Name__c', 'Client__c', 'Excess_Premium__c', 'FYC__c', 'Flat_Extra_1000__c', 'Modal_Commission_Premium__c', 'Modal_Premium__c', 'Pay_Method__c', 'Policy_Number__c', 'Premium_Mode__c', 'Premium__c', 'Product_Name__c', 'Product_Type__c', 'SmartOffice_ID__c', 'State_of_Issue__c', 'Status_Date__c', 'Target_Amount__c', 'X1035__c', 'BrokerDealer__c', 'Product_Line__c', 'Sandbox_EXTID__c', 'Product_TypeX__c', 'Premium_Type__c', 'Payment_Type__c', 'State_of_IssueFORMULA__c', 'SmartOffice_Modified_Date__c', 'SmartOffice_Created_Date__c', 'SmartOffice_Id_Number__c', 'CollateralAssignmentReceived__c', 'CollateralAssignmentInforce__c', 'CollateralAssignmenttoPolicyOwner__c', 'ReferralAdvisor__c', 'SO_ReferralAgent_txt__c', 'Face_Amount__c', 'CaseManager__c', 'DurationOfLoan__c', 'EstimatedLoanCloseDate__c', 'LenderRequiredCoverageForLoan__c', 'ApplicationEntered__c', 'PrimaryAdvisorMarketingMgr__c', 'Borrower_TrustSpot_Testimonial__c', 'Contact_Owner__c', 'Client_Email__c', 'Referral_Advisor_Email__c', 'Advisor_Office_Code__c', 'Client_First_Name__c', 'commannprem__c', 'Count__c', 'PolicyType__c', 'Death_Benefit_Formula__c']
    
    first_attributes = get_first_attributes(records)
    first_attributes.remove("attributes")
    # print("OKOKOKOKOKOKOK")
    # print(first_attributes)
    records = rest_query_all(f"select {','.join(first_attributes)} from Opportunity")
    
    # for record in records:
    #     print(json.dumps(record, indent=4))

    custom_names = {}
    for attr in attributes:
        custom_names[attr] = attr.replace('__', '_')
        
    obj_list = []
    
    for record in records:
        db_params = add_key(
            record,
            obj_params={},
            attributes=attributes,
            pref="",
            custom_names=custom_names,
            date_attributes={},
        )
        
        obj_list.append(Opportunity(**db_params))
    
    # try to use bulk_sync instead of bulk_create
    try:
        logger.info(f"Finished {len(obj_list)}")
        bulk_create(Opportunity, obj_list)
        # bulk_sync(Opportunity, obj_list=obj_list)
    except Exception as e:
        logger.warning(e)
        logger.warning("Bulk_create error ...")
        # logger.warning("Bulk_sync error ...")
        
    return JsonResponse({"status": status.HTTP_200_OK})

def generate_prospect(request):
    id_global = "0"
    real_attribute = set()
    list_attribute = []
    count = 0
    while (True):
        # count = count + 1
        # if count == 5:
        #     break
        access_token = get_access_token()
        recordss = query_pardot_2(query='prospect', id=id_global, access_token=access_token)
        if recordss[1] == id_global:
            break
        id_global = recordss[1]
        records = recordss[0]
        attributes = get_attributes(records)

        # Print models with cleaned attributes (e.g. "a__c" -> "a_c", "a_c_" -> "a_c")
        # Cleaned attributes with no "__" and last "_"
        for i in range(len(attributes)):
            attributes[i] = attributes[i].replace('__', '_')
            j = len(attributes[i]) - 1
            while j >= 0 and attributes[i][j] == '_':
                attributes[i] = attributes[i][:-1]
                j = j - 1
        for i in range(len(attributes)):
            attributes[i] = attributes[i].replace('__', '_')
            real_attribute.add(attributes[i])

    for ch in real_attribute:
        list_attribute.append(ch)

    print(generate_long_model_code(
        model_name='Prospect',
        table_name='pardot_prospects',
        attributes=list_attribute,
        primary_key='id',
        parent_class='TimeStamp',
    ))

    return JsonResponse({"status": status.HTTP_200_OK})


def save_prospect_2(request):
    global_id = "55595852"
    # Code used to get raw attributes and models with cleaned attributes
    while (True):
        access_token = get_access_token()
        recordss = query_pardot_2(query='prospect', id=global_id, access_token=access_token)
        if recordss[1] == global_id:
            break
        global_id = recordss[1]
        records = recordss[0]
        attributes = get_attributes(records)

        # Print models with cleaned attributes (e.g. "a__c" -> "a_c", "a_c_" -> "a_c")
        # Cleaned attributes with no "__" and last "_"
        for i in range(len(attributes)):
            attributes[i] = attributes[i].replace('__', '_')
            j = len(attributes[i]) - 1
            while j >= 0 and attributes[i][j] == '_':
                attributes[i] = attributes[i][:-1]
                j = j - 1
        for i in range(len(attributes)):
            attributes[i] = attributes[i].replace('__', '_')
        
        # print(attributes)

        # print(generate_long_model_code(
        #     model_name='Prospect',
        #     table_name='pardot_prospects',
        #     attributes=attributes,
        #     primary_key='id',
        #     parent_class='TimeStamp',
        # ))

        custom_names = {}
        for attr in attributes:
            custom_names[attr] = attr.replace('__', '_')
            
        obj_list = []
        
        # logger.info(records)

        for record in records:
            db_params = add_key(
                record,
                obj_params={},
                attributes=attributes,
                pref="",
                custom_names=custom_names,
                date_attributes={},
            )
            
            obj_list.append(Prospect(**db_params))

        # json.dumps(obj_list, indent=4)
        
        # try to use bulk_sync instead of bulk_create
        try:
            logger.info(f"Finished {len(obj_list)}")
            bulk_create(Prospect, obj_list)
            # bulk_sync(Prospect, obj_list=obj_list)
        except Exception as e:
            logger.warning(e)
            logger.warning("Bulk_create error ...")
            # logger.warning("Bulk_sync error ...")

    return JsonResponse({"status": status.HTTP_200_OK})


def save_prospect(request):
    # Code used to get raw attributes and models with cleaned attributes
    access_token = get_access_token()
    records = query_pardot(query='prospect', id="0", access_token=access_token)
    attributes = get_attributes(records)

    # Print models with cleaned attributes (e.g. "a__c" -> "a_c", "a_c_" -> "a_c")
    # Cleaned attributes with no "__" and last "_"
    for i in range(len(attributes)):
        attributes[i] = attributes[i].replace('__', '_')
        j = len(attributes[i]) - 1
        while j >= 0 and attributes[i][j] == '_':
            attributes[i] = attributes[i][:-1]
            j = j - 1
    for i in range(len(attributes)):
        attributes[i] = attributes[i].replace('__', '_')
    
    # print(attributes)

    print(generate_long_model_code(
        model_name='Prospect',
        table_name='pardot_prospects',
        attributes=attributes,
        primary_key='id',
        parent_class='TimeStamp',
    ))

    # custom_names = {}
    # for attr in attributes:
    #     custom_names[attr] = attr.replace('__', '_')
        
    # obj_list = []
    
    # for record in records:
    #     db_params = add_key(
    #         record,
    #         obj_params={},
    #         attributes=attributes,
    #         pref="",
    #         custom_names=custom_names,
    #         date_attributes={},
    #     )
        
    #     obj_list.append(Prospect(**db_params))

    # # json.dumps(obj_list, indent=4)
    
    # # try to use bulk_sync instead of bulk_create
    # try:
    #     logger.info(f"Finished {len(obj_list)}")
    #     bulk_create(Prospect, obj_list)
    #     # bulk_sync(Prospect, obj_list=obj_list)
    # except Exception as e:
    #     logger.warning(e)
    #     logger.warning("Bulk_create error ...")
    #     # logger.warning("Bulk_sync error ...")
    return JsonResponse({"status": status.HTTP_200_OK})

def save_opportunities_pardot(request):
    # Code used to get raw attributes and models with cleaned attributes
    access_token = get_access_token()
    records = query_pardot(query='opportunity', id="0", access_token=access_token)
    attributes = get_attributes(records)

    # Print models with cleaned attributes (e.g. "a__c" -> "a_c", "a_c_" -> "a_c")
    # Cleaned attributes with no "__" and last "_"
    for i in range(len(attributes)):
        attributes[i] = attributes[i].replace('__', '_')
        j = len(attributes[i]) - 1
        while j >= 0 and attributes[i][j] == '_':
            attributes[i] = attributes[i][:-1]
            j = j - 1
    for i in range(len(attributes)):
        attributes[i] = attributes[i].replace('__', '_')

    # print(generate_long_model_code(
    #     model_name='Opportunity',
    #     table_name='pardot_opportunities',
    #     attributes=attributes,
    #     primary_key='id',
    #     parent_class='TimeStamp',
    # ))

    custom_names = {}
    for attr in attributes:
        custom_names[attr] = attr.replace('__', '_')
        
    obj_list = []
    
    for record in records:
        db_params = add_key(
            record,
            obj_params={},
            attributes=attributes,
            pref="",
            custom_names=custom_names,
            date_attributes={},
        )
        
        obj_list.append(Opportunity(**db_params))

    # json.dumps(obj_list, indent=4)
    
    # try to use bulk_sync instead of bulk_create
    try:
        logger.info(f"Finished {len(obj_list)}")
        bulk_create(Opportunity, obj_list)
        # bulk_sync(Opportunity, obj_list=obj_list)
    except Exception as e:
        logger.warning(e)
        logger.warning("Bulk_create error ...")
        # logger.warning("Bulk_sync error ...")

    return JsonResponse({"status": status.HTTP_200_OK})


def export_csv(request = None):
    # response = HttpResponse(content_type="csv")
    # response['Content-Disposition'] = 'attachment; filename=Data' + \
    #     str(datetime.date.today()) + '.csv'
    # writer = csv.writer(response)
    # print(request)
    
    with open('./csv' + str(datetime.date.today()) + '.csv', 'w', newline = '', encoding='UTF8') as f:
        writer = csv.writer(f)
        writer.writerow(['Table name', 'Field name', 'Sample data', 'Field type'])
        app_models = apps.get_app_config('api').get_models()
        for model in app_models:
            try:
                samples = model.objects.all().values()[:30]
                for field in model._meta.get_fields():
                    cnt = 0
                    sample_list = []
                    for sample in samples:
                        if not sample[field.name] in sample_list:
                            cnt += 1
                            sample_list.append(sample[field.name])
                        if cnt == 5:
                            break
                    writer.writerow([model._meta.db_table, field.name, sample_list, "string"])
            except ProgrammingError:
                Exception("Table", model._meta.db_table, "does not exists")
                continue
            except:
                Exception("Error")


TABLE_NAME_TO_HUBSPOT_OBJECT_MAP = {
    "salesforce_contacts": "contacts",
    "salesforce_accounts": "companies",
    "salesforce_opportunities": "deals",
    "pardot_prospects": "contacts",
    "pardot_opportunities": "deals",
    "smartoffice_contacts": "contacts",
}

def export_xlsx(request):
    workbook = xlsxwriter.Workbook(f'Data.xlsx')
    app_models = apps.get_app_config('api').get_models()
    for model in app_models:
        if request is not None and model._meta.db_table not in request:
            continue
        print(model._meta.db_table)
        try:
            worksheet = workbook.add_worksheet(model._meta.db_table[:31])
            samples = model.objects.all().values()
            model_fields = model._meta.get_fields()
            
            cell_format = workbook.add_format()
            cell_format.set_pattern(1)
            cell_format.set_bg_color('yellow')
            
            worksheet.set_column(first_col=0, last_col=4, width=35)
            worksheet.write_row(
                row=0,
                col=0,
                data=['Field name', 'Record 1', 'Record 2', 'Record 3', 'Field type'],
                cell_format=cell_format
            )
            
            worksheet.write_column(row=1, col=0, data=[field.name for field in model_fields])
            worksheet.write_column(row=1, col=4, data=[field.get_internal_type() for field in model_fields])
            
            sample_cnt_dict = {}
            
            for i, sample in enumerate(samples):
                cnt = sum([(sample[field.name] is not None and str(sample[field.name]) != '') for field in model_fields])
                sample_cnt_dict[i] = cnt
                
            sample_cnt_dict = sorted(sample_cnt_dict.items(), key=operator.itemgetter(1), reverse=True)[:3]
            
            for i, sample in enumerate(sample_cnt_dict):
                worksheet.write_column(
                    row=1,
                    col=i + 1,
                    data=[str(samples[sample[0]][field.name]) if samples[sample[0]][field.name] != None else '' for field in model_fields]
                )
        except ProgrammingError:
            Exception("Table", model._meta.db_table, "does not exists")
            worksheet.write(0, 0, "Table", model._meta.db_table, "does not exists")
            # traceback.print_exc()
            print("does not exists", end='\n')
            continue
        except:
            print("Error something")
            traceback.print_exc()
            break
    workbook.close()

def save_contact_email(request: HttpRequest) -> HttpResponse:
    """Save SmartOffice Contact entity.

    Args:
        request (HttpRequest): nothing.

    Returns:
        HttpResponse: [description]
    """
    records = smarthome.get_records('WebAddress')

    attributes = [field.name for field in WebAddress._meta.get_fields() if '__' not in field.name]
    logger.info(f"{attributes=}")
    obj_list = []
    for record in records:
        db_params = add_key(
            record,
            obj_params={},
            attributes=attributes,
            pref='',
            custom_names={},
            date_attributes=[],
        )
        db_params['id'] = record['@id']
        obj_list.append(WebAddress(**db_params))
        
    bulk_sync(WebAddress, obj_list=obj_list)
    return JsonResponse({"status": status.HTTP_200_OK})

def smartoffice_pendingcases_detail(request: HttpRequest) -> HttpResponse:
    """Save SmartOffice Contact entity.

    Args:
        request (HttpRequest): nothing.

    Returns:
        HttpResponse: [description]
    """
    records = smarthome.get_records('NewBusiness')

    attributes = [field.name for field in NewBusiness._meta.get_fields() if '__' not in field.name]
    logger.info(f"{attributes=}")
    obj_list = []
    for record in records:
        db_params = add_key(
            record,
            obj_params={},
            attributes=attributes,
            pref='',
            custom_names={},
            date_attributes=[],
        )
        db_params['id'] = record['@id']
        obj_list.append(NewBusiness(**db_params))
        
    bulk_sync(NewBusiness, obj_list=obj_list)
    return JsonResponse({"status": status.HTTP_200_OK})

def smartoffice_pendingcases_requirements_delivery(request: HttpRequest) -> HttpResponse:
    """Save SmartOffice Contact entity.

    Args:
        request (HttpRequest): nothing.

    Returns:
        HttpResponse: [description]
    """
    records = smarthome.get_records('Requirement')

    attributes = [field.name for field in Requirement._meta.get_fields() if '__' not in field.name]
    logger.info(f"{attributes=}")
    obj_list = []
    for record in records:
        db_params = add_key(
            record,
            obj_params={},
            attributes=attributes,
            pref='',
            custom_names={},
            date_attributes=[],
        )
        db_params['id'] = record['@id']
        obj_list.append(Requirement(**db_params))
        
    bulk_sync(Requirement, obj_list=obj_list)
    return JsonResponse({"status": status.HTTP_200_OK})