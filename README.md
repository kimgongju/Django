## SmartOffice
https://sidevkit.ez-data.com/

https://www.ez-data.com/login.shtml?FRONTEND=1&login=display

Office: AdvisorsChoiceInsSvc

Username: bkoplan

Password: Insurance01

https://sidevkit.ez-data.com/Main/SearchOperation#Pagination

## Salesforce
https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/query_walkthrough.htm
https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_queryall.htm


To get all attributes of an entity: use SOQL to execute the query
```
SELECT FIELDS(ALL) FROM Account LIMIT 1
```

To get SF access_token: python manage.py getaccesstoken

Example to generate model (make sure to remove "attributes_type", "attributes_url"):
```
records = sf_utils.rest_query_all("select fields(all) from Account limit 200")
attributes = utilities.get_attributes(records)
print(attributes) // Use this for attributes array 
attributes = [attr.replace('__', '_') for attr in attributes]
print(utilities.generate_model_code(
    model_name='Account',
    table_name='accounts',
    attributes=attributes,
    primary_key='Id',
    parent_class='TimeStamp',
))
```
