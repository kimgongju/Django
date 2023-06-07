import sys
import logging
import traceback
from django.utils import timezone
from django.db import models, transaction
from datetime import datetime
from typing import List, OrderedDict
from string import punctuation
import keyword
import re

# Utilities
def get_logger(name: str, level: str) -> logging.Logger:
    logger = logging.getLogger(name)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '%(filename)s:%(lineno)s-%(funcName)20s-%(levelname)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)
    return logger

logger = get_logger('api-utilities', 'DEBUG')


def timestamp_to_date(ts):
    if len(str(ts)) > 0:
        return str(datetime.fromtimestamp(int(ts) / 1e3))
    else:
        return None


def to_snake(s: str) -> str:
    ans = ''
    for (i, c) in enumerate(s):
        if c.isupper():
            if i != 0:
                ans += '_'
        ans += c.lower()
    return ans


def to_plural(name: str) -> str:
    if name[-1] != 'y':
        name += 's'
    else:
        name = name[:-1] + 'ies'
    return name

def generate_model_code(model_name: str, table_name: str, attributes: list, primary_key: str, parent_class: str) -> str:
    # Fields
    attribute_code = f'    {primary_key} = models.CharField(max_length=50, blank=True, default="", primary_key=True)\n'
    for attribute in attributes:
        if attribute != primary_key:
            attribute_code += f'    {attribute} = models.TextField(blank=True, default="", null=True)\n'
            
    model_code = f"""    

class {model_name}({parent_class}):
{attribute_code}
    def save(self, **kwargs):
        super({model_name}, self).save()

    class Meta:
        db_table ='{table_name}'
    """
    
    return model_code


# this function is used to generate model code for long models (Use TextField instead of CharField)
def generate_long_model_code(model_name: str, table_name: str, attributes: list, primary_key: str, parent_class: str) -> str:
    # Fields
    attribute_code = f'    {primary_key} = models.CharField(max_length=50, blank=True, default="", primary_key=True)\n'
    for attribute in attributes:
        if attribute != primary_key:
            attribute_code += f'    {attribute} = models.TextField(blank=True, default="", null=True)\n'
            
    model_code = f"""

class {model_name}({parent_class}):
{attribute_code}
    def save(self, **kwargs):
        super({model_name}, self).save()

    class Meta:
        db_table ='{table_name}'
    """
    
    return model_code


def get_attributes(obj_list: list) -> list:
    db_params = {}

    try:
        for obj in obj_list:
            db_params = add_key(
                obj,
                obj_params=db_params,
                attributes=[],
                pref='',
                custom_names={},
                date_attributes=[],
            )
    except Exception as e:
        logger.warning("Error when adding keys")
        logger.warning(e)
        logger.warning(traceback.format_exc())

    return list(db_params.keys())

def get_first_attributes(obj_list: list) -> list:
    db_params = {}

    try:
        for obj in obj_list:
            db_params = add_first_key(
                obj,
                obj_params=db_params,
                attributes=[],
                pref='',
                custom_names={},
                date_attributes=[],
            )
    except Exception as e:
        logger.warning("Error when adding keys")
        logger.warning(e)
        logger.warning(traceback.format_exc())

    return list(db_params.keys())

def add_first_key(
    obj: dict, obj_params: dict, attributes: list, pref: str,
    custom_names: dict, date_attributes: list
):
    for key in obj:
        name = f'{pref}{"_" if len(pref) else ""}{key}'

        # Skip lists or potential list of relationships
        if isinstance(obj[key], list):
            continue

        if name in date_attributes:
            obj[key] = timestamp_to_date(obj[key])

        # We'll return an object with all possible attributes if there is none specified.
        # Otherwise do it normally.
        if len(attributes) == 0:
            obj_params[name] = ''
        elif name in attributes:
            if name in custom_names:
                obj_params[custom_names[name]] = obj[key]
            else:
                obj_params[name] = obj[key]

    return obj_params    

def add_key(
    obj: dict, obj_params: dict, attributes: list, pref: str,
    custom_names: dict, date_attributes: list
):
    for key in obj:
        name = f'{pref}{"_" if len(pref) else ""}{key}'
    
        # Skip lists or potential list of relationships
        if isinstance(obj[key], list):
            continue

        # Recursive on objects
        if isinstance(obj[key], dict) or isinstance(obj[key], OrderedDict):
            obj_params = add_key(
                obj=obj[key],
                obj_params=obj_params,
                attributes=attributes,
                custom_names=custom_names,
                pref=name,
                date_attributes=date_attributes,
            )
            continue

        if name in date_attributes:
            obj[key] = timestamp_to_date(obj[key])

        # We'll return an object with all possible attributes if there is none specified.
        # Otherwise do it normally.
        if len(attributes) == 0:
            obj_params[name] = ''
        elif name in attributes:
            if name in custom_names:
                obj_params[custom_names[name]] = obj[key]
            else:
                obj_params[name] = obj[key]

    return obj_params


def bulk_sync(Model: models.Model, obj_list: List[models.Model], batch_size: int=5000) -> dict:
    logger.debug(f"Bulk_syncing {Model.__name__}")
    
    def get_obj_identifier(obj: models.Model) -> str:
        return '-'.join(
            [getattr(obj, field.name) for field in obj._meta.get_fields() if field.primary_key])

    fields = [
        field.name for field in Model._meta.get_fields()
            if (not field.primary_key and field.editable)
                and field.name not in ['at_hubspot', 'at_quickbooks']
    ]
    
    # auto_add is not called when call bulk_update
    fields.append('updated_at')
    logger.info(f"{sorted(fields)=}")

    with transaction.atomic():
        new_objs = []
        existing_objs = []
        db_objs = Model.objects.all()
        db_obj_identifiers = [get_obj_identifier(obj) for obj in db_objs]
        
        new_objs = [obj for obj in obj_list if get_obj_identifier(obj) not in db_obj_identifiers]
        existing_objs = [obj for obj in obj_list if get_obj_identifier(obj) in db_obj_identifiers]
        for obj in existing_objs:
            obj.updated_at = timezone.now()
        
        created = Model.objects.bulk_create(new_objs, ignore_conflicts=True, batch_size=batch_size)
        updated = Model.objects.bulk_update(existing_objs, fields=fields, batch_size=batch_size)

    logger.info(f"Created {len(new_objs)=}, updated {len(existing_objs)=}")
    return {
        'created': created,
        'updated': updated,
    }

def clean_attribute_name(s: str) -> str:
    # Filter all punctuations except '_'
    s = ''.join([c for c in s if c not in punctuation.replace('_', '')])

    # No whitespace, no '__'
    s = re.sub(' +', '_', s)
    s = re.sub('_+', '_', s)

    # No ending '_'
    if s[-1] == '_':
        s = s[:-1]
    
    # Identifier can not be a number
    if sum([x.isnumeric() for x in s]) == len(s):
        s = '_' + s

    # Identifier can not start with number
    if s[0].isnumeric():
        s = '_' + s

    # Identifier can not be a keyword
    for kw in keyword.kwlist:
        if s == kw:
            s = '_' + s
    
    return s
