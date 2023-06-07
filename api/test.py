import time
from datetime import datetime

def date_to_timestamp(date: str, date_format: str) -> str:
    try:
        date = date.replace('-', '/')[:len("yyyy/mm/dd")].rstrip()
        element = datetime.strptime(date, date_format)
        timestamp = datetime.timestamp(element)
        timestamp = str(int(int(str(timestamp).replace('.','')+'00') + 2.52e+7))
        return timestamp
    except ValueError:
        return None

str_temp = '2018-12-19 11:26:30'
print(date_to_timestamp(str_temp.replace('-', '/'), '%Y/%m/%d'))