
import json
import requests
import xmltodict as xmltodict
from entsoe import EntsoeRawClient
import pandas as pd
from datetime import datetime, date
import calendar
import os

def get_xml():
    # 24*60=1440
    # 1440/96=15

    client = EntsoeRawClient(api_key='447518cc-aba5-4b0a-86c6-5aa8a7f3869f')
    params = {
        # 'documentType': 'A75',
        'documentType': 'A75',
        'processType': 'A16',
        'psrType': 'B14',
        'in_Domain': '10Y1001A1001A83F'
    }

    data_start = 2017
    now = datetime.now()
    current_year = datetime.now().year
    while data_start <= current_year:
        for month in range(1, 12):

            monthRange = calendar.monthrange(data_start, month)[1]
            if data_start == current_year :
                if month == int(now.strftime("%m")):
                    monthRange = now.day
                if month > int(now.strftime("%m")):
                    return

            current_month = F"0{month}" if month < 10  else month

            start_date = F'{data_start}{current_month}01'
            start = pd.Timestamp(start_date, tz='Europe/Berlin')

            tz = 'Europe/Berlin'

            month += 1
            current_month = F"0{month}" if month < 10  else month
            end_date = F'{data_start}{current_month}01'
            end = pd.Timestamp(end_date, tz='Europe/Berlin')
            print( F"{start_date} - {end_date} days = {monthRange}")
            response = client._base_request(params=params, start=start, end=end)
            write_to_json(response, F"{start_date} - {end_date}", data_start, monthRange, tz)
        data_start += 1;
    print('-----------')

def get_xml_year():
    # 24*60=1440
    # 1440/96=15

    client = EntsoeRawClient(api_key='447518cc-aba5-4b0a-86c6-5aa8a7f3869f')
    params = {
        # 'documentType': 'A75',
        'documentType': 'A75',
        'processType': 'A16',
        'psrType': 'B14',
        'in_Domain': '10Y1001A1001A83F'
    }

    data_start = 2017
    now = datetime.now()
    current_year = datetime.now().year
    while data_start <= current_year:

        start_date = F'{data_start}0101'
        start = pd.Timestamp(start_date, tz='Europe/Berlin')

        f_date = date(data_start, 1, 1)
        data_start += 1

        l_date = date(data_start, 1, 1)

        delta = l_date - f_date
        count_days = delta.days

        end_date = F'{data_start}0101'
        end = pd.Timestamp(end_date, tz='Europe/Berlin')

        tz = 'Europe/Berlin'

        print( F"{start_date} - {end_date} days = {count_days}")
        response = client._base_request(params=params, start=start, end=end)

        print(F"tz = {tz}")
        write_to_json(response, F"{start_date} - {end_date}", 'years/psrType', count_days, tz)
        # data_start += 1;
    print('-----------')

def get_xml_year_day_ahead():
    # 24*60=1440
    # 1440/96=15

    client = EntsoeRawClient(api_key='447518cc-aba5-4b0a-86c6-5aa8a7f3869f')
    params = {
        'documentType': 'A65',
        'processType': 'A01',
        'outBiddingZone_Domain': '10Y1001A1001A83F'
    }

    data_start = 2017
    now = datetime.now()
    current_year = datetime.now().year
    while data_start <= current_year:

        start_date = F'{data_start}0101'
        start = pd.Timestamp(start_date, tz='Europe/Berlin')

        f_date = date(data_start, 1, 1)
        data_start += 1

        l_date = date(data_start, 1, 1)

        delta = l_date - f_date
        count_days = delta.days

        end_date = F'{data_start}0101'
        end = pd.Timestamp(end_date, tz='Europe/Berlin')

        tz = 'Europe/Berlin'

        print( F"{start_date} - {end_date} days = {count_days}")
        response = client._base_request(params=params, start=start, end=end)

        print(F"tz = {tz}")
        write_to_json(response, F"{start_date} - {end_date}", 'years/day_ahead', count_days, tz)
        # data_start += 1;
    print('-----------')

def get_xml_year_day_ahead_price():
    # 24*60=1440
    # 1440/96=15

    client = EntsoeRawClient(api_key='447518cc-aba5-4b0a-86c6-5aa8a7f3869f')
    params = {
        'documentType': 'A44',
        'in_Domain': '10Y1001A1001A63L',
        'out_Domain': '10Y1001A1001A63L',
    }

    data_start = 2017
    now = datetime.now()
    current_year = datetime.now().year
    while data_start <= current_year:

        start_date = F'{data_start}0101'
        start = pd.Timestamp(start_date, tz='Europe/Berlin')

        f_date = date(data_start, 1, 1)
        data_start += 1

        l_date = date(data_start, 1, 1)

        delta = l_date - f_date
        count_days = delta.days

        end_date = F'{data_start}0101'
        end = pd.Timestamp(end_date, tz='Europe/Berlin')

        tz = 'Europe/Berlin'

        print( F"{start_date} - {end_date} days = {count_days}")

        try:
            response = client._base_request(params=params, start=start, end=end)
        except:
            continue
        print(F"tz = {tz}")
        write_to_json(response, F"{start_date} - {end_date}", 'years/day_ahead_price', count_days, tz)
        # data_start += 1;
    print('-----------')

def get_xml_nasdaq_resource():
    API_key = 'n_SyzGbL1wx-zwsywJTy'
    resource = {
        'ODA/PALUM_USD': 'Aluminum',
        'ODA/PCOPP_USD': 'Copper',
        'SHFE/RBV2013': 'Shanghai Steel Rebar Futures',
        'ODA/PNICK_USD': 'Nickel',
        'JOHNMATT/PLAT': 'Platinum, Johnson Mathey London',
        'JOHNMATT/PALL': 'Palladium, Johnson Mathey London',
    }

    path = F"./data/nasdaq"
    isdir = os.path.isdir(path)
    if not isdir:
        os.mkdir(path)
    for key, item in resource.items():
        res = requests.get(F"https://data.nasdaq.com/api/v3/datasets/{key}?api_key={API_key}")
        with open(F"data/nasdaq/{item}.json", "w") as json_file:
            json_file.write(res.text)
            json_file.close()
        print('-----------')


def write_to_json(response, file_name, save_path, count_days, tz):
    dict_data = xmltodict.parse(response.content)
    dict_data['count_days'] = count_days
    dict_data['tz'] = tz
    data_str = json.dumps(dict_data)

    path = F"./data/{save_path}"
    isdir = os.path.isdir(path)
    if not isdir:
        os.mkdir(path)
    with open(F"data/{save_path}/{file_name}.json", "w") as json_file:
        json_file.write(data_str)
        json_file.close()


if __name__ == '__main__':
    # get_xml()

    # get_xml_year()
    # get_xml_year_day_ahead()
    get_xml_year_day_ahead_price()
    get_xml_nasdaq_resource()