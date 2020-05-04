import configparser
import requests
import urlparse
import pandas as pd
from datetime import datetime
from collections import Iterable
import time


def parse_qr_code(qr_code_string):
    qr_code_parsed = urlparse.parse_qs(qr_code_string)
    dtm_str = qr_code_parsed["t"][0]
    if len(dtm_str) == 13:
        dtm = datetime.strptime(dtm_str, "%Y%m%dT%H%M")
    elif len(dtm_str) == 15:
        dtm = datetime.strptime(dtm_str, "%Y%m%dT%H%M%S")
    else:
        print('Unknown dtm format: ' + dtm_str)
        exit(-1)
    return {'fpd': qr_code_parsed["fp"][0],
            'fn': qr_code_parsed["fn"][0],
            'fd': qr_code_parsed["i"][0],
            'n': qr_code_parsed["n"][0],
            'sum': (qr_code_parsed["s"][0]).replace(".", ""),
            'dtm': dtm
            }


def parse_receipt(qr_code, config):
    dtm_str = datetime.strftime(qr_code['dtm'], "%Y-%m-%dT%H:%M:00")

    headers = {'Device-Id' : '', 'Device-OS' : ''}
    payload = {'fiscalSign': qr_code['fpd'], 'date': dtm_str, 'sum' : qr_code['sum']}
    check = requests.get(
        'https://proverkacheka.nalog.ru:9999/v1/ofds/*/inns/*/fss/'+qr_code['fn']+'/operations/1/tickets/'+qr_code['fd'],
        params=payload, headers=headers, auth=(config["FNS"]["phone_number"], config["FNS"]["password"]))

    if check.status_code != 204:
        print("Receipt was not found [%d]: FN = %s, FD = %s, FPD = %s, date = %s, sum = %s"
              % (check.status_code, qr_code['fn'], qr_code['fd'], qr_code['fpd'], dtm_str, qr_code['sum']))
        return check.status_code

    receipt_details = requests.get(
        'https://proverkacheka.nalog.ru:9999/v1/inns/*/kkts/*/fss/'+qr_code['fn']+'/tickets/'+qr_code['fd']+'?fiscalSign='+qr_code['fpd']+'&sendToEmail=no',
         headers=headers, auth=(config["FNS"]["phone_number"], config["FNS"]["password"]))

    print("Receipt load completed [%d]: FN = %s, FD = %s, FPD = %s, date = %s, sum = %s" % (
        receipt_details.status_code, qr_code['fn'], qr_code['fd'], qr_code['fpd'], dtm_str, qr_code['sum']))
    if receipt_details.status_code != 200:
        return receipt_details.status_code

    products = receipt_details.json()
    new_items = pd.DataFrame(products['document']['receipt']['items'])
    new_items['price'] = new_items['price'] // 100
    new_items['sum'] = new_items['sum'] // 100
    new_items['date'] = datetime.strftime(qr_code['dtm'], "%d.%m.%Y %H:%M").replace(" 0", " ")
    new_items['receipt_sum'] = int(qr_code['sum']) // 100
    new_items.set_index(['date', 'receipt_sum'], inplace=True)
    return new_items


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read("settings.ini")
    new_items = list()

    qr_codes = pd.read_csv('qr_codes.csv', encoding='utf-8')

    existing_items = pd.read_csv('receipt_items.csv', encoding='utf-8')
    existing_items.set_index(['date', 'receipt_sum'], inplace=True)
    existing_items.sort_index(inplace=True)

    for qr_code in qr_codes['code']:
        qr_code_parsed = parse_qr_code(qr_code)
        dtm = datetime.strftime(qr_code_parsed['dtm'], "%d.%m.%Y %H:%M").replace(" 0", " ")
        sum = int(qr_code_parsed['sum']) // 100
        if not (dtm, sum) in existing_items.index:
            parsed_items = parse_receipt(qr_code_parsed, config)
            if not isinstance(parsed_items, Iterable):
                while not isinstance(parsed_items, Iterable) and parsed_items == 202:
                    time.sleep(float(config["FNS"]["api_call_delay_in_seconds"]))
                    parsed_items = parse_receipt(qr_code_parsed, config)
            if isinstance(parsed_items, Iterable):
                new_items.append(parsed_items)
            time.sleep(float(config["FNS"]["api_call_delay_in_seconds"]))

    new_items.append(existing_items)
    existing_items = pd.concat(new_items, join='outer', sort=False)
    existing_items.drop_duplicates(inplace=True)
    existing_items.to_csv('receipt_items.csv', header=True, encoding='utf-8', float_format='%.3f')