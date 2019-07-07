import requests
import json
import csv
import re
import time
# import pandas

headers = {'content-type': 'application/json',
           'authorization': 'Basic YWNvbmNhZ3VhcG9rZXJfcmVwb3J0aW5nOmFjb25jYWd1YXBva2VyX3JlcG9ydGluZ19wYXNz'}


def post_start(url, headers_, data_):
    full_url = str(url + '/integration/rest/dgoj/report/start')
    request_start = requests.post(url=full_url, headers=headers_, json=data_)
    json_data = json.loads(request_start.content)
    status = json_data.get('status')
    return status


def post_status(url, headers_, data_):
    full_url = str(url + '/integration/rest/dgoj/report/status')
    request_status = requests.post(url=full_url, headers=headers_, json=data_)
    json_data = json.loads(request_status.content)
    report_status = json_data.get('reportStatus')
    access_key = report_status.get('accessKey')
    if access_key is not None:
        access_key_str = str('{"accessKey":"' + access_key + '"}')
        json_ak = json.loads(access_key_str)
        return json_ak
    else:
        return ""


def post_data(url, headers_, key_):
    full_url = str(url + '/integration/rest/dgoj/report/data-stream')
    raw_data = requests.post(url=full_url, headers=headers_, json=key_)
    request_data = raw_data.content
    return request_data


with open('data.json') as json_file:
    data = json.load(json_file)
base_url = data['base_url']
data = data['data']
start = post_start(base_url, headers, data)
key = post_status(base_url, headers, data)
if key == "":
    for i in range(1, 15):
        time.sleep(1)
        key = post_status(base_url, headers, data)
        if key != "":
            break

s = post_data(base_url, headers, key)
data_s = s.decode('utf-8').splitlines()
x = str(data['reportingPeriod']['year']) + '-' + str(data['reportingPeriod']['month']) + '-' + str(data['reportingPeriod']['day'])
with open("tmp.csv", "a", newline='') as csv_file:
    writer = csv.writer(csv_file, delimiter=' ', doublequote=False, escapechar=' ')
    writer.writerow(x)
    for line in data_s:
        writer.writerow(re.split("\s+", line))



