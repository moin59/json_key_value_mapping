import numpy as np
import pandas as pd
import string
import glob
import re
import time
import json
import os
from pandas.io.json import json_normalize

# read json file and and convert as dictionary for furthe work
class read_file:
    data_dir = os.path.dirname(os.path.realpath(__file__))
    empty_dict = {}

    def f_import(self, path):
        js_file = glob.glob(path + "/*.json")
        for file_ in js_file:
            js_data = pd.read_json(file_, typ='series')
        self.empty_dict = js_data.to_dict()
        print('json file read successful.')

data = read_file()
data.f_import(read_file.data_dir)
new_dict = data.empty_dict

# remove and replace all the missing value, empty dictionay, empty list
######  worker0.1 #############
def dict_manager(items):
    result = {}
    for key, value in items:
        if value is None:
            value = 0
        elif value is '':
            value = 0
        elif type(value) is list and len(value) == 0:
            value = 0
        elif type(value) is dict and len(value) == 0:
            value = 0
        result[key] = value
    return result
print('Data management successful.')

dict_str = json.dumps(new_dict)
result_js = json.loads(dict_str, object_pairs_hook=dict_manager)

# looking for 'entry' to rename
def search(key, js):
    for k, v in js.iteritems():
        if k == key:
            yield v

        elif isinstance(v, dict):
            for result in search(key, v):
                yield result

        elif isinstance(v, list):
            for d in v:
                for result in search(key, d):
                    yield result
print('Find Entry Block.')

# searching for nested json "entry" 
find_result = list(search('entry', result_js))

####{'entry': [{'k1': 'V1', 'k2': 'v2'}]}####
####mapping to {'entry': [{'V1': 'v2'}]} ####
for item in find_result[0:]:
    for i in item[0:]:
        entry_name = i['name']
        i[entry_name] = i.pop('value')
        del i['name']
print('Rename Successful.')

# saving file
with open("saved_file/renamed.json", 'w') as rename:
    json.dump(result_js, rename, sort_keys=True)
    rename.close()
print("file saved, done!")
