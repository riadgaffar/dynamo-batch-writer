import json
import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
sample_data_file = ROOT_DIR + '/sample/data.json'

def get_json_data():
    data = None
    with open(sample_data_file, 'r') as f:
        data = json.load(f)
    
    return data