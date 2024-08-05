import gzip
import pandas as pd
import json
import os

def un_gz(file_name, outputformat='json'):
    f_name = file_name.replace(".gz", "")
    g_file = gzip.GzipFile(file_name)
    open(f_name+'.'+outputformat, "wb+").write(g_file.read())
    g_file.close()
    os.remove(file_name)

def read_json_to_pandasFrame(file_path):
    data = []
    with open(file_path, 'r') as file:
        for line in file:
            data.append(json.loads(line.strip()))
    return pd.json_normalize(data)
