import pandas as pd
import json

def get_element_from_json_column(col,element):
    col = col.apply(json.dumps)
    # extract the 'name' element from the JSON data
    return pd.json_normalize(col.apply(json.loads))[element]
