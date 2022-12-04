import requests as r
import pandas as pd

def api_json_to_df(api_url):
    inf = pd.DataFrame( r.get(api_url).json() )
    return inf