import requests as r
import pandas as pd
import sys
import os
tvl_dir = os.path.abspath('L2 TVL')
sys.path.append(tvl_dir)
# sys.path.append('../')
import defillama_utils as dfl
# sys.path.pop()
# header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0'}
# df = dfl.get_all_protocol_tvls_by_chain_and_token(100_000_000)

# print(df.head())

protocols = r.get('https://api.llama.fi/lite/protocols2', headers={'Content-Type': 'application/json'}).json()
print(protocols)
