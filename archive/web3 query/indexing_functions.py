#!/usr/bin/env python
# coding: utf-8

# In[4]:


import requests
import pandas as pd
from datetime import datetime, timedelta
from configparser import ConfigParser

config = ConfigParser()
config.read('../config.ini')
etherscan_api = str(config.get('ETHERSCAN','etherscan_api'))
#header for parsing
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}


# In[5]:


# get latest block number #Etherscan
def get_latest_block(chain_name):
    block_num = ''
    api_str = str(config.get('ETHERSCAN',chain_name)) + 'api'        '?module=proxy&action=eth_blockNumber&apikey=' + etherscan_api
    block_num = requests.get(api_str,headers=headers).json()['result']
    return block_num

def get_latest_block_info(chain_name):
    api_str = str(config.get('ETHERSCAN',chain_name)) + 'api'        '?module=proxy&action=eth_getBlockByNumber&tag=' + get_latest_block(chain_name)         + '&boolean=true&apikey=' + etherscan_api
    
    block_info = requests.get(api_str,headers=headers).json()
    return block_info

# get block from trailing time # Etherscan
def get_block_by_timestamp(input_time, chain_name):
    block_num = ''
    api_str = str(config.get('ETHERSCAN',chain_name)) + 'api' +        '?module=block&action=getblocknobytime&timestamp=' + str(input_time) +        '&closest=before&apikey=' + etherscan_api
    block_num = requests.get(api_str,headers=headers).json()['result']
    #print(chain_name + ': ' + str(block_num))
    return block_num
    

# get contract transactions, given a start and end block
def get_address_transactions_by_block_range(start_block, end_block, address, chain_name):
        transactions = ''
        api_str = str(config.get('ETHERSCAN',chain_name)) + 'api'             '?module=account&action=txlist&address=' + str(address) + '&startblock='             + str(start_block) + '&endblock=' + str(end_block) +'&sort=desc&apikey=' + str(etherscan_api)
        result = requests.get(api_str,headers=headers).json()['result']
        return result

