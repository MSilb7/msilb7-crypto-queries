import pandas as pd
import asyncio, aiohttp, nest_asyncio
from aiohttp_retry import RetryClient, ExponentialRetry
import requests as r
import numpy as np
nest_asyncio.apply()

header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0'}
statuses = {x for x in range(100, 600)}
statuses.remove(200)
statuses.remove(429)

async def get_tvl(apistring, header, statuses, chains, prot):
        prod = []
        retry_client = RetryClient()

        async with retry_client.get(apistring, retry_options=ExponentialRetry(attempts=10), raise_for_status=statuses) as response:
                try:
                        prot_req = await response.json()
                        cats = prot_req['category']
                        prot_req = prot_req['chainTvls']
                        for ch in chains:
                                ad = pd.json_normalize( prot_req[ch]['tokens'] )
                                ad_usd = pd.json_normalize( prot_req[ch]['tokensInUsd'] )
                                try: #if there's generic tvl
                                        ad_tvl = pd.json_normalize( prot_req[ch]['tvl'] )
                                        ad_tvl = ad_tvl[['date','totalLiquidityUSD']]
                                        ad_tvl = ad_tvl.rename(columns={'totalLiquidityUSD':'total_app_tvl'})
                                        # print(ad_tvl)
                                except:
                                        continue
                        #             ad = ad.merge(how='left')
                                if not ad.empty:
                                        ad = pd.melt(ad,id_vars = ['date'])
                                        ad = ad.rename(columns={'variable':'token','value':'token_value'})
                                        if not ad_usd.empty:
                                                ad_usd = pd.melt(ad_usd,id_vars = ['date'])
                                                ad_usd = ad_usd.rename(columns={'variable':'token','value':'usd_value'})
                                                ad = ad.merge(ad_usd,on=['date','token'])
                                        else:
                                                ad['usd_value'] = ''
                                        if not ad_tvl.empty:
                                                ad = ad.merge(ad_tvl,on=['date'],how = 'outer')
                                        else:
                                                ad['total_app_tvl'] = ''
                                        
                                        ad['date'] = pd.to_datetime(ad['date'], unit ='s') #convert to days
                                        try:
                                                ad['token'] = ad['token'].str.replace('tokens.','', regex=False)
                                        except:
                                                continue
                                        ad['protocol'] = prot
                                        ad['chain'] = ch
                                        ad['category'] = cats
                                #         ad['start_date'] = pd.to_datetime(prot[1])
                                        # ad['date'] = ad['date'] - timedelta(days=1) #change to eod vs sod
                                        prod.append(ad)
                                        # print(ad)
                except Exception as e:
                        raise Exception("Could not convert json")
        await retry_client.close()
        # print(prod)
        return prod

def get_range(protocols, chains = '', header = header, statuses = statuses):
        data_dfs = []
        fee_df = []
        if isinstance(chains, list):
                og_chains = chains #get starting value
        elif chains == '':
                og_chains = chains
        else:
                og_chains = [chains] #make it a list
        # for dt in date_range:
        #         await asyncio.gather()
        #         data_dfs.append(res_df)
        #         # res.columns
        # try:
        #         loop.close()
        # except:
        #         #nothing
        loop = asyncio.get_event_loop()
        #get by app
        api_str = 'https://api.llama.fi/protocol/'
        # print(protocols)
        prod = []
        tasks = []
        for index,proto in protocols.iterrows():
                prot = proto['slug']
                try:
                        if og_chains == '':
                                chains = proto['chainTvls']
                        else:
                                chains = og_chains
                except:
                        chains = og_chains
                apic = api_str + prot
                # print(chains)
                #     time.sleep(0.1)
                tasks.append( get_tvl(apic, header, statuses, chains, prot) )
        # print(tasks)
        data_dfs = loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
        # print(date_range)
        # loop.close()
        # print(data_dfs)
        # fee_df = pd.concat(data_dfs)
        # return fee_df
        df_list = []
        for dat in data_dfs:
                if isinstance(dat,list):
                        # print(dat)
                        for pt in dat: #each list within the list (i.e. multiple chains)
                                try:
                                        tempdf = pd.DataFrame(pt)
                                        if not tempdf.empty:
                                                # print(tempdf)
                                                df_list.append(tempdf)
                                except:
                                        continue
        df_df_all = pd.concat(df_list)
        df_df_all = df_df_all.fillna(0)
        
        return df_df_all

def remove_bad_cats(netdf):
        summary_df = netdf[\
                        (~netdf['chain'].str.contains('-borrowed')) &\
                        (~netdf['chain'].str.contains('-staking')) &\
                        (~netdf['chain'].str.contains('-pool2')) &\
                        (~netdf['chain'].str.contains('-treasury')) &\
                        (~netdf['chain'].str.contains('-vesting')) &\
                        (~netdf['chain'].str.contains('-Vesting')) &\
                        (~( netdf['chain'] == 'treasury') ) &\
                        (~( netdf['chain'] == 'borrowed') ) &\
                        (~( netdf['chain'] == 'staking') ) &\
                        (~( netdf['chain'] == 'vesting') ) &\
                        (~( netdf['chain'] == 'Vesting') ) &\
                        (~( netdf['chain'] == 'pool2') )       
#                         & (~( netdf_df['chain'] == 'Ethereum') )
                        ]
        return summary_df

def get_chain_tvls(chain_list):
        # get chain tvls
        chain_api = 'https://api.llama.fi/charts/'
        cl = []
        for ch in chain_list:
                try:
                        capi = chain_api + ch
                        cres = pd.DataFrame( r.get(capi, headers=header).json() )
                        cres['chain'] = ch
                        cres['date'] = pd.to_datetime(cres['date'], unit ='s') #convert to days
                        cl.append(cres)
                except:
                        continue
        chains = pd.concat(cl)
        return chains

def get_single_tvl(api_base, prot, chains, header = header, statuses = statuses):
        prod = []
        # retry_client = RetryClient()
        apistring = api_base + prot

        # response = retry_client.get(apistring, retry_options=ExponentialRetry(attempts=10), raise_for_status=statuses)
        try:
                prot_req = r.get(apistring).json()
                cats = prot_req['category']
                prot_req = prot_req['chainTvls']
                for ch in chains:
                        ad = pd.json_normalize( prot_req[ch]['tokens'] )
                        ad_usd = pd.json_normalize( prot_req[ch]['tokensInUsd'] )
                        try: #if there's generic tvl
                                ad_tvl = pd.json_normalize( prot_req[ch]['tvl'] )
                                ad_tvl = ad_tvl[['date','totalLiquidityUSD']]
                                ad_tvl = ad_tvl.rename(columns={'totalLiquidityUSD':'total_app_tvl'})
                                # print(ad_tvl)
                        except:
                                continue
                #             ad = ad.merge(how='left')
                        if not ad.empty:
                                ad = pd.melt(ad,id_vars = ['date'])
                                ad = ad.rename(columns={'variable':'token','value':'token_value'})
                                if not ad_usd.empty:
                                        ad_usd = pd.melt(ad_usd,id_vars = ['date'])
                                        ad_usd = ad_usd.rename(columns={'variable':'token','value':'usd_value'})
                                        ad = ad.merge(ad_usd,on=['date','token'])
                                else:
                                        ad['usd_value'] = ''
                                if not ad_tvl.empty:
                                        ad = ad.merge(ad_tvl,on=['date'],how = 'outer')
                                else:
                                        ad['total_app_tvl'] = ''
                                
                                ad['date'] = pd.to_datetime(ad['date'], unit ='s') #convert to days
                                try:
                                        ad['token'] = ad['token'].str.replace('tokens.','', regex=False)
                                except:
                                        continue
                                ad['protocol'] = prot
                                ad['chain'] = ch
                                ad['category'] = cats
                        #         ad['start_date'] = pd.to_datetime(prot[1])
                                # ad['date'] = ad['date'] - timedelta(days=1) #change to eod vs sod
                                prod.append(ad)
                                # print(ad)
        except Exception as e:
                raise Exception("Could not convert json")
        # retry_client.close()
        # print(prod)
        p_df = pd.concat(prod)
        return p_df

def get_protocols_by_chain(chain_name, exclude_cex = True, exclude_chain = True):
        protos = 'https://api.llama.fi/protocols'

        category_excludes = []
        if exclude_cex == True:
                category_excludes.append('CEX')
        if exclude_chain == True:
                category_excludes.append('Chain')

        s = r.Session()
        #get all protocols
        resp = pd.DataFrame( s.get(protos).json() )[['category','slug','chainTvls']]
        # extract the chain names
        resp['chainTvls'] = resp['chainTvls'].apply(lambda x: list(x.keys()) )
        # set a true/false if the array contains the chain we want
        resp['contains_chain'] = resp['chainTvls'].apply(lambda x: chain_name in x)
        # set a true/false if the array doesn't contains the categories we want to exclude
        resp['contains_cats'] = resp['category'].apply(lambda x: x not in category_excludes)
        # filter where we have a match on chain
        proto_list = resp[resp['contains_chain'] == True]
        # filter where we have a match on cats
        proto_list = proto_list[proto_list['contains_cats'] == True]
        # clean up
        proto_list = proto_list[['slug']]
        proto_list = proto_list.reset_index(drop=True)
        # boom
        return proto_list

def get_protocol_names_by_flag(check_flag):
        flag_str = '-' + check_flag
        protocols = r.get('https://api.llama.fi/lite/protocols2', headers=header).json()['protocols']
        protocols = [protocol for protocol in protocols if any(flag_str in key for key in protocol["chainTvls"])]
        protocol_names = [element["name"] for element in protocols]
        return protocol_names


def get_protocol_tvls(min_tvl = 0, excluded_cats = ['CEX','Chain']): #,excluded_flags = ['staking','pool2']):
        all_api = 'https://api.llama.fi/protocols'
        resp = pd.DataFrame( r.get(all_api, headers=header).json() )
        resp = resp[resp['tvl'] > min_tvl ] ##greater than X
        if excluded_cats != []: #If we have cagtegories to exclude
                resp = resp[~resp['category'].isin(excluded_cats)]
        # Get Other Flags
        doublecounts = get_protocol_names_by_flag('doublecounted')
        liqstakes = get_protocol_names_by_flag('liquidstaking')
        resp = resp.assign(is_doubelcount = resp['name'].isin(doublecounts))
        resp = resp.assign(is_liqstake = resp['name'].isin(liqstakes))
        # if excluded_flags != []: #If we have cagtegories to exclude
        #         for flg in excluded_flags:
        #                 resp = resp[resp[flg] != True]
        return resp

def get_all_protocol_tvls_by_chain_and_token(min_tvl = 0, excluded_cats = ['CEX','Chain']):
        res = get_protocol_tvls(min_tvl)
        protocols = res[['slug','chainTvls']]
        # re = res['chainTvls']
        protocols['chainTvls'] = protocols['chainTvls'].apply(lambda x: list(x.keys()) )
        df_df = get_range(protocols)

        proto_info = res['name','is_doubelcount','is_liqstake']
        df_df = df_df.merge(proto_info,on='name',how='left')

        return df_df
