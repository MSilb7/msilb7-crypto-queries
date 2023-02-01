#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# ! jupyter nbconvert --to python optimism_subgraph_tvls.ipynb


# In[ ]:


from subgrounds.subgrounds import Subgrounds
from subgrounds.pagination import ShallowStrategy
import pandas as pd
import requests as r
import defillama_utils as dfl

sgs = pd.DataFrame(
        [
                 ['l2dao-velodrome','https://api.thegraph.com/subgraphs/name/messari/velodrome-optimism','']
                ,['synthetix-curve','https://api.thegraph.com/subgraphs/name/convex-community/volume-optimism','']
        ]
        ,columns = ['dfl_id','subgraph_url','query']
)
sg = Subgrounds()
# curve_op = sg.load_subgraph("https://api.thegraph.com/subgraphs/name/messari/velodrome-optimism")
# display(sgs)


# In[ ]:


def create_sg(tg_api):
        csg = sg.load_subgraph(tg_api)
        return csg


# In[ ]:


def get_velodrome_pool_tvl(pid, min_ts = 0, max_ts = 99999999999999):
        velo = create_sg('https://api.thegraph.com/subgraphs/name/messari/velodrome-optimism')
        q1 = velo.Query.liquidityPoolDailySnapshots(
        orderDirection='desc',
        first=1000,
                where=[
                velo.Query.liquidityPoolDailySnapshot.pool == pid,
                velo.Query.liquidityPoolDailySnapshot.timestamp > min_ts,
                velo.Query.liquidityPoolDailySnapshot.timestamp <= max_ts,
                ]
        )
        velo_tvl = sg.query_df([
                q1.id,
                q1.pool.id,
                q1.timestamp,
                q1.pool.inputTokens.id,
                q1.pool.inputTokens.symbol,
                
                q1.totalValueLockedUSD
                ]
                , pagination_strategy=ShallowStrategy)
        velo_wts = sg.query_df([
                q1.id,
                q1.pool.id,
                q1.timestamp,
                q1.inputTokenWeights,
                ]
                , pagination_strategy=ShallowStrategy)
        velo_reserves = sg.query_df([
                q1.id,
                q1.pool.id,
                q1.timestamp,
                q1.inputTokenBalances,
                ]
                , pagination_strategy=ShallowStrategy)
        
        df_array = [velo_tvl, velo_wts, velo_reserves]

        for df in df_array:
                df.columns = df.columns.str.replace('liquidityPoolDailySnapshots_', '')
                df['id_rank'] = df.groupby(['id']).cumcount()+1

        velo_tvl = velo_tvl.merge(velo_wts, on =['id','id_rank','pool_id','timestamp'])
        velo_tvl = velo_tvl.merge(velo_reserves, on =['id','id_rank','pool_id','timestamp'])

        velo_tvl['timestamp_dt'] = pd.to_datetime(velo_tvl['timestamp'],unit='s')
        velo_tvl['timestamp_day'] = pd.to_datetime(velo_tvl['timestamp'],unit='s').dt.floor('d')

        velo_tvl['inputTokenBalances'] = velo_tvl['inputTokenBalances'] / (10 ** 18)
        velo_tvl['inputToken_tvl'] = velo_tvl['totalValueLockedUSD'] * ( velo_tvl['inputTokenWeights'] / 100 )
        # velo_tvl['inputToken_price'] = velo_tvl['inputToken_tvl'] / velo_tvl['inputTokenBalances']

        #Standardize Columns
        # date	token	token_value	usd_value	protocol
        velo_tvl['protocol'] = 'Velodrome'
        velo_tvl = velo_tvl[['timestamp_day','pool_inputTokens_symbol','inputTokenBalances','inputToken_tvl','protocol']]
        velo_tvl = velo_tvl.rename(columns={
                'timestamp_day':'date',
                'pool_inputTokens_symbol':'token',
                'inputTokenBalances':'token_value',
                'inputToken_tvl':'usd_value'
        })

        return velo_tvl


# In[ ]:


def get_curve_pool_tvl(pid, min_ts = 0, max_ts = 99999999999999):
        curve = create_sg('https://api.thegraph.com/subgraphs/name/convex-community/volume-optimism')
        q1 = curve.Query.dailyPoolSnapshots(
        orderDirection='desc',
        first=1000,
                where=[
                curve.Query.dailyPoolSnapshot.pool == pid,
                curve.Query.dailyPoolSnapshot.timestamp > min_ts,
                curve.Query.dailyPoolSnapshot.timestamp <= max_ts,
                ]
        )
        curve_tvl = sg.query_df([
                q1.id,
                q1.pool.address,
                q1.pool.name,
                q1.pool.symbol,
                q1.timestamp,
                # q1.tvl,
                # q1.adminFeesUSD,
                # q1.lpFeesUSD,
                q1.pool.coinNames,
                # q1.normalizedReserves,
                # q1.reservesUSD,
                ]
                , pagination_strategy=ShallowStrategy)
        curve_reserves_normal = sg.query_df([
                q1.id,
                q1.pool.address,
                q1.timestamp,
                q1.normalizedReserves,
                # q1.pool.coinNames,
                
                # q1.reservesUSD
                ]
                , pagination_strategy=ShallowStrategy)
        curve_reserves_usd = sg.query_df([
                q1.id,
                q1.pool.address,
                q1.timestamp,
                q1.reservesUSD
                ]
                , pagination_strategy=ShallowStrategy)

        df_array = [curve_tvl, curve_reserves_normal, curve_reserves_usd]

        for df in df_array:
                df.columns = df.columns.str.replace('dailyPoolSnapshots_', '')
                df['id_rank'] = df.groupby(['id']).cumcount()+1

        curve_tvl = curve_tvl.merge(curve_reserves_normal, on =['id','id_rank','pool_address','timestamp'])
        curve_tvl = curve_tvl.merge(curve_reserves_usd, on =['id','id_rank','pool_address','timestamp'])

        curve_tvl['normalizedReserves'] = curve_tvl['normalizedReserves'] / ( 10 ** 18 ) #decimal adjust
        # curve_tvl['reservePrice'] = curve_tvl['reservesUSD'] / curve_tvl['normalizedReserves'] 
        curve_tvl['timestamp_dt'] = pd.to_datetime(curve_tvl['timestamp'],unit='s')

        #Standardize Columns
        # date	token	token_value	usd_value	protocol
        curve_tvl['protocol'] = 'Curve'
        curve_tvl = curve_tvl[['timestamp_dt','pool_coinNames','normalizedReserves','reservesUSD','protocol']]
        curve_tvl = curve_tvl.rename(columns={
                'timestamp_dt':'date',
                'pool_coinNames':'token',
                'normalizedReserves':'token_value',
                'reservesUSD':'usd_value'
        })

        return curve_tvl


# In[ ]:


def get_hop_pool_tvl(pid, min_ts = 0, max_ts = 99999999999999):
    api_base_str = 'https://api.llama.fi/protocol/'
    prot_str = 'hop-protocol'
    hop = dfl.get_single_tvl(api_base_str, prot_str, ['Optimism'])
    hop = hop[(hop['token'] == pid) & (~hop['token_value'].isna())]
    hop = hop[['date','token','token_value','usd_value','protocol']]
    hop['protocol'] = 'Hop' #rename to match func
    hop.reset_index(inplace=True,drop=True)
    return hop


# In[ ]:


# Note, this is not in TVL tracking format - maybe we split this to a new file ~eventually
def get_messari_sg_pool_snapshots(slug, chains = ['optimism'], min_ts = 0, max_ts = 99999999999999):
        msr_dfs = []
        print(slug)
        for c in chains:
                print(c)
                try:
                        # Set Chain
                        curve = create_sg('https://api.thegraph.com/subgraphs/name/messari/' + slug + '-' + c)
                        # Get Query
                        q1 = curve.Query.liquidityPoolDailySnapshots(
                        orderDirection='desc',
                        first=1000,
                                where=[
                                curve.Query.liquidityPoolDailySnapshot.timestamp > min_ts,
                                curve.Query.liquidityPoolDailySnapshot.timestamp <= max_ts,
                                ]
                        )
                        # Pull Fields
                        msr_list = sg.query_df([
                                q1.id,
                                q1.timestamp,
                                q1.totalValueLockedUSD,
                                q1.dailyVolumeUSD,
                                q1.rewardTokenEmissionsUSD,
                                #protocol
                                q1.protocol.id,
                                q1.protocol.name,
                                q1.protocol.slug,
                                q1.protocol.network,
                                #pool
                                q1.pool.id,
                                q1.pool.name,
                                q1.pool.symbol,
                                q1.pool.inputTokens.id
                        ]
                        , pagination_strategy=ShallowStrategy)
                        msr_df = pd.concat(msr_list)
                except:
                        msr_df = pd.DataFrame()
                        continue
                msr_dfs.append(msr_df)
        
        #combine all chains
        msr_daily = pd.concat(msr_dfs)

        #fix up column names

        msr_daily.columns = msr_daily.columns.str.replace('liquidityPoolDailySnapshots_', '')

        col_list = msr_daily.columns.to_list()
        col_list.remove('pool_inputTokens_id') # we want to group by everything else 

        print(col_list)
        
        msr_daily = msr_daily.fillna(0)

        msr_daily = msr_daily.groupby(col_list).agg({'pool_inputTokens_id':lambda x: list(x)})
        msr_daily.reset_index(inplace=True)

        msr_daily['timestamp'] = pd.to_datetime(msr_daily['timestamp'],unit='s')
        msr_daily['date'] = msr_daily['timestamp'].dt.floor('d')
        msr_daily['id_rank'] = msr_daily.groupby(['id']).cumcount()+1
        
        return pd.DataFrame(msr_daily)


# In[ ]:


# pdf = get_curve_pool_tvl('0x061b87122ed14b9526a813209c8a59a633257bab')
# vdf = get_velodrome_pool_tvl('0xfc77e39de40e54f820e313039207dc850e4c9e60')
# get_hop_pool_tvl('SNX')
# display(vdf)


# In[ ]:


# pd.

# msr = get_messari_sg_pool_snapshots('curve-finance')
# display(msr)


# In[ ]:


# ! jupyter nbconvert --to python optimism_pool_tvls.ipynb

