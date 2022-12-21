#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# ! pip install pandas
# ! pip install requests
# ! pip install plotly
# ! pip install datetime
# ! pip install os
# ! pip freeze = requirements.txt


# In[ ]:


import pandas as pd
import requests as r
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
import os
import time
import optimism_subgraph_tvls as subg
import defillama_utils as dfl
import pandas_utils as pu


# In[ ]:


pwd = os.getcwd()
if 'L2 TVL' in pwd:
    prepend = ''
else:
    prepend = 'L2 TVL/'


# In[ ]:


# Protocol Incentive Start Dates
# Eventually, move this to its own file / csv
protocols = pd.DataFrame(
    [
        # name, incentive start date
            # General Programs
             [1,'velodrome',  3000000,          '2022-07-13',   '2022-11-17',   '', 'Partner Fund', 'defillama','']
            ,[1,'pooltogether',   450000, '2022-07-22',   '', '', 'Partner Fund', 'defillama','']
            ,[1,'lyra',   3000000,               '2022-08-02',   '',   '', 'Gov Fund - Phase 0', 'defillama','']
            ,[1,'rubicon',    900000,            '2022-07-15',   '',   '', 'Gov Fund - Phase 0', 'defillama','']
            ,[1,'perpetual-protocol', 9000000, '2022-07-14',   '',   '', 'Gov Fund - Phase 0', 'defillama','']
            ,[1,'thales', 900000,             '2022-07-15',   '',   '', 'Gov Fund - Phase 0', 'defillama',''] #TVL not relevant
            ,[1,'aave-v3',    5000000,            '2022-08-04',   '2022-11-04',   'Aave - Liquidity Mining', 'Partner Fund', 'defillama','']
            ,[1,'wepiggy',    300000,            '2022-08-03',   '',   '', 'Gov Fund - Phase 0', 'defillama','']
            ,[1,'stargate',   1000000,           '2022-08-05',   '',   '', 'Gov Fund - Phase 0', 'defillama','']
            ,[1,'pika-protocol',  9000000,      '2022-08-29',   '',   '', 'Gov Fund - Phase 0', 'defillama','']
            ,[1,'pickle', 200000,             '2022-09-09',   '',   '', 'Gov Fund - Season 1', 'defillama','']
            ,[1,'aelin',  900000,              '2022-09-12',   '2022-09-14',   '', 'Gov Fund - Phase 0', 'defillama','']
            ,[1,'polynomial-protocol',    900000, '2022-09-14',   '',   '', 'Gov Fund - Phase 0', 'defillama','']
            ,[1,'xtoken', 900000,             '2022-09-19',   '',   '', 'Gov Fund - Season 1', 'defillama','']
            ,[1,'hop-protocol',   1000000,       '2022-09-22',   '',   '', 'Gov Fund - Phase 0', 'defillama','']
            ,[1,'beethoven-x',    500000,        '2022-09-29',   '',   '', 'Gov Fund - Season 1', 'defillama','']
            ,[1,'beefy',  650000*.5,              '2022-10-24',   '',   '', 'Gov Fund - Season 1', 'defillama',''] #Incenvitived VELO - Seems like Beefy boost started Oct 24? Unclear
            ,[1,'hundred-finance',    300000,    '2022-11-28',   '',   '', 'Gov Fund - Season 1', 'defillama','']
            #Uniswap LM Program
            ,[0,'uniswap-v3', 150000,         '2022-10-26',   '2022-11-21',   'Uniswap LM - Phase 1', 'Gov Fund - Phase 0', 'defillama','']
            ,[1,'arrakis-finance',    50000,    '2022-10-26',   '2022-11-21',   'Uniswap LM - Phase 1', 'Gov Fund - Phase 0','defillama','']
            ,[1,'gamma',    50000,              '2022-10-26',   '2022-11-21',   'Uniswap LM - Phase 1', 'Gov Fund - Phase 0','defillama','']
            ,[1,'xtoken',    50000,             '2022-10-26',   '2022-11-21',   'Uniswap LM - Phase 1', 'Gov Fund - Phase 0','defillama','']
            # Other DEX Programs
            ,[0,'synthetix',  9000000,    '2022-08-25',   '',   'sUSD & sETH: Curve', 'Gov Fund - Phase 0', 'subgraph-curve',['0x7bc5728bc2b59b45a58d9a576e2ffc5f0505b35e','0x061b87122ed14b9526a813209c8a59a633257bab']] # susd/usd + seth/eth Curve incentives started
            ,[1,'synthetix',  20000* (abs(pd.to_datetime("today")-pd.to_datetime('2022-08-25')).days / 7 ),    '2022-08-25',   '',   'sUSD-3Crv: Curve', 'Gov Fund - Phase 0', 'subgraph-curve',['0x061b87122ed14b9526a813209c8a59a633257bab']] # susd/usd + seth/eth Curve incentives started
            ,[1,'synthetix',  20000* (abs(pd.to_datetime("today")-pd.to_datetime('2022-08-25')).days / 7 ),    '2022-08-25',   '',   'sETH-ETH: Curve', 'Gov Fund - Phase 0', 'subgraph-curve',['0x7bc5728bc2b59b45a58d9a576e2ffc5f0505b35e']] # susd/usd + seth/eth Curve incentives started
            ,[1,'l2dao',  300000,    '2022-07-20',   '2022-08-22',   'L2DAO/OP: Velodrome', 'Gov Fund - Phase 0', 'subgraph-velodrome',['0xfc77e39de40e54f820e313039207dc850e4c9e60']] # l2dao/op incentives - estimating end date based on last distribution to Velo gauge + 7 days
            ,[1,'beefy',  650000*.35,    '2022-09-13',   '',   'BIFI/OP: Velodrome', 'Gov Fund - Phase 0', 'subgraph-velodrome',['0x81f638e5d063618fc5f6a976e48e9b803b3240c0']] # bifi/op incentives
            # Season 2
            ,[1,'velodrome',  4000000,  '2022-11-24',   '',   'Velodrome #2 (Tour de OP)', 'Gov Fund - Season 2', 'defillama','']
            ,[1,'revert-compoundor',  240000,  '2022-11-03',   '',   '', 'Gov Fund - Season 2', 'defillama','']
            ]
        , columns = ['include_in_summary','protocol','num_op','start_date', 'end_date','name', 'op_source', 'data_source','contracts']
    )
# print(protocols[0])
protocols['id_format'] = protocols['protocol'].str.replace('-',' ').str.title()

date_cols = ['start_date', 'end_date']
for d in date_cols:
    protocols[d] = pd.to_datetime( protocols[d] )
    
protocols['id_format'] = protocols['protocol'].str.replace('-',' ').str.title()

# protocols['program_name'] = np.where( protocols['name'] == '', protocols['id_format'], protocols['name'])
protocols['coalesce'] = np.where( protocols['name'] == ''
                                    , protocols['id_format']
                                    , protocols['name']
                                    )
# Get count by coalesced name
pcounts = pd.DataFrame( protocols.groupby(['coalesce'])['name'].count() )
pcounts = pcounts.rename(columns={'name':'count'})

protocols = protocols.merge(pcounts, on = 'coalesce')


protocols['program_name'] = np.where( ( (protocols['name'] == '') )#| (protocols['count'] == 1) )
                                    , protocols['id_format']
                                    , protocols['id_format'] + ' - ' + protocols['name']
                                    )

protocols = protocols.sort_values(by='start_date', ascending=True)
                    
# display(protocols)


# In[ ]:


api_str = 'https://api.llama.fi/protocol/'

prod = []
s = r.Session()

dfl_protocols = protocols[protocols['data_source'] == 'defillama'].copy()

dfl_slugs = dfl_protocols[['protocol']].drop_duplicates()
dfl_slugs = dfl_slugs.rename(columns={'protocol':'slug'})
df_dfl = dfl.get_range(dfl_slugs[['slug']],['Optimism'])

df_dfl = df_dfl.merge(dfl_protocols, on ='protocol')

df_dfl = df_dfl[['date', 'token', 'token_value', 'usd_value', 'protocol', 'start_date','end_date','program_name']]


# In[ ]:


# display(df_dfl)


# In[ ]:


subg_protocols = protocols[protocols['data_source'].str.contains('subgraph')].copy()
subg_protocols['og_protocol'] = subg_protocols['protocol']
subg_protocols['protocol'] = subg_protocols['data_source'].str.replace('subgraph-','')
# display(subg_protocols)

dfs_sub = []
for index, program in subg_protocols.iterrows():
        for c in program['contracts']:
                if program['protocol'] == 'curve':
                        sdf = subg.get_curve_pool_tvl(c)
                elif program['protocol'] == 'velodrome':
                        sdf = subg.get_velodrome_pool_tvl(c)
                sdf['start_date'] = program['start_date']
                sdf['end_date'] = program['end_date']
                sdf['program_name'] = program['program_name']
                sdf['protocol'] = program['og_protocol']

                sdf['token_value'] = sdf['token_value'].fillna(0)
                sdf['usd_value'] = sdf['usd_value'].fillna(0)
                dfs_sub.append(sdf)
df_df_sub = pd.concat(dfs_sub)
# display(df_df_sub.columns)


# In[ ]:


# display(df_df_sub.sort_values(by='date'))


# In[ ]:


df_df_comb = pd.concat([df_dfl, df_df_sub])
# display(df_df_comb)
df_df_comb['start_date'] = pd.to_datetime(df_df_comb['start_date'])
df_df_comb['end_date'] = pd.to_datetime(df_df_comb['end_date'])
df_df_comb['date'] = pd.to_datetime(df_df_comb['date'])
# display(df_df_comb)

# Make sure datatypes are clean
df_df_comb['token_value'] = df_df_comb['token_value'].astype('float64')
df_df_comb['usd_value'] = df_df_comb['usd_value'].astype('float64')

#create an extra day to handle for tokens dropping to 0
#this is a temp fix - longer term also: Get max of a token x date and do date + 1 = 0 (i.e. weth to eth flips)
# find intermediate gaps. Call it a 0 flow in the in-between dates (i.e. pooltogether)
df_df_shift = df_df_comb.copy()
df_df_shift['date'] = df_df_shift['date'] + timedelta(days=1)
df_df_shift['token_value'] = 0
df_df_shift['usd_value'] = 0

#merge back in
df_df = pd.concat([df_df_comb,df_df_shift])
df_df = df_df[df_df['date'] <= pd.to_datetime("today") ]

# Group - Exclude End Date since this is often null and overwritting could be weird, especially if we actually know an end date
df_df['start_date'] = df_df['start_date'].fillna( pd.to_datetime("today").floor('d') )

df_df = df_df.groupby(['date','token','protocol','start_date','program_name']).sum(numeric_only=True).reset_index()

# display(
#         df_df[(df_df['protocol']=='revert-compoundor') & (df_df['date'] == '2022-11-09')] 
#         )


# In[ ]:


# df_df
# df_df = df_df.fillna(0)
df_df.sample(20)
# for prot in protocols:
#         print( prot[0] )
# display(
#         df_df[(df_df['protocol'] =='revert-compoundor') & (df_df['date'] =='2022-11-09')].tail(10)
# )


# In[ ]:


data_df = df_df.copy()#merge(cg_df, on=['date','token'],how='inner')

# data_df = data_df[data_df['token_value'] > 0] #Exclude this, so we can read flows

data_df.sort_values(by='date',inplace=True)
# data_df['token_value'] = data_df['token_value'].replace(0, np.nan) #keep zeroes
data_df['price_usd'] = data_df['usd_value']/data_df['token_value']

data_df['rank_desc'] = data_df.groupby(['protocol', 'program_name', 'token'])['date'].\
                            rank(method='dense',ascending=False).astype(int)

data_df.sort_values(by='date',inplace=True)

last_df = data_df[data_df['rank_desc'] == 1]
last_df = last_df.rename(columns={'price_usd':'last_price_usd'})
last_df = last_df[['token','protocol','program_name','last_price_usd']]
# display(last_df)


# In[ ]:


data_df = data_df.merge(last_df, on=['token','protocol','program_name'], how='left')

data_df['last_token_value'] = data_df.groupby(['token','protocol', 'program_name'])['token_value'].shift(1)

data_df['last_price_usd'] = data_df.groupby(['token','protocol', 'program_name'])['price_usd'].shift(1)

# If first instnace of token, make sure there's no price diff
data_df['last_price_usd'] = data_df[['last_price_usd', 'price_usd']].bfill(axis=1).iloc[:, 0]
#Forward fill if token drops off
data_df['price_usd'] = data_df[['price_usd','last_price_usd']].bfill(axis=1).iloc[:, 0]

data_df['last_token_value'] = data_df['last_token_value'].fillna(0)

data_df['net_token_flow'] = data_df['token_value'] - data_df['last_token_value']
data_df['net_price_change'] = data_df['price_usd'] - data_df['last_price_usd']

data_df['net_dollar_flow'] = data_df['net_token_flow'] * data_df['price_usd']
data_df['last_price_net_dollar_flow'] = data_df['net_token_flow'] * data_df['last_price_usd']

data_df['net_price_stock_change'] = data_df['last_token_value'] * data_df['net_price_change']


# display(data_df)


# In[ ]:


data_df.to_csv('tvl_flows_by_token.csv')


# In[ ]:


# data_df[data_df['protocol']=='perpetual-protocol'].sort_values(by='date')
# data_df.fillna(0)
# data_df.sample(5)
# data_df[(data_df['protocol'] == 'pooltogether') & (data_df['date'] >= '2022-10-06') & (data_df['date'] <= '2022-10-12')].tail(10)


# In[ ]:


netdf_df = data_df[data_df['date']>= data_df['start_date']][['date','protocol','program_name','net_dollar_flow','net_price_stock_change','last_price_net_dollar_flow','usd_value']]

netdf_df = netdf_df.groupby(['date','protocol','program_name']).sum(['net_dollar_flow','net_price_stock_change','last_price_net_dollar_flow','usd_value'])

# reset & get program data
netdf_df.reset_index(inplace=True)

netdf_df['tvl_change'] = netdf_df['usd_value'] - netdf_df.groupby(['protocol', 'program_name'])['usd_value'].shift(1)
netdf_df['error'] = netdf_df['tvl_change'] - (netdf_df['net_dollar_flow'] + netdf_df['net_price_stock_change'])

cumul_cols = ['net_dollar_flow','last_price_net_dollar_flow','net_price_stock_change']
for c in cumul_cols:
        netdf_df['cumul_' + c] = netdf_df.groupby(['protocol', 'program_name'])[c].cumsum()
        # netdf_df['cumul_last_price_net_dollar_flow'] = netdf_df.groupby(['protocol', 'program_name'])['last_price_net_dollar_flow'].cumsum()
        # netdf_df['cumul_net_price_stock_change'] = netdf_df.groupby(['protocol', 'program_name'])['net_price_stock_change'].cumsum()

# display(netdf_df)

# Bring Program info Back In
netdf_df = netdf_df.merge(protocols[['include_in_summary','program_name','protocol','op_source','start_date','end_date','num_op']], on=['program_name','protocol'])

#For Summary
if_ended_cols = ['net_dollar_flow','last_price_net_dollar_flow']
new_ended_cols = []
for e in if_ended_cols:
        netdf_df['cumul_' + e + '_if_ended'] = netdf_df[~netdf_df['end_date'].isna()].groupby(['protocol', 'program_name'])[e].cumsum()
        new_ended_cols.append('cumul_' + e + '_if_ended')
#
# print(new_ended_cols)
# display(netdf_df[netdf_df['protocol'] == 'revert-compoundor'])

for d in date_cols:
        netdf_df[d] = pd.to_datetime(netdf_df[d])

# check info at program end
# display(program_end_df)


# In[ ]:


summary_cols = ['cumul_net_dollar_flow','cumul_last_price_net_dollar_flow','cumul_net_price_stock_change','num_op']

# for sc in summary_cols:
#         netdf_df[sc] = netdf_df[sc].astype('int64')
summary_cols = summary_cols + new_ended_cols
# print(summary_cols)
program_end_df = netdf_df[pd.to_datetime(netdf_df['date']) == pd.to_datetime(netdf_df['end_date'])].groupby(['protocol', 'program_name']).sum(numeric_only=True)
program_end_df.reset_index(inplace=True)
# display(program_end_df)

# display(program_end_df)
for s in summary_cols:
        s_new = s+'_at_program_end'
        program_end_df = program_end_df.rename(columns={s:s_new})
        netdf_df = netdf_df.merge(program_end_df[['protocol','program_name',s_new]], on=['protocol','program_name'], how = 'left')

# netdf_df['cumul_net_dollar_flow_at_program_end'] = netdf_df[is_program_end].groupby(['protocol', 'program_name']).sum(['cumul_net_dollar_flow'])
# netdf_df['cumul_last_price_net_dollar_flow_at_program_end'] = netdf_df[netdf_df['date'] == netdf_df['end_date']]['last_price_net_dollar_flow'].groupby(['protocol', 'program_name']).cumsum()
# netdf_df['cumul_net_price_stock_change_at_program_end'] = netdf_df[netdf_df['date'] == netdf_df['end_date']]['net_price_stock_change'].groupby(['protocol', 'program_name']).cumsum()

netdf_df['program_rank_desc'] = netdf_df.groupby(['protocol', 'program_name'])['date'].\
                            rank(method='dense',ascending=False).astype(int)

# netdf_df.loc[ netdf_df['end_date'] == pd.to_datetime("2000-01-01"), 'end_date' ] == pd.to_datetime("1900-01-01")

# np.where( netdf_df['end_date'] <= pd.to_datetime("2000-01-01") , pd.NaT , netdf_df['end_date'] )
# display(netdf_df[netdf_df['protocol'] == 'hundred-finance'].sort_values(by='program_rank_desc'))


# In[ ]:


# netdf_df[(netdf_df['date'] >= '2022-10-06') & (netdf_df['date'] <= '2022-10-12')].tail(10)
netdf_df.tail()


# In[ ]:


during_str = 'During Program'
post_str = 'Post-Program'

netdf_df['period'] = np.where(
        netdf_df['date'] > netdf_df['end_date'], post_str, during_str
        )
netdf_df.to_csv(prepend + 'img_outputs/app/op_summer_daily_stats.csv')


# In[ ]:


# display(
#         netdf_df[(netdf_df['protocol'] =='revert-compoundor') & (netdf_df['date'] <='2022-11-15')].tail(10)
# )

# display(
#         data_df[(data_df['protocol'] =='revert-compoundor') & (data_df['date'] =='2022-11-09')]
# )


# In[ ]:


latest_data_df = netdf_df[netdf_df['program_rank_desc'] == 1]
latest_data_df['date'] = latest_data_df['date'].dt.date
# latest_data_df['days_since_program_end'] 
# latest_data_df.loc[latest_data_df['end_date'] != '', 'days_since_program_end'] = \
#         pd.to_datetime(latest_data_df['end_date']) \
#         - pd.to_datetime(latest_data_df['date'])

latest_data_df['days_since_program_end'] = \
        np.where(latest_data_df['end_date'] != '',
        pd.to_datetime(latest_data_df['end_date']) \
        - pd.to_datetime(latest_data_df['date']) \
        , \
        pd.to_datetime(latest_data_df['date']) \
        - pd.to_datetime(latest_data_df['start_date']) \
        )


# In[ ]:


# Generate agg summary df
season_summary = latest_data_df[latest_data_df['include_in_summary'] == 1].copy()

season_summary_s0_no_perp = season_summary[(season_summary['op_source'] == 'Gov Fund - Phase 0') \
                                                & (season_summary['protocol'] != 'perpetual-protocol')]

season_summary_s0_no_perp['op_source'] = 'Gov Fund - Phase 0 (Excl. Perp)'

season_summary = pd.concat([season_summary, season_summary_s0_no_perp])

season_summary = season_summary.groupby('op_source').sum()
season_summary.reset_index(inplace=True)
season_summary.head()


# In[ ]:


# display(season_summary)


# In[ ]:


df_list = [latest_data_df, season_summary]
latest_data_df.name = 'latest_data_df'
season_summary.name = 'season_summary'

for df in df_list:

        df['cumul_flows_per_op_at_program_end'] = df['cumul_net_dollar_flow_at_program_end'] / df['num_op_at_program_end']
        df['cumul_flows_per_op_latest'] = df['cumul_net_dollar_flow'] / df['num_op']

        df['last_price_net_dollar_flows_per_op_at_program_end'] = df['cumul_last_price_net_dollar_flow_at_program_end'] / df['num_op_at_program_end']
        df['last_price_net_dollar_flows_per_op_latest'] = df['cumul_last_price_net_dollar_flow'] / df['num_op']

        df['flows_retention'] = \
                        df['cumul_net_dollar_flow_if_ended'] / df['cumul_net_dollar_flow_at_program_end'] \
                        * np.where(df['cumul_net_dollar_flow'] < 0, -1, 1)
        df['last_price_net_dollar_flows_retention'] = \
                        df['cumul_last_price_net_dollar_flow_if_ended'] / df['cumul_last_price_net_dollar_flow_at_program_end'] \
                        * np.where(df['cumul_last_price_net_dollar_flow'] < 0, -1, 1)


# In[ ]:


for df in df_list:
    #get df name
    col_list = [
        'date','program_name', 'num_op','period','op_source','start_date','end_date'
        ,'cumul_net_dollar_flow_at_program_end'
        ,'cumul_net_dollar_flow'
        ,'cumul_flows_per_op_at_program_end','cumul_last_price_net_dollar_flow_at_program_end','cumul_flows_per_op_latest'
        , 'last_price_net_dollar_flows_per_op_at_program_end','last_price_net_dollar_flows_per_op_latest'
        ,'flows_retention', 'last_price_net_dollar_flows_retention'
    ]
    summary_exclude_list = ['date','program_name','period','start_date','end_date']
    sort_cols = ['Start','# OP']

    if df.name == 'latest_data_df':
        html_name = 'op_summer_latest_stats'
    elif df.name == 'season_summary':
        html_name = 'season_summary_stats'
        sort_cols = ['Source','# OP']
        col_list = [x for x in col_list if x not in summary_exclude_list]
    else:
        html_name = 'other'

    df_format = df.copy()
    new_cols = df_format.columns
    drop_cols = ['net_dollar_flow',
        'net_price_stock_change', 'last_price_net_dollar_flow', 'usd_value',
        'tvl_change', 'error'
        ]
    new_cols = new_cols.drop(drop_cols)
    # print(new_cols)
    df_format = df_format[new_cols]

    df_format['num_op'] = df_format['num_op'].apply(lambda x: '{0:,.0f}'.format(x) if not pd.isna(x) else x )
    df_format['flows_retention'] = df_format['flows_retention'].apply(lambda x: '{:,.1%}'.format(x) if not pd.isna(x) else x )
    df_format['last_price_net_dollar_flows_retention'] = df_format['last_price_net_dollar_flows_retention'].apply(lambda x: '{:,.1%}'.format(x) if not pd.isna(x) else x )

    df_format = df_format[col_list]
    df_format = df_format.reset_index(drop=True)
    df_format.to_csv(prepend + 'img_outputs/app/op_summer_latest_stats.csv')

    format_cols = [
        'cumul_flows_per_op_at_program_end','cumul_flows_per_op_latest','last_price_net_dollar_flows_per_op_at_program_end','last_price_net_dollar_flows_per_op_latest']
    format_mil_cols = [
        'cumul_net_dollar_flow', 'cumul_net_dollar_flow_at_program_end',
        'cumul_last_price_net_dollar_flow_at_program_end'
    ]
    for f in format_cols:
        df_format[f] = df_format[f].apply(lambda x: '${0:,.2f}'.format(x) if not pd.isna(x) else x )
    for fm in format_mil_cols:
        df_format[fm] = df_format[fm].apply(lambda x: '${0:,.2f}M'.format(x/1e6) if not pd.isna(x) else x )


    df_format = df_format.rename(columns={
        'date':'Date', 'program_name':'Program', 'num_op': '# OP'
        ,'period': 'Period','op_source': 'Source','start_date':'Start','end_date':'End'
        ,'cumul_net_dollar_flow_at_program_end':'Net Flows (at End Date)'
        ,'cumul_net_dollar_flow':'Net Flows (Latest)'
        ,'cumul_last_price_net_dollar_flow_at_program_end':'Net Flows @ Current Prices (Latest)'
        ,'cumul_flows_per_op_at_program_end': 'Net Flows per OP (at End Date)'
        ,'cumul_flows_per_op_latest': 'Net Flows per OP (Latest)'
        ,'last_price_net_dollar_flows_per_op_at_program_end': 'Net Flows per OP @ Current Prices (at End Date)'
        ,'last_price_net_dollar_flows_per_op_latest': 'Net Flows per OP @ Current Prices (Latest)'
        ,'flows_retention' : 'Net Flows Retained'
        ,'last_price_net_dollar_flows_retention' : 'Net Flows Retained @ Current Prices'
    })
    df_format = df_format.fillna('')
    df_format = df_format.reset_index(drop=True)
    df_format = df_format.sort_values(by=sort_cols, ascending = [True,False])

    pd_html = pu.generate_html(df_format)
    open(prepend + "img_outputs/app/" + html_name + ".html", "w").write(pd_html)


# latest_data_df_format.to_html('op_summer_latest_stats.html')


# In[ ]:


#Filter for Charts

netdf_df = netdf_df[netdf_df['date'] <= pd.to_datetime("today").floor('d')]


# In[ ]:


fig = px.line(netdf_df, x="date", y="net_dollar_flow", color="program_name", \
             title="Daily Net Dollar Flow since Program Announcement",\
            labels={
                     "date": "Day",
                     "net_dollar_flow": "Net Dollar Flow (N$F)"
                 }
            )
fig.update_layout(
    legend_title="App Name"
)
fig.update_layout(yaxis_tickprefix = '$')
fig.write_image(prepend + "img_outputs/svg/daily_ndf.svg")
fig.write_image(prepend + "img_outputs/png/daily_ndf.png")
fig.write_html(prepend + "img_outputs/daily_ndf.html", include_plotlyjs='cdn')

# cumul_fig = px.area(netdf_df, x="date", y="cumul_net_dollar_flow", color="program_name", \
#              title="Cumulative Dollar Flow since Program Announcement",\
#                    labels={
#                      "date": "Day",
#                      "cumul_net_dollar_flow": "Cumulative Net Dollar Flow (N$F)"
#                  }
#             ,areamode='group')
# cumul_fig.update_layout(yaxis_tickprefix = '$')
# cumul_fig_app.show()


cumul_fig = go.Figure()
proto_names = netdf_df['program_name'].drop_duplicates()
# print(proto_names)
for p in proto_names:
    cumul_fig.add_trace(go.Scatter(x=netdf_df[netdf_df['program_name'] == p]['date'] \
                                   , y=netdf_df[netdf_df['program_name'] == p]['cumul_net_dollar_flow'] \
                                    ,name = p\
                                  ,fill='tozeroy')) # fill down to xaxis

cumul_fig.update_layout(yaxis_tickprefix = '$')
cumul_fig.update_layout(
    title="Cumulative Net Dollar Flow since Program Announcement",
    xaxis_title="Day",
    yaxis_title="Cumulative Net Dollar Flow (N$F)",
    legend_title="App Name",
#     color_discrete_map=px.colors.qualitative.G10
)
cumul_fig.write_image(prepend + "img_outputs/svg/cumul_ndf.svg") #prepend + 
cumul_fig.write_image(prepend + "img_outputs/png/cumul_ndf.png") #prepend + 
cumul_fig.write_html(prepend + "img_outputs/cumul_ndf.html", include_plotlyjs='cdn')


fig_last = go.Figure()
proto_names = netdf_df['program_name'].drop_duplicates()
# print(proto_names)
for p in proto_names:
    fig_last.add_trace(go.Scatter(x=netdf_df[netdf_df['program_name'] == p]['date'] \
                                   , y=netdf_df[netdf_df['program_name'] == p]['cumul_last_price_net_dollar_flow'] \
                                    ,name = p\
                                  ,fill='tozeroy')) # fill down to xaxis

fig_last.update_layout(yaxis_tickprefix = '$')
fig_last.update_layout(
    title="Cumulative Net Dollar Flow since Program Announcement (At Most Recent Token Price)",
    xaxis_title="Day",
    yaxis_title="Cumulative Net Dollar Flow (N$F) - At Most Recent Price",
    legend_title="App Name",
#     color_discrete_map=px.colors.qualitative.G10
)
fig_last.write_image(prepend + "img_outputs/svg/cumul_ndf_last_price.svg")
fig_last.write_image(prepend + "img_outputs/png/cumul_ndf_last_price.png")
fig_last.write_html(prepend + "img_outputs/cumul_ndf_last_price.html", include_plotlyjs='cdn')
# cumul_fig.show()


# In[ ]:


# Program-Specific Charts

value_list = ['cumul_net_dollar_flow','cumul_last_price_net_dollar_flow']

for val in value_list:
  if val == 'cumul_last_price_net_dollar_flow':
    postpend = " - At Last Price"
    folder_path = "/last_price"
  else:
    postpend = ""
    folder_path = ""
  proto_names = netdf_df['program_name'].drop_duplicates()
  # print(proto_names)
  for p in proto_names:
      cumul_fig_app = go.Figure()
      p_df = netdf_df[netdf_df['program_name'] == p]
      # cumul_fig_app = px.area(p_df, x="date", y="cumul_net_dollar_flow", color="period")
      
      during_df = p_df[p_df['period'] == during_str]
      cumul_fig_app.add_trace(go.Scatter(x= during_df['date'] \
                                    , y= during_df[val] \
                                      , name = during_str \
                                    ,fill='tozeroy')) # fill down to xaxis
      
      post_df = p_df[p_df['period'] == post_str]
      cumul_fig_app.add_trace(go.Scatter(x= post_df['date'] \
                                    , y= post_df[val] \
                                      , name = post_str \
                                    ,fill='tozeroy')) # fill down to xaxis

      cumul_fig_app.update_layout(yaxis_tickprefix = '$')
      cumul_fig_app.update_layout(
          title=p + ": Cumulative Net Dollar Flow since Program Announcement" + postpend,
          xaxis_title="Day",
          yaxis_title="Cumulative Net Dollar Flow (N$F)",
          legend_title="Period",
      #     color_discrete_map=px.colors.qualitative.G10
      )
      
      if not os.path.exists(prepend + "img_outputs/app" + folder_path):
        os.mkdir(prepend + "img_outputs/app" + folder_path)
      if not os.path.exists(prepend + "img_outputs/app" + folder_path + "/svg"):
        os.mkdir(prepend + "img_outputs/app" + folder_path + "/svg")
      if not os.path.exists(prepend + "img_outputs/app" + folder_path + "/png"):
        os.mkdir(prepend + "img_outputs/app/" + folder_path + "/png")
      
      p_file = p
      p_file = p_file.replace(' ','_')
      p_file = p_file.replace(':','')
      p_file = p_file.replace('/','-')
      cumul_fig_app.write_image(prepend + "img_outputs/app" + folder_path + "/svg/cumul_ndf_" + p_file + ".svg") #prepend + 
      cumul_fig_app.write_image(prepend + "img_outputs/app" + folder_path + "/png/cumul_ndf_" + p_file + ".png") #prepend + 
      cumul_fig_app.write_html(prepend + "img_outputs/app" + folder_path + "/cumul_ndf_" + p_file + ".html", include_plotlyjs='cdn')
      # cumul_fig_app.show()


# In[ ]:


fig.show()
cumul_fig.show()
print("yay")


# In[ ]:


# ! jupyter nbconvert --to python optimism_app_net_flows.ipynb

