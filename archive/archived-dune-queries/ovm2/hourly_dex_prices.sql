--drop view if exists dune_user_generated.hourly_dex_prices
CREATE OR REPLACE view dune_user_generated.hourly_dex_prices AS

WITH hour_gs AS (
SELECT generate_series('11-11-2021'::timestamptz, DATE_TRUNC('hour',NOW()), '1 hour') AS hour
)

,dex_trades AS ( --only Uniswap now, should have SNX and others later
SELECT --pulled from eth dex_trades abstraction: https://github.com/duneanalytics/abstractions/blob/master/ethereum/dex/trades/insert_uniswap.sql
            t.evt_block_time AS block_time,
            'Uniswap' AS project,
            '3' AS version,
            'DEX' AS category,
            t."recipient" AS trader_a,
            NULL::bytea AS trader_b,
            abs(amount0) AS token_a_amount_raw,
            abs(amount1) AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            f.token0 AS token_a_address,
            f.token1 AS token_b_address,
            t.contract_address as exchange_contract_address,
            t.evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            t.evt_index
        FROM
            uniswap_v3."Pair_evt_Swap" t
        INNER JOIN dune_user_generated."uniswap_v3_pools" f ON f.pool = t.contract_address

)
, dex_price_stables AS(
--for tokens where dune doesn't have the price, calculate the median price, assuming USDT, DAI, USDC = 1
SELECT
hour,
token, symbol, decimals,
CASE WHEN symbol IN ('USDT','DAI','USDC') THEN 1
ELSE median_price
END AS median_price
,num_samples
FROM
(
SELECT *,
DENSE_RANK() OVER (PARTITION BY token ORDER BY hour DESC) AS hrank

FROM
(
SELECT
DATE_TRUNC('hour',block_time) AS hour,
token, symbol, decimals,
percentile_cont(0.5) WITHIN GROUP (ORDER BY token_price) AS median_price,
COUNT(*) AS num_samples
FROM (
    SELECT *,
    usd_amount/token_amount AS token_price
    FROM
    (
        SELECT --tokena
        t.block_time, t.exchange_contract_address,
        ea.symbol, ea.decimals,
        t.token_a_address AS token, --t.token_b_address,
        t.token_a_amount_raw/(10^ea.decimals) AS token_amount,
        --t.token_b_amount_raw/(10^eb.decimals) AS token_b_amount,
        CASE WHEN eb.symbol IN ('USDT','DAI','USDC') THEN --assume price = 1
            t.token_b_amount_raw/(10^eb.decimals) ELSE NULL
            END AS usd_amount

        FROM dex_trades t
        INNER JOIN erc20."tokens" ea --both need to have known decimals, we're not going to assume anything.
        ON ea."contract_address" = t.token_a_address
        INNER JOIN erc20."tokens" eb
        ON eb."contract_address" = t.token_b_address
        WHERE project = 'Uniswap' AND version = '3'
        
        AND t.token_a_amount_raw > 100 --min to exclude weird stuff
    ) tokena
    
    UNION ALL
    
    SELECT *,
    usd_amount/token_amount AS token_price
    FROM
    (
        SELECT --tokenb
        t.block_time, t.exchange_contract_address,
        eb.symbol, eb.decimals,
        t.token_b_address AS token,

        t.token_b_amount_raw/(10^eb.decimals) AS token_amount,
        CASE WHEN ea.symbol IN ('USDT','DAI','USDC') THEN --assume price = 1
        t.token_a_amount_raw/(10^ea.decimals) ELSE NULL
        END AS usd_amount
        
        FROM dex_trades t
        INNER JOIN erc20."tokens" ea --both need to have known decimals, we're not going to assume anything.
        ON ea."contract_address" = t.token_a_address
        INNER JOIN erc20."tokens" eb
        ON eb."contract_address" = t.token_b_address
        WHERE project = 'Uniswap' AND version = '3'
        
        AND t.token_b_amount_raw > 100 --min to exclude weird stuff
    ) tokenb
    
) a
WHERE token_price > 0
GROUP BY 1,2,3, 4

UNION ALL

SELECT
    '01-01-2000' AS hour, token::bytea, symbol, decimals, 1 AS median_price,1 AS num_samples
    FROM ( values
            ('\x7f5c764cbc14f9669b88837ca1490cca17c31607','USDC',6)
            ,('\x94b008aa00579c1307b0ef2c499ad98a8ce58e58','USDT',6)
            ,('\xda10009cbd5d07dd0cecc66161fc93d7c9000da1','DAI',18)
        ) t (token, symbol, decimals)

) b

) c
--WHERE hrank = 1 -- holdover if we want to turn this to latest price
)

--Use ETH Price to calculate other tokens that are only traded vs ETH, or more often traded vs eth

, dex_price_weth AS(

SELECT
hour,
token, symbol, decimals,
CASE WHEN symbol IN ('USDT','DAI','USDC') THEN 1
ELSE median_price
END AS median_price
,num_samples
FROM
(
SELECT *,
DENSE_RANK() OVER (PARTITION BY token ORDER BY hour DESC) AS hrank

FROM
(
SELECT
DATE_TRUNC('hour',block_time) AS hour,
token, symbol, decimals,
percentile_cont(0.5) WITHIN GROUP (ORDER BY token_price) AS median_price,
COUNT(*) AS num_samples
FROM (
    SELECT *,
    usd_amount/token_amount AS token_price
    FROM
    (
        SELECT --tokena
        t.block_time, t.exchange_contract_address,
        ea.symbol, ea.decimals,
        t.token_a_address AS token, 
        t.token_a_amount_raw/(10^ea.decimals) AS token_amount,
        
            t.token_b_amount_raw/(10^eb.decimals) * dp.median_price --#eth * latestusd
             AS usd_amount

        FROM dex_trades t
        INNER JOIN dune_user_generated.uniswap_v3_pools p ON
        t."exchange_contract_address" = p.pool
        INNER JOIN erc20."tokens" ea --both need to have known decimals, we're not going to assume anything.
        ON ea."contract_address" = t.token_a_address
        INNER JOIN erc20."tokens" eb
        ON eb."contract_address" = t.token_b_address
        INNER JOIN dex_price_stables dp ON --latest eth price
            t.token_b_address = dp.token
            AND dp.hour = DATE_TRUNC('hour',t.block_time)

        WHERE t.token_a_amount_raw > 100 --min to exclude weird stuff
        AND t.token_b_address = '\x4200000000000000000000000000000000000006' -- weth

    ) tokena
    
    UNION ALL
    
    SELECT *,
    usd_amount/token_amount AS token_price
    FROM
    (
        SELECT 
        t.block_time, t.exchange_contract_address,
        eb.symbol, eb.decimals,
        t.token_b_address AS token,
        t.token_b_amount_raw/(10^eb.decimals) AS token_amount,
        t.token_a_amount_raw/(10^ea.decimals) * dp.median_price --#eth * latestusd
         AS usd_amount
        FROM dex_trades t
        INNER JOIN dune_user_generated.uniswap_v3_pools p ON
        t."exchange_contract_address" = p.pool
        INNER JOIN erc20."tokens" ea --both need to have known decimals, we're not going to assume anything.
        ON ea."contract_address" = t.token_a_address
        INNER JOIN erc20."tokens" eb
        ON eb."contract_address" = t.token_b_address
        INNER JOIN dex_price_stables dp ON --latest eth price
            t.token_a_address = dp.token
            AND dp.hour = DATE_TRUNC('hour',t.block_time)
        WHERE t.token_b_amount_raw > 100 --min to exclude weird stuff
        AND t.token_a_address = '\x4200000000000000000000000000000000000006' --weth
    ) tokenb
    
) a
WHERE token_price > 0
GROUP BY 1,2,3, 4

) b

) c
--WHERE hrank = 1 -- holdover if we want to turn this to latest price
)

, dex_price_special_tokens AS (

SELECT s.hour, s.token, s.symbol, s.decimals,
median_price * price_ratio AS median_price,
 s.num_samples

FROM (
    WITH bridge AS (
        SELECT DATE_TRUNC('hour', dt) AS hour, "bridge_token"::bytea AS token, "bridge_symbol" AS symbol, "bridge_decimals" AS decimals , pr.num_samples,
        erc20_token AS mapped_token, "price_ratio"
    
        FROM dune_user_generated."hourly_bridge_token_price_ratios" pr--https://dune.xyz/queries/264679

        )
    
   , synth AS (
    
    SELECT DATE_TRUNC('hour', hour) AS hour, token::bytea, symbol, decimals, num_samples,
        '\x8c6f28f2f1a3c87f0f938b96d27520d9751ec8d9'::bytea AS mapped_token, "median_price" AS price_ratio
    FROM dune_user_generated."hourly_synth_prices" sp
        )
    SELECT * FROM bridge UNION ALL
    SELECT * FROM synth
    ) s

INNER JOIN dex_price_stables p
        ON s.mapped_token = p.token
        AND s.hour = p.hour
        
)


, price AS (
WITH get_best_price_estimate AS (
SELECT hour, token, symbol, decimals, median_price, num_samples
    FROM (    
        SELECT *, DENSE_RANK() OVER (PARTITION BY hour, token ORDER BY num_samples DESC, rnk ASC) AS p_rank --pick which price to take
        FROM (
            SELECT hour, token, symbol, decimals, median_price, num_samples, 1 AS rnk FROM dex_price_stables --WHERE "window" = 1 --when trades happened
            UNION ALL
            SELECT hour, token, symbol, decimals, median_price, num_samples, 2 AS rnk FROM dex_price_weth
            UNION ALL
            SELECT hour, token, symbol, decimals, median_price, num_samples, -1 AS rnk FROM dex_price_special_tokens --bridge tokens
            ) a
        ) r
    WHERE p_rank = 1
    )
SELECT hour, token, symbol, decimals, median_price, num_samples FROM get_best_price_estimate
UNION ALL
SELECT hour, '\xdeaddeaddeaddeaddeaddeaddeaddeaddead0000' AS token, 'ETH' AS symbol, 18 AS decimals, median_price, num_samples
FROM get_best_price_estimate WHERE token = '\x4200000000000000000000000000000000000006'
UNION ALL --Synapse Bridge WETH
SELECT hour, '\x121ab82b49b2bc4c7901ca46b8277962b4350204' AS token, 'ETH' AS symbol, 18 AS decimals, median_price, num_samples
FROM get_best_price_estimate WHERE token = '\x4200000000000000000000000000000000000006'
UNION ALL --Synapse Bridge WETH #2
SELECT hour, '\x1259adc9f2a0410d0db5e226563920a2d49f4454' AS token, 'ETH' AS symbol, 18 AS decimals, median_price, num_samples
FROM get_best_price_estimate WHERE token = '\x4200000000000000000000000000000000000006'

)
--Fill in gaps

, hour_token_gs AS (
WITH token_list AS (
    SELECT token, symbol, decimals FROM price 
    GROUP BY 1,2,3
    )

SELECT 
hour, token, symbol, decimals
FROM
token_list, hour_gs

)

--logic to fill in gaps https://dba.stackexchange.com/questions/186218/carry-over-long-sequence-of-missing-values-with-postgres
SELECT DATE_TRUNC('hour',hour) AS hour, token::bytea AS token, symbol::text, decimals::INT,

CAST( COALESCE(median_price, first_price) AS double precision) AS median_price,
COALESCE(num_samples,first_num_samples)::BIGINT AS num_samples

FROM (
    SELECT
    hour, token, symbol, decimals, grp
        , first_value(median_price) OVER (PARTITION BY token, grp ORDER BY hour) AS median_price
        , first_value(num_samples) OVER (PARTITION BY token, grp ORDER BY hour) AS num_samples
        ,CASE WHEN grp = 0 THEN AVG(CASE WHEN grp = 1 THEN median_price ELSE NULL END) OVER (PARTITION BY token) ELSE NULL END AS first_price
        ,(CASE WHEN grp = 0 THEN AVG(CASE WHEN grp = 1 THEN num_samples ELSE NULL END) OVER (PARTITION BY token) ELSE NULL END)::bigint AS first_num_samples
         
    FROM (
        SELECT 
        gs.hour, gs.token, gs.symbol, gs.decimals, p.median_price, p.num_samples, 
            count(p.median_price) OVER (PARTITION BY gs.token ORDER BY gs.hour) AS grp
        FROM hour_token_gs gs
        LEFT JOIN price p
            ON gs.hour = p.hour
            AND gs.token = p.token
            AND gs.symbol = p.symbol
            AND gs.decimals = p.decimals
        ) fill
    ) bf
