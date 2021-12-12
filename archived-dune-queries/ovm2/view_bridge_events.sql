--drop view if exists dune_user_generated.view_bridge_events;
CREATE OR REPLACE view dune_user_generated.view_bridge_events AS


WITH unified_bridges AS (

    WITH ext_bridges AS (
    --CELER
    SELECT 
    'Celer' AS protocol, 'In' AS bridge_type, 
    evt_block_time AS dt, evt_tx_hash,
    sender, receiver, token, amount, "srcChainId" AS chain
    FROM celer."CBridge_evt_LogNewTransferIn" --transfer in to celer, so bridge out of OE
    GROUP BY 1,2,3,4,5,6,7,8,9 --DISTINCT
    
    UNION ALL
    
    SELECT 
    'Celer' AS protocol, 'Out' AS bridge_type, 
    evt_block_time AS dt, evt_tx_hash,
    sender, receiver, token, amount, "dstChainId" AS chain
    FROM celer."CBridge_evt_LogNewTransferOut" --transfer out of celer, so bridge in to OE
    GROUP BY 1,2,3,4,5,6,7,8,9 --DISTINCT
        
    UNION ALL
    --HOP
    
    SELECT 
    'Hop' AS protocol, 'Out' AS bridge_type, 
    ts.evt_block_time AS dt, ts.evt_tx_hash,
    t."from" AS sender, "recipient" AS receiver, l."contract_address" AS token, ts.amount, ts."chainId" AS chain
    FROM hop_protocol."L2_OptimismBridge_evt_TransferSent" ts
        INNER JOIN optimism.transactions t
            ON t.hash = ts.evt_tx_hash
        INNER JOIN erc20."ERC20_evt_Transfer" l --find the erc20 token
            ON l.evt_tx_hash = ts.evt_tx_hash
            AND (l."from" = t."from" --sender
                OR
                    (l."from" = t."to" --eth transfer
                    AND l."contract_address" = '\x4200000000000000000000000000000000000006'
                    )
                )
    GROUP BY 1,2,3,4,5,6,7,8,9 --DISTINCT
    
    UNION ALL
    
    SELECT 
    'Celer' AS protocol, 'Out' AS bridge_type, "evt_block_time", "evt_tx_hash",
    "sender","receiver","token","amount","dstChainId" AS chain
    
    FROM celer."Bridge_evt_Send"
    GROUP BY 1,2,3,4,5,6,7,8,9 --DISTINCT
            
    UNION ALL --relay bridge contract (bonder offline)
    
    SELECT 
    'Celer' AS protocol, 'In' AS bridge_type, "evt_block_time", "evt_tx_hash",
    "sender","receiver","token","amount","srcChainId" AS chain
    
    FROM celer."Bridge_evt_Relay"
    GROUP BY 1,2,3,4,5,6,7,8,9 --DISTINCT

    UNION ALL
    
    SELECT
    'Hop' AS protocol, 'In' AS bridge_type, 
    tc.evt_block_time AS dt, tc.evt_tx_hash,
    NULL::bytea AS sender, "recipient" AS receiver, l."contract_address" AS token, tc.amount, 1 AS chain --hop doesn't share source chain, but assume this is L1?
    
    FROM hop_protocol."L2_OptimismBridge_evt_TransferFromL1Completed" tc
        LEFT JOIN erc20."ERC20_evt_Transfer" l --find the erc20 token
            ON l.evt_tx_hash = tc.evt_tx_hash
            AND (l."to" = tc.recipient --receiver
                OR
                    (/*l."from" = tc.recipient --eth transfer
                    AND*/ l."contract_address" = '\x4200000000000000000000000000000000000006'
                    )
                )
    GROUP BY 1,2,3,4,5,6,7,8,9 --DISTINCT
    
    UNION ALL
    
    SELECT protocol, bridge_type, dt, evt_tx_hash, sender, receiver, token, amount, chain
    FROM (
        SELECT DISTINCT
        'Hop' AS protocol, 'In' AS bridge_type, 
        wb.evt_block_time AS dt, wb.evt_tx_hash,
        NULL::bytea AS sender, l."to" AS receiver, l."contract_address" AS token, amount, NULL::numeric AS chain --hop doesn't share source chain...
        ,l."evt_index", DENSE_RANK() OVER (PARTITION BY l.evt_tx_hash ORDER BY l."evt_index" DESC) AS index_rank --we want the last one, usually 12
        FROM hop_protocol."L2_OptimismBridge_evt_WithdrawalBonded" wb
            INNER JOIN dune_user_generated."Hop_L2_AmmWrapper" amm
                ON wb.contract_address = amm.contract_address
                
            INNER JOIN erc20."ERC20_evt_Transfer" l --find the erc20 token
                ON l.evt_tx_hash = wb.evt_tx_hash
                AND (
                    l."from" = amm."_ammwrapper" --from ammwrapper
                    OR
                    l.contract_address = '\x4200000000000000000000000000000000000006'
                    )
    
        WHERE NOT EXISTS (SELECT 1 FROM hop_protocol."L2_OptimismBridge_evt_TransferFromL1Completed" tc WHERE tc.evt_tx_hash = wb.evt_tx_hash)
        AND amm."call_success" = true
        ) herc
    WHERE index_rank = 1 --last transfer, which is the erc20
    GROUP BY 1,2,3,4,5,6,7,8,9 --DISTINCT
            
        UNION ALL --Synapse
        
        SELECT
        protocol, bridge_type, s.evt_block_time AS dt, s."evt_tx_hash" AS evt_tx_hash, sender, receiver,
        COALESCE(t.contract_address,s.token) as token, COALESCE(t.value,s.amount) as amount, 
        s.chain
        --get actual token
        FROM (
            SELECT
            'Synapse' AS protocol, 'Out' AS bridge_type, "evt_block_time", "evt_tx_hash", NULL::bytea AS sender,
            "to" AS receiver, "token" AS token, "amount" AS amount,"chainId" AS chain
            
            FROM synapse."SynapseBridge_evt_TokenRedeem"
            
            UNION ALL
            
            SELECT
            'Synapse' AS protocol, 'Out' AS bridge_type, "evt_block_time", "evt_tx_hash", NULL::bytea AS sender,
            "to" AS receiver, "token" AS token, "amount" AS amount,"chainId" AS chain
            
            FROM synapse."SynapseBridge_evt_TokenRedeemAndSwap"
                
            ) s
        LEFT JOIN erc20."ERC20_evt_Transfer" t
            ON t.evt_tx_hash = s.evt_tx_hash
            AND t.contract_address != s.token
            GROUP BY 1,2,3,4,5,6,7,8,9 --DISTINCT
            
        UNION ALL
        
        SELECT
        protocol, bridge_type, block_time, "tx_hash", sender, receiver,
        (CASE WHEN is_base = 1 THEN t.contract_address ELSE token END) AS contract_address, 
        COALESCE(t.value,s.amount) as amount, s.chain
        --get actual token
        FROM (
            SELECT --synapse."SynapseBridge_evt_TokenMintAndSwap"
            'Synapse' AS protocol, 'In' AS bridge_type, 
            "block_time", "tx_hash",
            substring( l.topic2, 13, 20)::bytea AS sender, substring( l.topic2, 13, 20)::bytea AS receiver,
            substring( decode ( SUBSTRING ( encode(l."data", 'hex') , (64*0)+1 , 64 ), 'hex'),13,20)::bytea AS token,
            bytea2numeric ( decode ( SUBSTRING ( encode(l."data", 'hex') , (64*1)+1 , 64 ), 'hex')) AS amount,
            bytea2numeric ( decode ( SUBSTRING ( encode(l."data", 'hex') , (64*4)+1 , 64 ), 'hex')) AS chain,
            bytea2numeric ( decode ( SUBSTRING ( encode(l."data", 'hex') , (64*7)+1 , 64 ), 'hex')) AS is_base
            
            FROM optimism.logs l
            
            WHERE l.contract_address = '\xaf41a65f786339e7911f4acdad6bd49426f2dc6b'
                AND l.topic1 IN ( '\x4f56ec39e98539920503fd54ee56ae0cbebe9eb15aa778f18de67701eeae7c65' )
            ) s
        LEFT JOIN erc20."ERC20_evt_Transfer" t
            ON t.evt_tx_hash = s.tx_hash
            AND t.contract_address != s.token
            GROUP BY 1,2,3,4,5,6,7,8,9 --DISTINCT
        
        UNION ALL
        
        SELECT --synapse nToken --SynapseBridge_evt_TokenMint
            'Synapse' AS protocol, 'In' AS bridge_type, 
            "block_time", "tx_hash",
            substring( l.topic2, 13, 20)::bytea AS sender, substring( l.topic2, 13, 20)::bytea AS receiver,
            substring( decode ( SUBSTRING ( encode(l."data", 'hex') , (64*0)+1 , 64 ), 'hex'),13,20)::bytea AS token,
            bytea2numeric ( decode ( SUBSTRING ( encode(l."data", 'hex') , (64*1)+1 , 64 ), 'hex')) AS amount,
            --bytea2numeric ( decode ( SUBSTRING ( encode(l."data", 'hex') , (64*4)+1 , 64 ), 'hex')) 
            NULL AS chain
            
            FROM optimism.logs l
            
            WHERE l.contract_address = '\xaf41a65f786339e7911f4acdad6bd49426f2dc6b'
                AND l.topic1 IN ( '\xbf14b9fde87f6e1c29a7e0787ad1d0d64b4648d8ae63da21524d9fd0f283dd38' )
        
        
        UNION ALL --Connext / Ins and Out use the same event, args are in a json
        
        SELECT bridge, bridge_type, dt, evt_tx_hash, sender, receiver,
        CASE WHEN token = '\x0000000000000000000000000000000000000000' THEN '\xdeaddeaddeaddeaddeaddeaddeaddeaddead0000'
            ELSE token END AS token,
        amount, chain
        
        FROM (
            SELECT 
            'Connext' AS bridge,
            CASE WHEN "sendingChainId" = 10 THEN 'In'
                WHEN "receivingChainId" = 10 THEN 'Out'
                ELSE NULL END
                AS bridge_type, 
            
            evt_block_time AS dt, evt_tx_hash, "initiator" AS sender,  "receivingAddress" AS receiver,
            
            CASE WHEN "sendingChainId" = 10 THEN "sendingAssetId"
                    WHEN "receivingChainId" = 10 THEN "receivingAssetId"
                    ELSE NULL END
                    AS token,
            amount, 
            
            CASE WHEN "sendingChainId" = 10 THEN "receivingChainId"
                    WHEN "receivingChainId" = 10 THEN "sendingChainId"
                    ELSE NULL END AS chain
            FROM (
                SELECT *
                ,(json_args->>'amount')::numeric AS "amount"
                ,(json_args->>'sendingChainId')::INT AS "sendingChainId"
                ,(json_args->>'receivingChainId')::INT AS "receivingChainId"
                ,REPLACE((json_args->>'initiator')::text,'0x','\x')::bytea AS "initiator"
                ,REPLACE((json_args->>'receivingAddress')::text,'0x','\x')::bytea AS "receivingAddress"
                ,REPLACE((json_args->>'sendingAssetId')::text,'0x','\x')::bytea AS "sendingAssetId"
                ,REPLACE((json_args->>'receivingAssetId')::text,'0x','\x')::bytea AS "receivingAssetId"
                FROM (
                    SELECT *, (args::json->>'txData')::json AS json_args
                    FROM connext."TransactionManager_evt_TransactionFulfilled"
                    ) jso
                ) raw
        ) mapped
        
        UNION ALL --Poly Network
        
        SELECT
        bridge, bridge_type, dt, evt_tx_hash,
        sender, receiver, token, amount,
        p.actual_chainid AS chain
        FROM (
            SELECT 
            'Poly Network' AS bridge, 'In' AS bridge_type,
            evt_block_time AS dt, evt_tx_hash AS evt_tx_hash,
            NULL AS sender, "toAddress" AS receiver,
            "toAssetHash" AS token, amount, "fromChainId" AS chain
            FROM
            poly_network."LockProxy_evt_UnlockEvent" u
            INNER JOIN poly_network."LockProxy_call_unlock" l 
                ON l."call_tx_hash" = u.evt_tx_hash
            
            UNION ALL
            
            SELECT 
            'Poly Network' AS bridge, 'Out' AS bridge_type,
            evt_block_time AS dt, evt_tx_hash AS evt_tx_hash,
            "fromAddress" AS sender, "toAddress" AS receiver,
            "fromAssetHash" AS token, amount,
            "toChainId" AS chain
            FROM
            poly_network."LockProxy_evt_LockEvent" u
            ) a
        LEFT JOIN dune_user_generated.poly_network_chainids p
            ON p.poly_chainid = a.chain
    )
    
    ,standard_bridges AS
    (
    --https://github.com/ethereum-optimism/ethereum-optimism.github.io/blob/master/optimism.tokenlist.json
    --for chainId 1, the optimismBridgeAddress is the L1 address for depositing
    --for chainid 10, the optimismBridgeAddress is the L2 address for withdrawing
    SELECT * FROM
    (
        --Gateway for SNX
        SELECT
        -- event decoding https://ath.mirror.xyz/mbR1n_CvflL1KIKCTG42bnM4HpfGBqDPNndH8mu2eJw
        'Standard Bridge / Other' AS protocol,
        'In' AS bridge_type,
        l.block_time AS dt, l.tx_hash AS evt_tx_hash,
        substring( l.topic2, 13, 20)::bytea AS sender,
        substring( l.topic2, 13, 20)::bytea AS receiver,
        '\x8700daec35af8ff88c16bdf0418774cb3d7599b4'::bytea AS token,
        bytea2numeric(l.data) AS amount,
        CASE WHEN t."from" = '\x0000000000000000000000000000000000000000' --if from burn address then ethereum, else idk
        THEN 1 ELSE NULL END AS chain 
        FROM optimism.logs l 
        INNER JOIN optimism.transactions t
            ON l."tx_hash" = t.hash
        WHERE l.contract_address = '\x3f87ff1de58128ef8fcb4c807efd776e1ac72e51'
            AND l.topic1 ='\x162eb12ad2bd8b6ca7960f162208414ab3bc2da9f37953788ffd8cf850c3492b' --snx bridge deposit finalized
        GROUP BY 1,2,3,4,5,6,7,8,9 --DISTINCT
        
        UNION ALL
        --Gateway for  tokens and DAI
        SELECT
        -- event decoding https://ath.mirror.xyz/mbR1n_CvflL1KIKCTG42bnM4HpfGBqDPNndH8mu2eJw
        'Standard Bridge / Other' AS protocol,
        'In' AS bridge_type,
        l.block_time AS dt, l.tx_hash AS evt_tx_hash,
        substring(l.topic4, 13, 20)::bytea AS sender,
        substring( decode ( SUBSTRING ( encode(l."data", 'hex') , 1 , 64 ), 'hex'), 13, 20)::bytea AS receiver,
        substring(l.topic3, 13, 20)::bytea AS token,
        bytea2numeric ( decode ( SUBSTRING ( encode(l."data", 'hex') , 65 , 64 ), 'hex')) AS amount,
        CASE WHEN t."from" = '\x0000000000000000000000000000000000000000' --if from burn address then ethereum, else idk
        THEN 1 ELSE NULL END AS chain 
        FROM optimism.logs l 
        INNER JOIN optimism.transactions t
            ON l."tx_hash" = t.hash
        WHERE l.contract_address IN ('\x467194771dae2967aef3ecbedd3bf9a310c76c65','\x4200000000000000000000000000000000000010')
            AND l.topic1 =
                    '\xb0444523268717a02698be47d0803aa7468c00acbed2f8bd93a0459cde61dd89' --dai and others bridge deposit finalized
        GROUP BY 1,2,3,4,5,6,7,8,9 --DISTINCT
        
        UNION ALL
        --use erc20 transfer events for out
        SELECT
        CASE WHEN t."to" = '\x467194771dAe2967Aef3ECbEDD3Bf9a310C76C65' THEN 'Maker'
            ELSE 'Standard Bridge / Other' END AS protocol,
        'Out' AS bridge_type,
        t.block_time AS dt, t.hash AS evt_tx_hash,
        l."from" AS sender, l."from"  AS receiver, l."contract_address" AS token, l.value AS amount,
        1 AS chain -- these are L2->L1 bridges
        FROM erc20."ERC20_evt_Transfer" l
        INNER JOIN optimism.transactions t
            ON l."evt_tx_hash" = t.hash
            WHERE
            t."to" IN ('\x3f87ff1de58128ef8fcb4c807efd776e1ac72e51' --snx
                        ,'\x4200000000000000000000000000000000000010' --others
                        ,'\x467194771dAe2967Aef3ECbEDD3Bf9a310C76C65'--dai
                        )
            /*AND l.contract_address IN ('\x3f87ff1de58128ef8fcb4c807efd776e1ac72e51' --snx
                            ,'\x4200000000000000000000000000000000000010' --others
                            ,'\x467194771dAe2967Aef3ECbEDD3Bf9a310C76C65'--dai
                            )*/
            AND l."to" != '\x4200000000000000000000000000000000000011' --not to OE Fees Addr
        GROUP BY 1,2,3,4,5,6,7,8,9 --DISTINCT
            
        ) a
    WHERE sender != '\x4200000000000000000000000000000000000011' --not to OE Fees Addr
        AND receiver != '\x4200000000000000000000000000000000000011' --not to OE Fees Addr
    )

SELECT *
FROM (
    SELECT all_b.*, ROW_NUMBER() OVER (PARTITION BY evt_tx_hash) AS rn,
    COALESCE(chain_name, CASE WHEN all_b.chain IS NULL THEN 'Unknown' ELSE NULL END, all_b.chain::text,'Other') AS chain_name
    FROM (
        SELECT *, 
        CASE WHEN evt_tx_hash IN (SELECT evt_tx_hash FROM standard_bridges) THEN 1 ELSE 0 END AS is_standard_bridge
        FROM ext_bridges
        
    UNION ALL
        
        SELECT *, 1 AS is_standard_bridge FROM standard_bridges
            WHERE evt_tx_hash NOT IN (SELECT evt_tx_hash FROM ext_bridges)
        ) all_b
    LEFT JOIN dune_user_generated.chain_list cl
        ON all_b.chain = cl.chain_chainid
    ) r
WHERE rn = 1 --just to ensure no duplicates across bridges
    
)
/*
-- We keep Optimism in hourly throughout because we don't have prices in dex.trades
, dex_trades AS ( --only Uniswap now, should have SNX and others later
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
        WHERE f."token0" IN (SELECT token FROM unified_bridges)
            OR f."token1" IN (SELECT token FROM unified_bridges)
)

, gs AS
(
SELECT generate_series(
    DATE_TRUNC('hour',(SELECT MIN(dt) FROM unified_bridges))
    , DATE_TRUNC('hour',(SELECT MAX(dt) FROM unified_bridges))  --doing day so we keep partial weeks/months
    , '1 hour') AS dt
) -- Generate all days since the first day

,bridge_token_price AS (
SELECT
gs.dt,
LP_contract, erc20_token, bridge_token, bridge_symbol, price_ratio, bridge_decimals
FROM (
SELECT *,
lead(dt, 1, now() ) OVER (PARTITION BY bridge_token
                            ORDER BY dt asc) AS next_dt
FROM dune_user_generated.hourly_bridge_token_price_ratios --https://dune.xyz/queries/264679
) f
INNER JOIN gs
ON f.dt <= gs.dt
AND gs.dt < f.next_dt


)

, dex_price AS(
--for tokens where dune doesn't have the price, calculate the avg price in it's last hour of swaps (6 hr if no swaps in the last hour)
--since we don't have an amount field for unmapped tokens, I'll calc it based on... raw amount
-- through a few queries, this gets the unit price of each token (assuming 18 decimals)
SELECT * FROM
(
SELECT
gs.dt,
token, symbol, decimals,
CASE WHEN symbol IN ('USDT','DAI','USDC') THEN 1
ELSE median_price
END AS median_price,
DENSE_RANK() OVER (PARTITION BY token ORDER BY gs.dt DESC) AS hrank
FROM
(
SELECT *,
lead(dt, 1, now() ) OVER (PARTITION BY token
                            ORDER BY dt asc) AS next_dt

FROM
(
SELECT
DATE_TRUNC('hour',block_time) AS dt,
token, symbol, decimals,
percentile_cont(0.5) WITHIN GROUP (ORDER BY token_price) AS median_price
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
        INNER JOIN dune_user_generated.uniswap_v3_pools p ON
        t."exchange_contract_address" = p.pool
        INNER JOIN erc20."tokens" ea --both need to have known decimals, we're not going to assume anything.
        ON ea."contract_address" = t.token_a_address
        INNER JOIN erc20."tokens" eb
        ON eb."contract_address" = t.token_b_address
        WHERE project = 'Uniswap' AND version = '3'
        AND t.block_time >= '2021-07-08'
        --AND 10^decimals > 0
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
        --t.token_a_address,
        t.token_b_address AS token,
        --t.token_a_amount_raw/(10^ea.decimals) AS token_a_amount,
        t.token_b_amount_raw/(10^eb.decimals) AS token_amount,
        CASE WHEN ea.symbol IN ('USDT','DAI','USDC') THEN --assume price = 1
        t.token_a_amount_raw/(10^ea.decimals) ELSE NULL
        END AS usd_amount
        
        FROM dex_trades t
        INNER JOIN dune_user_generated.uniswap_v3_pools p ON
        t."exchange_contract_address" = p.pool
        INNER JOIN erc20."tokens" ea --both need to have known decimals, we're not going to assume anything.
        ON ea."contract_address" = t.token_a_address
        INNER JOIN erc20."tokens" eb
        ON eb."contract_address" = t.token_b_address
        WHERE project = 'Uniswap' AND version = '3'
        AND t.block_time >= '2021-07-08'
        --AND 10^decimals > 0
        AND t.token_b_amount_raw > 100 --min to exclude weird stuff
    ) tokenb
    
) a
WHERE token_price > 0
GROUP BY 1,2,3, 4

UNION ALL

SELECT
    '01-01-2000' AS dt, token::bytea, symbol, decimals, 1 AS median_price
    FROM ( values
            ('\x7f5c764cbc14f9669b88837ca1490cca17c31607','USDC',6)
            ,('\x94b008aa00579c1307b0ef2c499ad98a8ce58e58','USDT',6)
            ,('\xda10009cbd5d07dd0cecc66161fc93d7c9000da1','DAI',18)
        ) t (token, symbol, decimals)

UNION ALL

SELECT
    '11-11-2021' AS dt, token::bytea, symbol, decimals, median_price --prices before uniswap trades
    FROM ( values
            ('\x4200000000000000000000000000000000000006','WETH',18,4750)
            ,('\x68f180fcce6836688e9084f035309e29bf0a2095','WBTC',18,65000)
        ) t (token, symbol, decimals,median_price)

) b
) c
INNER JOIN gs
ON c.dt <= gs.dt
AND gs.dt < c.next_dt
) d

)

, prices AS (
WITH token_prices AS (
    SELECT 
    dt,
    token, symbol, decimals,
    median_price, hrank
    FROM dex_price
    
    UNION ALL -- ETH Price from WETH
    SELECT dt,
    '\xdeaddeaddeaddeaddeaddeaddeaddeaddead0000' AS token, 'ETH' AS symbol, 18 AS decimals,
    CASE WHEN dt < '2021-11-12 01:01' THEN 4749.18275 ELSE median_price END AS median_price, hrank
    FROM dex_price
    WHERE token = '\x4200000000000000000000000000000000000006'
    
    UNION ALL -- ETH Price from WETH
    SELECT dt,
    '\x121ab82b49b2bc4c7901ca46b8277962b4350204' AS token, 'WETH' AS symbol, 18 AS decimals, --Synapse WETH
    CASE WHEN dt < '2021-11-12 01:01' THEN 4749.18275 ELSE median_price END AS median_price, hrank
    FROM dex_price
    WHERE token = '\x4200000000000000000000000000000000000006'
    
    UNION ALL -- ETH Price from WETH
    SELECT dt,
    '\x1259adc9f2a0410d0db5e226563920a2d49f4454' AS token, 'WETH' AS symbol, 18 AS decimals, --Synapse WETH
    CASE WHEN dt < '2021-11-12 01:01' THEN 4749.18275 ELSE median_price END AS median_price, hrank
    FROM dex_price
    WHERE token = '\x4200000000000000000000000000000000000006'
    
    )
    SELECT * FROM token_prices
    UNION ALL
    
    SELECT --bridge_token prices
    dt,
    bridge_token AS token, bridge_symbol AS symbol, bridge_decimals AS decimals,
    median_price * price_ratio AS median_price,
    DENSE_RANK() OVER (PARTITION BY bridge_token ORDER BY dt DESC) AS hrank
    FROM (
        SELECT 
        h.dt,
        LP_contract, erc20_token, bridge_token, bridge_symbol, price_ratio,
        h.bridge_decimals, p.median_price
        FROM bridge_token_price h
        INNER JOIN token_prices p
        ON h.erc20_token = p.token
        AND h.dt = p.dt
        ) h
)*/

SELECT
gb.dt, evt_tx_hash,
chain, chain_name,
protocol,
gb.token,
--migrate all WETH to ETH
-- CASE WHEN gb.token IN ('\x4200000000000000000000000000000000000006',
--                         '\x121ab82b49b2bc4c7901ca46b8277962b4350204',
--                         '\x1259adc9f2a0410d0db5e226563920a2d49f4454'
--                         )

--     THEN 'WETH' ELSE 
    COALESCE(e.symbol,dp.symbol,REPLACE(gb.token::text,'\x','0x')) 
    --END 
    AS symbol,
bridge_type,
amount/(10^COALESCE(e.decimals,dp.decimals)) AS num_tokens,
dp.median_price*amount/(10^COALESCE(e.decimals,dp.decimals)) AS amount_usd,
sender, receiver,
is_standard_bridge

FROM unified_bridges gb

LEFT JOIN erc20."tokens" e
    ON e."contract_address" = gb.token

LEFT JOIN dune_user_generated."new_hourly_dex_prices" dp
    ON DATE_TRUNC('hour',gb.dt) = dp.hour
    AND gb.token = dp.token
--WHERE chain != 10 OR chain IS NULL--weird.... not sure why some are internal optimism transfers
