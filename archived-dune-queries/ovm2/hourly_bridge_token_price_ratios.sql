--drop view if exists dune_user_generated.hourly_bridge_token_price_ratios;
CREATE OR REPLACE view dune_user_generated.hourly_bridge_token_price_ratios AS
WITH bridge_tokens AS (
    SELECT --hop tokens
    DATE_TRUNC('hour',evt_block_time) AS dt,
        LP_contract, erc20_token, bridge_token, bridge_symbol, bridge_decimals,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY 
            ratio
            ) AS price_ratio,
            COUNT(*) AS num_samples
    FROM (
    SELECT
        s.evt_block_time,
        LP_contract, erc20_token, bridge_token, bridge_symbol, bridge_decimals,
        CASE WHEN "boughtId" = 0 THEN "tokensBought"::decimal/"tokensSold"::decimal --if buy bridge_token then buys per sells is bridge_token price
                ELSE "tokensSold"::decimal/"tokensBought"::decimal
                END
                AS ratio
        FROM hop_protocol."Swap_evt_TokenSwap" s
        INNER JOIN
            ( --map hToken to ERC20
            WITH e AS (
                SELECT al.contract_address AS LP_contract, e.contract_address AS token, t.symbol, t.decimals FROM hop_protocol."Swap_evt_AddLiquidity" al
                INNER JOIN erc20."ERC20_evt_Transfer" e
                    ON e.evt_tx_hash = al.evt_tx_hash
                INNER JOIN erc20."tokens" t
                    ON e.contract_address = t.contract_address
                WHERE (
                        LEFT(t.symbol,1) != 'h' --t is not h
                        )
                 AND LEFT(t.symbol,4) != 'HOP-'
                 GROUP BY 1,2,3,4
                )
            ,h AS (
            SELECT al.contract_address AS LP_contract, ht.contract_address AS token, ht.symbol, ht.decimals FROM hop_protocol."Swap_evt_AddLiquidity" al
                INNER JOIN erc20."ERC20_evt_Transfer" e
                    ON e.evt_tx_hash = al.evt_tx_hash
                INNER JOIN erc20."tokens" ht
                    ON e.contract_address = ht.contract_address
                WHERE (
                        LEFT(ht.symbol,1) = 'h' --ht is h
                        )
                 AND LEFT(ht.symbol,4) != 'HOP-'
                 GROUP BY 1,2,3,4
                )
            SELECT e.LP_contract, e.token AS erc20_token, e.symbol AS erc20_symbol,
                                h.token AS bridge_token, h.symbol AS bridge_symbol, h.decimals AS bridge_decimals
            FROM e INNER JOIN h ON e.LP_contract = h.LP_contract
             ) m
    ON m.LP_contract = s."contract_address"
    GROUP BY 1,2,3,4,5,6,7
    
    UNION ALL
--Synapse Tokens
        SELECT
        s.evt_block_time, NULL::bytea AS LP_contract, t.contract_address AS erc20_token, s.token AS bridge_token,
        et.symbol AS bridge_symbol, et.decimals AS bridge_decimals,
        t.value::decimal/s.amount::decimal AS ratio
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
        INNER JOIN erc20."ERC20_evt_Transfer" t
            ON t.evt_tx_hash = s.evt_tx_hash
            AND t.contract_address != s.token
        INNER JOIN erc20."tokens" et
            ON et."contract_address" = s.token
    ) n
GROUP BY 1,2,3,4,5,6

)

SELECT dt, LP_contract,
CASE WHEN erc20_token IN ('\x121ab82b49b2bc4c7901ca46b8277962b4350204','\x1259adc9f2a0410d0db5e226563920a2d49f4454') --other WETHS
THEN '\x4200000000000000000000000000000000000006'
ELSE erc20_token END AS erc20_token

,bridge_token, bridge_symbol, bridge_decimals,price_ratio, num_samples

FROM bridge_tokens
