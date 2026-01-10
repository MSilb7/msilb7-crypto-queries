--drop view if exists dune_user_generated.hourly_synth_prices;
CREATE OR REPLACE view dune_user_generated.hourly_synth_prices AS

--Seems like there isn't a set swap event, so we take tokens to and tokens from the sender within "trade" transactions to calculate the exchange rate.
WITH token_sent AS (
    SELECT evt_block_time, evt_tx_hash, r.contract_address AS token, r.value, decimals, symbol FROM optimism.transactions t
        INNER JOIN erc20."ERC20_evt_Transfer" r
            ON t.hash = r.evt_tx_hash
            AND t."from" = r."from"
        INNER JOIN erc20."tokens" e
            ON e."contract_address" = r."contract_address"
        WHERE t."to" = '\x8700daec35af8ff88c16bdf0418774cb3d7599b4'
        AND substring(data from 1 for 4) = '\x30ead760' --methodid

    )
, token_received AS (
    SELECT evt_block_time, evt_tx_hash, r.contract_address AS token, r.value, decimals, symbol FROM optimism.transactions t
        INNER JOIN erc20."ERC20_evt_Transfer" r
            ON t.hash = r.evt_tx_hash
            AND t."from" = r."to"
        INNER JOIN erc20."tokens" e
            ON e."contract_address" = r."contract_address"
        WHERE t."to" = '\x8700daec35af8ff88c16bdf0418774cb3d7599b4'
        AND substring(data from 1 for 4) = '\x30ead760' --methodid
        
    )
    /*, susd_fees AS ( --the user pays the fees, but this shouldn't go in to the price conversion, so we subtract from the sUSD total
    SELECT evt_block_time, evt_tx_hash, r.contract_address AS token, r.value, decimals, symbol FROM erc20."ERC20_evt_Transfer" r
        INNER JOIN optimism.transactions t
            ON t.hash = r.evt_tx_hash
        INNER JOIN erc20."tokens" e
            ON e."contract_address" = r."contract_address"
        WHERE t."to" = '\x8700daec35af8ff88c16bdf0418774cb3d7599b4'
        AND substring(data from 1 for 4) = '\x30ead760' --methodid
        AND r."from" = '\x0000000000000000000000000000000000000000'
        AND r."to" = '\xfeefeefeefeefeefeefeefeefeefeefeefeefeef'
        AND r."contract_address" = '\x8c6f28f2f1a3c87f0f938b96d27520d9751ec8d9'
        
    )*/
, ratios AS (
    SELECT
    s.evt_block_time, s.evt_tx_hash, s.decimals AS s_decimals, r.decimals AS r_decimals,
    r.token AS r_token, r.symbol AS r_symbol, s.token AS s_token, s.symbol AS s_symbol,
    CASE WHEN r.token = '\x8c6f28f2f1a3c87f0f938b96d27520d9751ec8d9' THEN
        ((r.value/*-f.value*/)/(10^r.decimals))::decimal/(s.value/(10^s.decimals))::decimal
    WHEN s.token = '\x8c6f28f2f1a3c87f0f938b96d27520d9751ec8d9' THEN
        ((s.value/*-f.value*/)/(10^s.decimals))::decimal/(r.value/(10^r.decimals))::decimal 
    END AS price_ratio,
    CASE WHEN r.token = '\x8c6f28f2f1a3c87f0f938b96d27520d9751ec8d9' THEN 'sent' --sent token, received sUSD
        WHEN s.token = '\x8c6f28f2f1a3c87f0f938b96d27520d9751ec8d9' THEN 'received' -- received token, sent sUSD
        ELSE '0' --ignore
        END AS token_side
    FROM token_sent s
    INNER JOIN token_received r
        ON s.evt_tx_hash = r.evt_tx_hash
    /*INNER JOIN susd_fees f
        ON s.evt_tx_hash = f.evt_tx_hash
        AND r.evt_tx_hash = f.evt_tx_hash*/
    )
    
SELECT *
FROM (
    SELECT
    DATE_TRUNC('hour',evt_block_time) AS hour,
    CASE WHEN token_side = 'sent' THEN s_token ELSE r_token END AS token,
    CASE WHEN token_side = 'sent' THEN s_symbol ELSE r_symbol END AS symbol,
    CASE WHEN token_side = 'sent' THEN s_decimals ELSE r_decimals END AS decimals,
    
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_ratio) AS median_price, -- already flipped
    COUNT(*) AS num_samples
    
    FROM ratios
    WHERE token_side != '0'
    GROUP BY 1,2,3,4
    
    ) pps
