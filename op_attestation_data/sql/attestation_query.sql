with base as (
        select *,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data
        from optimism.core.fact_event_logs
        where block_timestamp > '2022-12-14'
        and contract_address = '0xee36eaad94d1cc1d0eccadb55c38bffb6be06c77' 
        and topics[0]::string = '0x28710dfecab43d1e29e02aa56b2e1e610c0bae19135c9cf7a83a1adb6df96d85'
)
, decoded AS (
        select 
        block_number,
        block_timestamp,
        tx_hash,
        origin_from_address,
        origin_to_address,
        event_index,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS creator,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS about,
        replace(topics [3] :: STRING,'0x','') as key,
        try_hex_decode_string(key::string) as decoded_key,
        substr(data::string,131,(ethereum.public.udf_hex_to_int(segmented_data[1]::string) * 2)) as val,
        try_hex_decode_string(val::string) as val_text
        -- segmented_data[1]::string as val_text
  from base 
  )
SELECT 
  decoded_key, val, val_text, about, 
  block_timestamp, creator, tx_hash, 
  origin_from_address, origin_to_address
FROM decoded
ORDER BY block_timestamp DESC