# Optimism AttestationStation Data
`pull_attestations_shroomdk.ipynb` uses Flipside Crypto data and Flipside's [ShroomDK (SDK)](https://sdk.flipsidecrypto.xyz/shroomdk) to query all on-chain attestations made on the [AttestationStation](https://community.optimism.io/docs/governance/attestation-station/) deployed on [Optimism](https://www.optimism.io/).

Learn more about the vision behind the AttestationStation and Optimism's approach to on-chain identity in the [Optimistic Dev Blog: Making Blockchains Human-friendly](https://dev.optimism.io/making-blockchains-human-friendly/)

# How to Pull All Attestation Station Data
Once you have a ShroomDK API Key (requires NFT mint), store in your .env as:
` FLIPSIDE_SHROOMDK_KEY = 'Your API Key' `

From there, you can run `pull_attestations_shroomdk.ipynb` in jupyter notebooks. This will query for all attestations in Flipside's databse, and store results in a file `attestation_data` csv, with today's date appended to the file name.

To make any modifications to the query (i.e. specific attestations, set date ranges), modify the query in `sql/attestation_query.sql`

Note: These csvs will be large, so you can add something like `op_attestation_data/*.csv` to your `.gitignore`.

## Data Schema
All attestations are stored as key : value pairs ([see the code](https://community.optimism.io/docs/governance/attestation-station/#attestationcreated)).

- **decoded_key:** The key of the attestation
- **val:** The value of the attestation (numeric).
- **val_text:** The value of the attestation (in text).
*Note: Since vlaues can be stored as either values or text, it's up to the analyst or reader to pick the appropriate decoding*
- **about:** What address the attestation is being made about
- **block_timestamp:** What is the timestamp of the block where the attestation was made.
- **creator:** What address created the attestation (think: Is this sender trustworthy, or may this be spam?)
- **tx_hash:** The transaction hash of the transaction where the attestation was made.
- **origin_from_address:** The sender (from) address of the transaction where the attestation was made.
- **origin_to_address:** The contract (to) address of the transaction where the attestation was made.

## Other References
- AttestationStation Contract Address: [`0xEE36eaaD94d1Cc1d0eccaDb55C38bFfB6Be06C77`](https://optimistic.etherscan.io/address/0xEE36eaaD94d1Cc1d0eccaDb55C38bFfB6Be06C77)
- [Flipside SDK Docs](https://docs.flipsidecrypto.com/shroomdk-sdk/get-started)
- [Flipside SDK Examples](https://github.com/FlipsideCrypto/sdk)
