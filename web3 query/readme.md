# L2 Gas Price Comparison Queries
- **indexing_functions.py**: Uses the Etherscan API to pull the latest block, block info, and getting a block by timestamp (i.e. what block was 3 days ago)
- **Tx fee comps.ipynb**: Super beginner-level code. Uses the web3 package to pull a transaction list from the Etherscan API. Since L2s use distinct fields to calulate gas prices (i.e. L1 fee vs L2 fee), this then uses web3.py to pull transaction and transcation_receipt data for each transaction *(this takes a long time - TODO: Figure out how to batch these like ethereum-etl).* 
