from flask import Flask, request, jsonify
from web3 import Web3
from web3.middleware import geth_poa_middleware
import pandas as pd
import json
from functools import cache
import threading 
import queue
import time
from concurrent.futures import ThreadPoolExecutor

app = Flask(__name__)

# Replace with the actual Optimism RPC URL
# optimism_rpc_url = 'https://linea-mainnet.infura.io/v3/e2b4d9fa19c748489fb6c0d6bf411be4'
optimism_rpc_url = 'https://andromeda.metis.io'

# Create a Web3 instance to connect to the Optimism blockchain
web3 = Web3(Web3.HTTPProvider(optimism_rpc_url))
web3.middleware_onion.inject(geth_poa_middleware, layer=0)

LATEST_BLOCK = web3.eth.get_block_number()
# FROM_BLOCK = 1070504 - 1000
FROM_BLOCK = 0

# Replace with the actual Aave V2 contract address
# contract_address = "0x871AfF0013bE6218B61b28b274a6F53DB131795F"


#gets how many decimals our reserve is
def get_reserve_decimals(reserve_address):
    decimals = 0
    if reserve_address == '0xEA32A96608495e54156Ae48931A7c20f0dcc1a21': # usdc
        decimals = 1e6
    elif reserve_address == '0xbB06DCA3AE6887fAbF931640f67cab3e3a16F4dC': # usdt
        decimals = 1e6
    elif reserve_address == '0x433E43047B95cB83517abd7c9978Bdf7005E9938': # wbtc
        decimals = 1e8
    elif reserve_address == '0x420000000000000000000000000000000000000A': # weth
        decimals = 1e18
    elif reserve_address == '0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000': # metis
        decimals = 1e8
    
    return decimals
#gets our reserve price
#@cache
def get_tx_usd_amount(reserve_address, token_amount):
    contract_address = '0xfAF84D4679A5313Ed6d6484316f2401BEdD669Ea'
    contract_abi = [{"inputs":[{"internalType":"address[]","name":"assets","type":"address[]"},{"internalType":"address[]","name":"sources","type":"address[]"},{"internalType":"address","name":"fallbackOracle","type":"address"},{"internalType":"address","name":"baseCurrency","type":"address"},{"internalType":"uint256","name":"baseCurrencyUnit","type":"uint256"}],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"asset","type":"address"},{"indexed":True,"internalType":"address","name":"source","type":"address"}],"name":"AssetSourceUpdated","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"baseCurrency","type":"address"},{"indexed":False,"internalType":"uint256","name":"baseCurrencyUnit","type":"uint256"}],"name":"BaseCurrencySet","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"fallbackOracle","type":"address"}],"name":"FallbackOracleUpdated","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":True,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"inputs":[],"name":"BASE_CURRENCY","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"BASE_CURRENCY_UNIT","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"}],"name":"getAssetPrice","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address[]","name":"assets","type":"address[]"}],"name":"getAssetsPrices","outputs":[{"internalType":"uint256[]","name":"","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getFallbackOracle","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"}],"name":"getSourceOfAsset","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"renounceOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address[]","name":"assets","type":"address[]"},{"internalType":"address[]","name":"sources","type":"address[]"}],"name":"setAssetSources","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"fallbackOracle","type":"address"}],"name":"setFallbackOracle","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"}]
    contract = web3.eth.contract(address=contract_address, abi=contract_abi)
    value_usd = contract.functions.getAssetPrice(reserve_address).call()
    decimals = get_reserve_decimals(reserve_address)
    usd_amount = (value_usd/1e18)*(token_amount/decimals)
    # print(usd_amount)
    return usd_amount

# # gets a unique list of users from our all_events.csv file
# # returns a list of unique users
def get_unique_users():

    df = pd.read_csv('all_events.csv')
    df = df.drop_duplicates(subset='wallet_address')

    unique_user_list = df['wallet_address'].tolist()

    return unique_user_list

def get_unclaimed_metis_amount():
    contract_address = '0xd93e25a8b1d645b15f8c736e1419b4819ff9e6ef'
    contract_address = web3.to_checksum_address(contract_address)
    contract_abi = [ { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "asset", "type": "address" }, { "indexed": True, "internalType": "address", "name": "reward", "type": "address" }, { "indexed": False, "internalType": "uint256", "name": "emission", "type": "uint256" }, { "indexed": False, "internalType": "uint256", "name": "distributionEnd", "type": "uint256" } ], "name": "AssetConfigUpdated", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "asset", "type": "address" }, { "indexed": True, "internalType": "address", "name": "reward", "type": "address" }, { "indexed": False, "internalType": "uint256", "name": "index", "type": "uint256" } ], "name": "AssetIndexUpdated", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "user", "type": "address" }, { "indexed": True, "internalType": "address", "name": "claimer", "type": "address" } ], "name": "ClaimerSet", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "previousOwner", "type": "address" }, { "indexed": True, "internalType": "address", "name": "newOwner", "type": "address" } ], "name": "OwnershipTransferred", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "user", "type": "address" }, { "indexed": True, "internalType": "address", "name": "reward", "type": "address" }, { "indexed": False, "internalType": "uint256", "name": "amount", "type": "uint256" } ], "name": "RewardsAccrued", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "user", "type": "address" }, { "indexed": True, "internalType": "address", "name": "reward", "type": "address" }, { "indexed": True, "internalType": "address", "name": "to", "type": "address" }, { "indexed": False, "internalType": "address", "name": "claimer", "type": "address" }, { "indexed": False, "internalType": "uint256", "name": "amount", "type": "uint256" } ], "name": "RewardsClaimed", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "vault", "type": "address" } ], "name": "RewardsVaultUpdated", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "user", "type": "address" }, { "indexed": True, "internalType": "address", "name": "asset", "type": "address" }, { "indexed": True, "internalType": "address", "name": "reward", "type": "address" }, { "indexed": False, "internalType": "uint256", "name": "index", "type": "uint256" } ], "name": "UserIndexUpdated", "type": "event" }, { "inputs": [ { "internalType": "address[]", "name": "assets", "type": "address[]" }, { "internalType": "address", "name": "to", "type": "address" } ], "name": "claimAllRewards", "outputs": [ { "internalType": "address[]", "name": "rewardTokens", "type": "address[]" }, { "internalType": "uint256[]", "name": "claimedAmounts", "type": "uint256[]" } ], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address[]", "name": "assets", "type": "address[]" }, { "internalType": "address", "name": "user", "type": "address" }, { "internalType": "address", "name": "to", "type": "address" } ], "name": "claimAllRewardsOnBehalf", "outputs": [ { "internalType": "address[]", "name": "rewardTokens", "type": "address[]" }, { "internalType": "uint256[]", "name": "claimedAmounts", "type": "uint256[]" } ], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address[]", "name": "assets", "type": "address[]" } ], "name": "claimAllRewardsToSelf", "outputs": [ { "internalType": "address[]", "name": "rewardTokens", "type": "address[]" }, { "internalType": "uint256[]", "name": "claimedAmounts", "type": "uint256[]" } ], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address[]", "name": "assets", "type": "address[]" }, { "internalType": "uint256", "name": "amount", "type": "uint256" }, { "internalType": "address", "name": "to", "type": "address" }, { "internalType": "address", "name": "reward", "type": "address" } ], "name": "claimRewards", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address[]", "name": "assets", "type": "address[]" }, { "internalType": "uint256", "name": "amount", "type": "uint256" }, { "internalType": "address", "name": "user", "type": "address" }, { "internalType": "address", "name": "to", "type": "address" }, { "internalType": "address", "name": "reward", "type": "address" } ], "name": "claimRewardsOnBehalf", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address[]", "name": "assets", "type": "address[]" }, { "internalType": "uint256", "name": "amount", "type": "uint256" }, { "internalType": "address", "name": "reward", "type": "address" } ], "name": "claimRewardsToSelf", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "components": [ { "internalType": "uint88", "name": "emissionPerSecond", "type": "uint88" }, { "internalType": "uint256", "name": "totalSupply", "type": "uint256" }, { "internalType": "uint32", "name": "distributionEnd", "type": "uint32" }, { "internalType": "address", "name": "asset", "type": "address" }, { "internalType": "address", "name": "reward", "type": "address" } ], "internalType": "struct DistributionTypes.RewardsConfigInput[]", "name": "config", "type": "tuple[]" } ], "name": "configureAssets", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address[]", "name": "assets", "type": "address[]" }, { "internalType": "address", "name": "user", "type": "address" } ], "name": "getAllUserRewardsBalance", "outputs": [ { "internalType": "address[]", "name": "rewardTokens", "type": "address[]" }, { "internalType": "uint256[]", "name": "unclaimedAmounts", "type": "uint256[]" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" } ], "name": "getAssetDecimals", "outputs": [ { "internalType": "uint8", "name": "", "type": "uint8" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "user", "type": "address" } ], "name": "getClaimer", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" }, { "internalType": "address", "name": "reward", "type": "address" } ], "name": "getDistributionEnd", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "getRewardTokens", "outputs": [ { "internalType": "address[]", "name": "", "type": "address[]" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" } ], "name": "getRewardsByAsset", "outputs": [ { "internalType": "address[]", "name": "", "type": "address[]" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" }, { "internalType": "address", "name": "reward", "type": "address" } ], "name": "getRewardsData", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" }, { "internalType": "uint256", "name": "", "type": "uint256" }, { "internalType": "uint256", "name": "", "type": "uint256" }, { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "reward", "type": "address" } ], "name": "getRewardsVault", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "user", "type": "address" }, { "internalType": "address", "name": "asset", "type": "address" }, { "internalType": "address", "name": "reward", "type": "address" } ], "name": "getUserAssetData", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address[]", "name": "assets", "type": "address[]" }, { "internalType": "address", "name": "user", "type": "address" }, { "internalType": "address", "name": "reward", "type": "address" } ], "name": "getUserRewardsBalance", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "user", "type": "address" }, { "internalType": "address", "name": "reward", "type": "address" } ], "name": "getUserUnclaimedRewardsFromStorage", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "user", "type": "address" }, { "internalType": "uint256", "name": "totalSupply", "type": "uint256" }, { "internalType": "uint256", "name": "userBalance", "type": "uint256" } ], "name": "handleAction", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [], "name": "owner", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "renounceOwnership", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "user", "type": "address" }, { "internalType": "address", "name": "caller", "type": "address" } ], "name": "setClaimer", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" }, { "internalType": "address", "name": "reward", "type": "address" }, { "internalType": "uint32", "name": "distributionEnd", "type": "uint32" } ], "name": "setDistributionEnd", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "vault", "type": "address" }, { "internalType": "address", "name": "reward", "type": "address" } ], "name": "setRewardsVault", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "newOwner", "type": "address" } ], "name": "transferOwnership", "outputs": [], "stateMutability": "nonpayable", "type": "function" } ]


    collateral_list = ['0x37fa438edfb7044e9444b4022b2516c4daa4592f', '0x18ba3e87876f4982810d321d447b81d01cdf6668', '0x826ed083724909196e6598452be4fdfe0fa6c7cd', '0x73d49ac28c4fea2b8e7c6bf45d64a2e68ed53be0', '0x7f5ec43a46df54471dae95d3c05bebe7301b75ff']
    reward_list = ['0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000', '0xe1537fef008944d1c8dcafbace4dc76d31d22dc5']
    user_address_list = get_unique_users()

    # user_address_list = ['0xb9D314FB81Efe57eEf6dBd40c3c379e19e7f6EaE']
    #checksums our addresses
    collateral_list = [web3.to_checksum_address(x) for x in collateral_list]
    reward_list = [web3.to_checksum_address(x) for x in reward_list]
    user_address_list = [web3.to_checksum_address(x) for x in user_address_list]

    contract = web3.eth.contract(address=contract_address, abi=contract_abi)


    user_list = []
    unclaimed_reward_list = []
    unclaimed_amount_list = []

    i = 0
    #loops through all users, collaterals, and rewards
    for user in user_address_list:
        print(i, '/', len(user_address_list))
        i += 1
        #finds out how much unclaimed rewards they have
        unclaimed_reward_data = contract.functions.getAllUserRewardsBalance(collateral_list, user).call()
        
        if len(unclaimed_reward_data[1]) > 1:
            print(unclaimed_reward_data[1])

        reward_token = unclaimed_reward_data[0][0]
        reward_amount = unclaimed_reward_data[1][0]

        # print(reward_token, reward_amount)

        #adds info to lists if they have any rewards
        if reward_amount > 0:
            user_list.append(user)
            unclaimed_reward_list.append(reward_token)
            unclaimed_amount_list.append(reward_amount)
    
    df = pd.DataFrame()
    df['wallet_address'] = user_list
    df['unclaimed_reward_token'] = unclaimed_reward_list
    df['unclaimed_quantity_raw'] = unclaimed_amount_list
    
    df['total_deployment_unclaimed_rewards_raw'] = df['unclaimed_quantity_raw']
    df['total_deployment_unclaimed_rewards_raw'] = sum(df['total_deployment_unclaimed_rewards_raw'])
    
    df.to_csv('deployment_rewards.csv', index=False)

    return df

#cleans up our dataframe
def df_cleanup():
    df = pd.read_csv('deployment_rewards.csv')

    df['unclaimed_quantity_cleaned'] = df['unclaimed_quantity_raw'].astype(float) / 1e18
    df['total_deployment_unclaimed_rewards_cleaned'] = df['total_deployment_unclaimed_rewards_raw'].astype(float) / 1e18

    df.to_csv('deployment_rewards.csv', index=False)
    return
#gets our web3 contract object
# @cache
def get_contract():
    contract_address = "0x65dEc665ea1e96Ee5203DB321b5Cd413b81B2bd2"
    contract_abi = [{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"reserve","type":"address"},{"indexed":False,"internalType":"address","name":"user","type":"address"},{"indexed":True,"internalType":"address","name":"onBehalfOf","type":"address"},{"indexed":False,"internalType":"uint256","name":"amount","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"borrowRateMode","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"borrowRate","type":"uint256"},{"indexed":True,"internalType":"uint16","name":"referral","type":"uint16"}],"name":"Borrow","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"reserve","type":"address"},{"indexed":False,"internalType":"address","name":"user","type":"address"},{"indexed":True,"internalType":"address","name":"onBehalfOf","type":"address"},{"indexed":False,"internalType":"uint256","name":"amount","type":"uint256"},{"indexed":True,"internalType":"uint16","name":"referral","type":"uint16"}],"name":"Deposit","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"target","type":"address"},{"indexed":True,"internalType":"address","name":"initiator","type":"address"},{"indexed":True,"internalType":"address","name":"asset","type":"address"},{"indexed":False,"internalType":"uint256","name":"amount","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"premium","type":"uint256"},{"indexed":False,"internalType":"uint16","name":"referralCode","type":"uint16"}],"name":"FlashLoan","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"collateralAsset","type":"address"},{"indexed":True,"internalType":"address","name":"debtAsset","type":"address"},{"indexed":True,"internalType":"address","name":"user","type":"address"},{"indexed":False,"internalType":"uint256","name":"debtToCover","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"liquidatedCollateralAmount","type":"uint256"},{"indexed":False,"internalType":"address","name":"liquidator","type":"address"},{"indexed":False,"internalType":"bool","name":"receiveAToken","type":"bool"}],"name":"LiquidationCall","type":"event"},{"anonymous":False,"inputs":[],"name":"Paused","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"reserve","type":"address"},{"indexed":True,"internalType":"address","name":"user","type":"address"}],"name":"RebalanceStableBorrowRate","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"reserve","type":"address"},{"indexed":True,"internalType":"address","name":"user","type":"address"},{"indexed":True,"internalType":"address","name":"repayer","type":"address"},{"indexed":False,"internalType":"uint256","name":"amount","type":"uint256"}],"name":"Repay","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"reserve","type":"address"},{"indexed":False,"internalType":"uint256","name":"liquidityRate","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"stableBorrowRate","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"variableBorrowRate","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"liquidityIndex","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"variableBorrowIndex","type":"uint256"}],"name":"ReserveDataUpdated","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"reserve","type":"address"},{"indexed":True,"internalType":"address","name":"user","type":"address"}],"name":"ReserveUsedAsCollateralDisabled","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"reserve","type":"address"},{"indexed":True,"internalType":"address","name":"user","type":"address"}],"name":"ReserveUsedAsCollateralEnabled","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"reserve","type":"address"},{"indexed":True,"internalType":"address","name":"user","type":"address"},{"indexed":False,"internalType":"uint256","name":"rateMode","type":"uint256"}],"name":"Swap","type":"event"},{"anonymous":False,"inputs":[],"name":"Unpaused","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"reserve","type":"address"},{"indexed":True,"internalType":"address","name":"user","type":"address"},{"indexed":True,"internalType":"address","name":"to","type":"address"},{"indexed":False,"internalType":"uint256","name":"amount","type":"uint256"}],"name":"Withdraw","type":"event"},{"inputs":[],"name":"FLASHLOAN_PREMIUM_TOTAL","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"LENDINGPOOL_REVISION","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MAX_NUMBER_RESERVES","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MAX_STABLE_RATE_BORROW_SIZE_PERCENT","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"interestRateMode","type":"uint256"},{"internalType":"uint16","name":"referralCode","type":"uint16"},{"internalType":"address","name":"onBehalfOf","type":"address"}],"name":"borrow","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"address","name":"onBehalfOf","type":"address"},{"internalType":"uint16","name":"referralCode","type":"uint16"}],"name":"deposit","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"},{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"balanceFromBefore","type":"uint256"},{"internalType":"uint256","name":"balanceToBefore","type":"uint256"}],"name":"finalizeTransfer","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"receiverAddress","type":"address"},{"internalType":"address[]","name":"assets","type":"address[]"},{"internalType":"uint256[]","name":"amounts","type":"uint256[]"},{"internalType":"uint256[]","name":"modes","type":"uint256[]"},{"internalType":"address","name":"onBehalfOf","type":"address"},{"internalType":"bytes","name":"params","type":"bytes"},{"internalType":"uint16","name":"referralCode","type":"uint16"}],"name":"flashLoan","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"getAddressesProvider","outputs":[{"internalType":"contract ILendingPoolAddressesProvider","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"}],"name":"getConfiguration","outputs":[{"components":[{"internalType":"uint256","name":"data","type":"uint256"}],"internalType":"struct DataTypes.ReserveConfigurationMap","name":"","type":"tuple"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"}],"name":"getReserveData","outputs":[{"components":[{"components":[{"internalType":"uint256","name":"data","type":"uint256"}],"internalType":"struct DataTypes.ReserveConfigurationMap","name":"configuration","type":"tuple"},{"internalType":"uint128","name":"liquidityIndex","type":"uint128"},{"internalType":"uint128","name":"variableBorrowIndex","type":"uint128"},{"internalType":"uint128","name":"currentLiquidityRate","type":"uint128"},{"internalType":"uint128","name":"currentVariableBorrowRate","type":"uint128"},{"internalType":"uint128","name":"currentStableBorrowRate","type":"uint128"},{"internalType":"uint40","name":"lastUpdateTimestamp","type":"uint40"},{"internalType":"address","name":"aTokenAddress","type":"address"},{"internalType":"address","name":"stableDebtTokenAddress","type":"address"},{"internalType":"address","name":"variableDebtTokenAddress","type":"address"},{"internalType":"address","name":"interestRateStrategyAddress","type":"address"},{"internalType":"uint8","name":"id","type":"uint8"}],"internalType":"struct DataTypes.ReserveData","name":"","type":"tuple"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"}],"name":"getReserveNormalizedIncome","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"}],"name":"getReserveNormalizedVariableDebt","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getReservesList","outputs":[{"internalType":"address[]","name":"","type":"address[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"getUserAccountData","outputs":[{"internalType":"uint256","name":"totalCollateralETH","type":"uint256"},{"internalType":"uint256","name":"totalDebtETH","type":"uint256"},{"internalType":"uint256","name":"availableBorrowsETH","type":"uint256"},{"internalType":"uint256","name":"currentLiquidationThreshold","type":"uint256"},{"internalType":"uint256","name":"ltv","type":"uint256"},{"internalType":"uint256","name":"healthFactor","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"getUserConfiguration","outputs":[{"components":[{"internalType":"uint256","name":"data","type":"uint256"}],"internalType":"struct DataTypes.UserConfigurationMap","name":"","type":"tuple"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"},{"internalType":"address","name":"aTokenAddress","type":"address"},{"internalType":"address","name":"stableDebtAddress","type":"address"},{"internalType":"address","name":"variableDebtAddress","type":"address"},{"internalType":"address","name":"interestRateStrategyAddress","type":"address"}],"name":"initReserve","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"contract ILendingPoolAddressesProvider","name":"provider","type":"address"}],"name":"initialize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"collateralAsset","type":"address"},{"internalType":"address","name":"debtAsset","type":"address"},{"internalType":"address","name":"user","type":"address"},{"internalType":"uint256","name":"debtToCover","type":"uint256"},{"internalType":"bool","name":"receiveAToken","type":"bool"}],"name":"liquidationCall","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"paused","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"},{"internalType":"address","name":"user","type":"address"}],"name":"rebalanceStableBorrowRate","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"rateMode","type":"uint256"},{"internalType":"address","name":"onBehalfOf","type":"address"}],"name":"repay","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"},{"internalType":"uint256","name":"configuration","type":"uint256"}],"name":"setConfiguration","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bool","name":"val","type":"bool"}],"name":"setPause","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"},{"internalType":"address","name":"rateStrategyAddress","type":"address"}],"name":"setReserveInterestRateStrategyAddress","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"},{"internalType":"bool","name":"useAsCollateral","type":"bool"}],"name":"setUserUseReserveAsCollateral","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"},{"internalType":"uint256","name":"rateMode","type":"uint256"}],"name":"swapBorrowRateMode","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"address","name":"to","type":"address"}],"name":"withdraw","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"}]
    # Create contract instance
    contract = web3.eth.contract(address=contract_address, abi=contract_abi)

    return contract

# print(get_contract())

#returns our lists
def user_data(user_address, events, enum_name):
    df = pd.DataFrame()

    user_address_list = []
    tx_hash_list = []
    timestamp_list = []
    token_address_list = []
    token_volume_list = []
    token_usd_amount_list = []
    lend_borrow_type_list = []

    user = 'user'

    # start_time = time.time()
    for event in events:


        payload_address = event['args'][user].lower()
        tx_hash = event['transactionHash'].hex()
        
        if payload_address.lower() == '0x4d8d90FAF90405b9743Ce600E98A2Aa8CdF579a0'.lower():
            if enum_name == 'LEND' or enum_name == 'BORROW':
                user = 'onBehalfOf'
                payload_address = event['args'][user].lower()


        # block = web3.eth.get_block(event['blockNumber'])
        # if block['timestamp'] >= 1701086400:
        if enum_name != 'COLLATERALISE':
            if payload_address == user_address:
                block = web3.eth.get_block(event['blockNumber'])

                user_address_list.append(payload_address)
                tx_hash_list.append(tx_hash)
                timestamp_list.append(block['timestamp'])
                token_address_list.append(event['args']['reserve'])
                token_volume_list.append(event['args']['amount'])
                token_usd_amount_list.append(get_tx_usd_amount(event['args']['reserve'], (event['args']['amount'])))
                lend_borrow_type_list.append(enum_name)
        
        else:
            if event['args'][user].lower() == user_address:
                block = web3.eth.get_block(event['blockNumber'])

                user_address_list.append(event['args'][user].lower())
                tx_hash_list.append(event['transactionHash'].hex())
                timestamp_list.append(block['timestamp'])
                token_address_list.append(event['args']['reserve'])
                token_volume_list.append(0)
                token_usd_amount_list.append(0)
                lend_borrow_type_list.append(enum_name)
    # i = 0


    df['wallet_address'] = user_address_list
    df['txHash'] = tx_hash_list
    df['timestamp'] = timestamp_list
    df['tokenAddress'] = token_address_list
    df['tokenVolume'] = token_volume_list
    df['tokenUSDAmount'] = token_usd_amount_list
    df['lendBorrowType'] = lend_borrow_type_list

    # print('User Data Event Looping done in: ', time.time() - start_time)
    return df

#handles our weth_gateway events and returns the accurate user_address
def handle_weth_gateway(event, enum_name):

    payload_address = event['args']['user'].lower()

    if payload_address.lower() == '0x4d8d90FAF90405b9743Ce600E98A2Aa8CdF579a0'.lower():
        if enum_name == 'LEND' or enum_name == 'BORROW':
            user = 'onBehalfOf'
            payload_address = event['args'][user].lower()
    
    return payload_address

#makes our dataframe
def user_data_2(user_address, events, enum_name):
    
    df = pd.DataFrame()

    user_address_list = []
    tx_hash_list = []
    timestamp_list = []
    token_address_list = []
    token_volume_list = []
    token_usd_amount_list = []
    lend_borrow_type_list = []

    user = ''

    start_time = time.time()
    i = 1
    print(len(events))
    for event in events:
        print(i, '/', len(events))
        i+=1
        # if enum_name == 'REPAY':
        #     user = 'user'
        # elif enum_name == 'COLLATERALISE':
        #     user = 'user'
        # else:
        #     user = 'user'

        # block = web3.eth.get_block(event['blockNumber'])
        # if block['timestamp'] >= 1701086400:
        if enum_name != 'COLLATERALISE':
            
            exists_list = already_part_of_df(event, enum_name)

            tx_hash = exists_list[0]
            wallet_address = exists_list[1]
            exists = exists_list[2]

            if exists == False and len(wallet_address) < 2:
                
                #adds wallet_address if it doesn't exist
                if len(wallet_address) < 2:
                    wallet_address = handle_weth_gateway(event, enum_name)
                

                block = web3.eth.get_block(event['blockNumber'])

                user_address_list.append(wallet_address)
                tx_hash_list.append(tx_hash)
                timestamp_list.append(block['timestamp'])
                token_address = event['args']['reserve']
                token_address_list.append(token_address)
                token_volume = event['args']['amount']
                token_volume_list.append(token_volume)
                # token_usd_amount_list.append(get_tx_usd_amount(token_address, token_volume))
                token_usd_amount_list.append(0)
                lend_borrow_type_list.append(enum_name)
            
            else:
                print('Skipped')

        else:
            exists_list = already_part_of_df(event, enum_name)

            tx_hash = exists_list[0]
            wallet_address = exists_list[1]
            exists = exists_list[2]
            
            if exists == False and len(wallet_address) < 2:
                
                wallet_address = handle_weth_gateway(event, enum_name)

                block = web3.eth.get_block(event['blockNumber'])

                user_address_list.append(wallet_address)
                tx_hash_list.append(tx_hash)
                timestamp_list.append(block['timestamp'])
                token_address_list.append(event['args']['reserve'])
                token_volume_list.append(0)
                token_usd_amount_list.append(0)
                lend_borrow_type_list.append(enum_name)
            
            else:
                print('Skipped')

    df['wallet_address'] = user_address_list
    df['txHash'] = tx_hash_list
    df['timestamp'] = timestamp_list
    df['tokenAddress'] = token_address_list
    df['tokenVolume'] = token_volume_list
    df['tokenUSDAmount'] = token_usd_amount_list
    df['lendBorrowType'] = lend_borrow_type_list

    print('User Data Event Looping done in: ', time.time() - start_time)
    return df

# will tell us whether we need to find new data
# returns a list of [tx_hash, wallet_address]
def already_part_of_df(event, enum):

    all_exist = False
    tx_hash = ''
    wallet_address = ''

    df = pd.read_csv('all_events.csv')

    tx_hash = event['transactionHash'].hex()
    tx_hash = tx_hash.lower()

    new_df = tx_hash_exists(df, tx_hash)

    if len(new_df) > 0:
        new_df = lend_borrow_type_exists(new_df, enum)

        if len(new_df) > 0:
            wallet_address = handle_weth_gateway(event, enum).lower()
            new_df = wallet_address_exists(df, wallet_address)

            if len(new_df) > 0:
                all_exist = True

    response_list = [tx_hash, wallet_address, all_exist]

    return response_list

#returns a df if a tx_hash exists
def tx_hash_exists(df, tx_hash):

    new_df = pd.DataFrame()

    if ((df['txHash'] == tx_hash)).any():
        new_df = df.loc[df['txHash'] == tx_hash]
    
    return new_df

#returns whether a enum_name exists, and returns blank df if not
def lend_borrow_type_exists(df, lend_borrow_type):

    if ((df['lendBorrowType'] == lend_borrow_type)).any():
        df = df.loc[df['lendBorrowType'] == lend_borrow_type]

    else:
        df = pd.DataFrame()

    return df

#returns df if wallet_address exists
def wallet_address_exists(df, wallet_address):

    if ((df['wallet_address'] == wallet_address)).any():
        df = df.loc[df['wallet_address'] == wallet_address]

    else:
        df = pd.DataFrame()

    return df

#gets all borrow events
# @cache
def get_borrow_events(contract):
    # latest_block = web3.eth.get_block_number()
    # from_block = latest_block - 100000
    # from_block = 1052610

    events = contract.events.Borrow.get_logs(fromBlock=FROM_BLOCK, toBlock='latest')

    return events

#gets all deposit events
# @cache
def get_lend_events(contract):
    # latest_block = web3.eth.get_block_number()
    # from_block = latest_block - 100000
    # from_block = 1052610

    events = contract.events.Deposit.get_logs(fromBlock=FROM_BLOCK, toBlock='latest')

    return events

#gets all repay events
# @cache
def get_repay_events(contract):
    # latest_block = web3.eth.get_block_number()
    # from_block = latest_block - 100000
    # from_block = 1052610

    events = contract.events.Repay.get_logs(fromBlock=FROM_BLOCK, toBlock='latest')

    return events

#gets all collateralise events
# @cache
def get_collateralise_events(contract):
    # latest_block = web3.eth.get_block_number()
    # from_block = latest_block - 100000
    # from_block = 1052610

    events = contract.events.ReserveUsedAsCollateralEnabled.get_logs(fromBlock=FROM_BLOCK, toBlock='latest')

    return events


#gets all of our borrow transactions
# @cache
def get_borrow_transactions(user_address, contract):

    df = pd.DataFrame()

    start_time = time.time()

    events = get_borrow_events(contract)
    print('Events found in: ', time.time() - start_time)

    if len(events) > 1:
        try:
            # df = user_data(user_address, events, 'BORROW')
            df = user_data_2(user_address, events, 'BORROW')
        except:
            df = pd.DataFrame()

    return df

#gets all of our deposit transactions
# @cache
def get_lend_transactions(user_address, contract):
    
    df = pd.DataFrame()

    events = get_lend_events(contract)

    if len(events) > 1:
        try:
            # df = user_data(user_address, events, 'LEND')
            df = user_data_2(user_address, events, 'LEND')
        except:
            df = pd.DataFrame()

    return df

#gets all of our repayment transactions
# @cache
def get_repay_transactions(user_address, contract):

    df = pd.DataFrame()


    events = get_repay_events(contract)

    if len(events) > 1:
        try:
            # df = user_data(user_address, events, 'REPAY')
            df = user_data_2(user_address, events, 'REPAY')
        except:
            df = pd.DataFrame()
    
    return df

# @cache
def get_collateralalise_transactions(user_address, contract):
    
    df = pd.DataFrame()
    
    events = get_collateralise_events(contract)
    if len(events) > 1:
        try:
            # df = user_data(user_address, events, 'COLLATERALISE')
            df = user_data_2(user_address, events, 'COLLATERALISE')
        except:
            df = pd.DataFrame()

    return df

#takes in our user address and will populate all the needed fields for our api_response
# @cache
def get_all_user_transactions(user_address):

    df = pd.DataFrame()

    df_list = []

    if len(user_address) == 42:
        contract = get_contract()

        start_time = time.time()
        borrow_df = get_borrow_transactions(user_address, contract)
        # print(borrow_df)
        make_user_data_csv(borrow_df)
        print('Borrower Transactions found in: ', time.time() - start_time)
        start_time = time.time()
        lend_df = get_lend_transactions(user_address, contract)
        make_user_data_csv(lend_df)
        print(lend_df)
        print('Lend Transactions found in: ', time.time() - start_time)
        start_time = time.time()
        repay_df = get_repay_transactions(user_address, contract)
        make_user_data_csv(repay_df)
        # print(repay_df)
        print('Repay Transactions found in: ', time.time() - start_time)
        start_time = time.time()
        collateralize_df = get_collateralalise_transactions(user_address, contract)
        make_user_data_csv(collateralize_df)
        # print(collateralize_df)
        print('Collaterise Transactions found in: ', time.time() - start_time)

        # properly redoes our collateralizes
        handle_gateway_collateralise()

        # df_list = [borrow_df, lend_df, repay_df, collateralize_df]

        df_list = [lend_df]

        df = pd.concat(df_list)
    
    # print(df)

    return df

# formats our dataframe response
def make_api_response_string(df):
    
    data = []

    #if we have an address with no transactions
    if len(df) < 1:
        temp_df = pd.DataFrame()
        data.append({
           "txHash": 'N/A',
            "timestamp": -1,
            "tokenAddress": 'N/A',
            "tokenVolume": '-1',
            "tokenUSDAmount": -1,
            "lendBorrowType": 'N/A'
        })

    else:
        temp_df = df[['txHash', 'timestamp', 'tokenAddress', 'tokenVolume', 'tokenUSDAmount', 'lendBorrowType']]
        # Process data
        for i in range(temp_df.shape[0]):
            row = temp_df.iloc[i]
            data.append({
                "txHash": str(row['txHash']),
                "timestamp": int(row['timestamp']),
                "tokenAddress": str(row['tokenAddress']),
                "tokenVolume": str(row['tokenVolume']),
                "tokenUSDAmount": float(row['tokenUSDAmount']),
                "lendBorrowType": str(row['lendBorrowType'])
            })

    # Create JSON response
    response = {
        "error": {
            "code": 0,
            "message": "success"
        },
        "data": {
            "result": data
        }
    }
    
    return response

# executes all of functions
def search_and_respond(address, queue):

    df = get_all_user_transactions(address)
    
    response = make_api_response_string(df)

    queue.put(response)

    queue.join()

    make_user_data_csv(df)
    # return response

#just reads from csv file
def search_and_respond_2(address, queue):
    
    df = pd.read_csv('all_events.csv')

    df = df.loc[df['wallet_address'] == address]

    response = make_api_response_string(df)

    queue.put(response)

    #new_df = get_all_user_transactions(address)

    #make_user_data_csv(new_df)

#makes a dataframe and stores it in a csv file
def make_user_data_csv(df):
    old_df = pd.read_csv('all_events.csv')
    old_df = old_df.drop_duplicates(subset=['wallet_address', 'txHash', 'lendBorrowType'], keep='last')

    combined_df_list = [df, old_df]
    combined_df = pd.concat(combined_df_list)
    combined_df = combined_df.drop_duplicates(subset=['wallet_address', 'txHash', 'lendBorrowType'], keep='last')

    combined_df['txHash'] = combined_df['txHash'].str.lower()
    combined_df['tokenAddress'] = combined_df['tokenAddress'].str.lower()

    # print(df)
    # print(len(old_df), len(df), len(combined_df))
    if len(combined_df) >= len(old_df):
        combined_df.to_csv('all_events.csv', index=False)
        print('CSV Made')
    return

#gets rid of weth_gateway_collateralizes
# adds a collateral row for each lend row for users who have borrowed something
# removes duplicates
def handle_gateway_collateralise():
    df = pd.read_csv('all_events.csv')
    df['wallet_address'] = df['wallet_address'].str.lower()
    df['txHash'] = df['txHash'].str.lower()

    df = df[df.wallet_address != '0x9546f673ef71ff666ae66d01fd6e7c6dae5a9995']

    lend_df = df.loc[df['lendBorrowType'] == 'LEND']

    borrow_df = df.loc[df['lendBorrowType'] == 'BORROW']

    borrower_wallet_list = borrow_df['wallet_address'].tolist()

    #gives us only wallet addresses that have borrowed something
    #this means that the user should have 'collateralized' some of their assets to begin with
    lend_df = lend_df[lend_df['wallet_address'].isin(borrower_wallet_list)]

    collateralize_df = lend_df

    collateralize_df['tokenVolume'] = '0'
    collateralize_df['tokenUSDAmount'] = 0
    collateralize_df['lendBorrowType'] = 'COLLATERALISE'

    make_user_data_csv(collateralize_df)

    return

get_unclaimed_metis_amount()
df_cleanup()

# user_address = '0xc4f8d9139d737c711d6af282c87c81c898b2bd66'.lower()
# get_all_user_transactions(user_address)

# contract = get_contract()
# print(get_repay_events(contract))

# #reads from csv
# @app.route("/transactions/", methods=["POST"])
# def get_transactions():

#     data = json.loads(request.data)  # Parse JSON string into JSON object

#     print(data)
#     address = data['address']
#     address = address.lower()
#     print(address)

#     # Create a queue to store the search result
#     result_queue = queue.Queue()

#     thread = threading.Thread(target=search_and_respond_2, args=(address, result_queue))
#     thread.start()
    
#     response = result_queue.get()

#     return jsonify(response), 200

# #get v3
# #makes our csv
# @app.route("/test/<address>", methods=["GET"])
# def balance_of(address):
    
#     address = address.lower()

#     df = get_all_user_transactions(address)

#     response = make_api_response_string(df)

#     # Create a queue to store the search result
#     result_queue = queue.Queue()

#     thread = threading.Thread(target=search_and_respond, args=(address, result_queue))
#     thread.start()
    
#     response = result_queue.get()

#     return jsonify(response), 200


# if __name__ == "__main__":
#     app.run()