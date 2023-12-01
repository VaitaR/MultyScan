import logging
from functools import wraps
import json
from typing import List, Union

import asyncio
import aiohttp
from web3 import Web3
from eth_abi import decode



def retry(attempts=3, delay=0.5):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            for _ in range(attempts):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    logging.warning(f"Error: {e}. Retrying in {delay} seconds...")
                    print("Retry attempt failed, retry number {0} in {1} seconds".format(_, delay))
                    await asyncio.sleep(delay)
            raise last_exception
        return wrapper
    return decorator

def decode_log(log, abi):
    w3 = Web3(Web3.HTTPProvider(f''))
    decoded_logs = []
    
    receipt_event_signature_hex = log['topics'][0]
    for event in [item for item in abi if item['type'] == 'event']:
        # Generate event signature hash
        name = event['name']
        inputs = ",".join([param['type'] for param in event['inputs']])
        event_signature_text = f"{name}({inputs})"
        event_signature_hex = w3.to_hex(w3.keccak(text=event_signature_text))

       # Check if the event signature matches the log's signature
        if event_signature_hex == receipt_event_signature_hex:
            decoded_log = {"event": event['name']}

            # Decode indexed topics
            indexed_params = [input for input in event['inputs'] if input['indexed']]
            for i, param in enumerate(indexed_params):
                topic = log['topics'][i+1]
                decoded_log[param['name']] = decode([param['type']], bytes.fromhex(topic[2:]))[0]

            # Decode non-indexed data
            non_indexed_params = [input for input in event['inputs'] if not input['indexed']]
            non_indexed_types = [param['type'] for param in non_indexed_params]
            non_indexed_values = decode(non_indexed_types, bytes.fromhex(log['data'][2:]))
            for i, param in enumerate(non_indexed_params):
                decoded_log[param['name']] = non_indexed_values[i]

            decoded_logs.append(decoded_log)
            break  # Break the inner loop as we've found the matching event

    return decoded_logs

def transactions_input_convert(data, abi):
    w3 = Web3(Web3.HTTPProvider(f''))
    contract = w3.eth.contract(address = '', abi = abi) 
    for transaction in data:
        # add checking for oX
        try:
            decoded_input = contract.decode_function_input(transaction['input'])
            transaction['decoded'] = decoded_input
        except:
            transaction['decoded'] = None
            print(f"Error: transaction - {transaction['hash']}") 
            print(f"Error for input - {transaction['input']}")    
    return data

# need change
def transform_logs(logs:list):
    keys_to_convert = ['blockNumber', 'timeStamp', 'gasPrice', 'gasUsed', 'logIndex', 'transactionIndex']
    for dictionary in logs:
        for key in keys_to_convert:
            if key in dictionary:
                dictionary[key] = int(dictionary[key], 16)
    return logs

class async_chain_scanner:
    chain_configs = {
        'eth': {
            'base_url': 'https://api.etherscan.io/api',
        },
        'bsc': {
            'base_url': 'https://api.bscscan.com/api',
        },
        'avalanche': {
            'base_url': 'https://api.snowtrace.io/api',
        },
        'fantom': {
            'base_url': 'https://api.ftmscan.com/api',
        },
        'polygon': {
            'base_url': 'https://api.polygonscan.com/api',
        },
        'polygon_zkevm': {
            'base_url': 'https://api-zkevm.polygonscan.com/api',
        },
        'optimism': {
            'base_url': 'https://api-optimistic.etherscan.io/api',
        },
        'arbitrum': {
            'base_url': 'https://api.arbiscan.io/api',
        },
        'linea': {
            'base_url': 'https://api.lineascan.build/api', # Предположительный URL, замените на фактический, если отличается
        }
    }

    def __init__(self, chain: str, api_key: str):
        self.validate_chain_and_api_key(chain, api_key)
        config = self.chain_configs[chain]
        self.api_key = api_key
        self.base_url = config['base_url']
        self.chain = chain
        self.requests_count = 0

    @staticmethod
    def validate_chain_and_api_key(chain: str, api_key: str):
        if chain not in async_chain_scanner.chain_configs:
            raise ValueError(f"Unsupported chain: {chain}")
        if not api_key:
            raise ValueError(f"API key for {chain} is not set.")

    @retry(attempts=3, delay=2)
    async def get_transactions_chunk(
        self, address, module:str, action:str, abi=None,
        startblock=0, endblock=99999999, offset=10000,  
        **kwargs):

        params = {
            'module': module,
            'action': action,
            'address': address,
            'startblock': startblock,
            'endblock': endblock,
            'page': 1,
            'offset': offset,
            'sort': 'asc',
            'apikey': self.api_key
        }
        # print(abi)
        params.update(kwargs)
        print(f"request params: {params}")
        if address is None or action is None:
            raise ValueError("address and action cannot be None")
        
        async with aiohttp.ClientSession() as session:
            async with session.get(self.base_url, params=params) as response:
                self.requests_count += 1
                if response.status != 200:
                    raise Exception(f"HTTP Error {response.status}: {response.text}")
                data = await response.json()
                if data.get('status') == '0' and data.get('message') == 'No transactions found':
                    raise Exception("No transactions found for the given parameters.")
                elif data.get("status") != "1":
                    error_message = data.get("message", "Unknown Error")
                    # print(f"API Warning: {error_message}")
                    print(f"request params: {params}")
                    print(f"Response: {data}, API Warning: {error_message}")
                    raise Exception(f"API Error: {error_message}")
                # if abi_convert is True:
                #     return 

                # print(f"Transactions: {data['result'][1:5]}...")
                print(f'transactions count: {len(data["result"])}')
                print(f'requests count: {self.requests_count}')
                # if abi is not None:
                #     return async_chain_scanner.input_convert(module=module, abi=abi, data=data['result'])

                # use in future web3 https://github.com/ethereum/web3.py/blob/d6d1084d155485ce6eb92408ee778aab016ee6d0/web3/_utils/method_formatters.py#L244
                # move in anpother function
                return data['result']   

    @retry(attempts=3, delay=2)
    async def get_abi(self, address):
        params = {
            'module': 'contract',
            'action': 'getabi',
            'address': address,
            'apikey': self.api_key
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(self.base_url, params=params) as response:
                self.requests_count += 1
                if response.status != 200:
                    raise Exception(f"HTTP Error {response.status}: {response.text}")
                data = await response.json()
                if data['status'] != "1":
                    error_message = data.get("message", "Unknown Error")
                    print(f"Status code: {data['status']}, API Warning: {error_message}")
                    return []
                abi = data['result']
                print(f'requests count: {self.requests_count}')
                return json.loads(abi)

    async def fetch_transactions_for_address(self, address:str, module: str, action: str, abi: List = None, 
                startblock: int = 0, endblock: int = 99999999, offset: int = 10000, **kwargs):
        pagination_page = 1
        last_block = startblock
        address_transactions = []

        while last_block < endblock:
            print(f'Address: {address}, Page: {pagination_page}')
            transactions = await self.get_transactions_chunk(
                address=address, module=module, action=action, abi=abi, startblock=0, endblock=99999999, offset=10000, 
                **kwargs)
            if not transactions:
                break
            address_transactions += transactions
            if len(transactions) < offset or last_block == transactions[-1]['blockNumber']:
                break

            last_block = int(transactions[-1]['blockNumber'], 16) if isinstance(transactions[-1]['blockNumber'], str) else transactions[-1]['blockNumber']
            pagination_page += 1

        return address_transactions

    async def fetch_transactions(
        self, addresses: Union[str, List[str]], module: str, action: str, abi: List = None, 
        startblock: int = 0, endblock: int = 99999999, offset: int = 10000, **kwargs):

        if isinstance(addresses, str):
            address = addresses
            task = self.fetch_transactions_for_address(address=address, module=module, action=action, abi=abi,
                    startblock=startblock, endblock=endblock, offset=offset, **kwargs)
            results = await asyncio.gather(task)
        else:
            tasks = [self.fetch_transactions_for_address(address=address, module=module, action=action, abi=abi,
                    startblock=startblock, endblock=endblock, offset=offset, **kwargs) for address in addresses]
            results = await asyncio.gather(*tasks)

        # if abi is not None and module == 'account':
        #     results = transactions_input_convert(results, abi)
        
        # if module == 'logs':
        #     results = transform_logs(results)
        #     if abi is not None:
        #         results = decode_log(results, abi)

        return results
