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

def decode_logs_data(logs, abi):
    for log in logs:
        # Convert hex values to integers
        keys_to_convert = ['blockNumber', 'timeStamp', 'gasPrice', 'gasUsed', 'logIndex', 'transactionIndex']
        for key in keys_to_convert:
            if key in log:
                log[key] = int(log[key], 16)
        # convert logs data
        w3 = Web3(Web3.HTTPProvider(f''))    
        receipt_event_signature_hex = log['topics'][0]
        event_list = [item for item in abi if item['type'] == 'event']

        for event in event_list:
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

                log['decoded'] = decoded_log
                break  # Break the inner loop as we've found the matching event

    return logs

def decode_transactions_input(data, abi):
    w3 = Web3(Web3.HTTPProvider(f''))
    contract = w3.eth.contract(address = '', abi = abi) 
    for transaction in data:
        # add checking for oX
        try:
            decoded_func, decoded_input = contract.decode_function_input(transaction['input'])
            transaction['decoded_func'] = decoded_func
            transaction['decoded_input'] = decoded_input
        except:
            transaction['decoded_func'] = None
            transaction['decoded_input'] = None
            # print(f"Error: transaction - {transaction['hash']}") 
            # print(f"Error for input - {transaction['input']}")    
    return data

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
        local_start = startblock
        address_transactions = []

        while local_start < endblock:
            transactions = await self.get_transactions_chunk(
                address=address, module=module, action=action, abi=abi, startblock=local_start, endblock=99999999, offset=10000, 
                **kwargs)
            print(f"Address: {address}, Start block: {local_start}, End block: {endblock},Transactions: {len(transactions)}") 
            if not transactions:
                break
            address_transactions += transactions
            if len(transactions) < offset:
                print('No more transactions')
                break
            if local_start == transactions[-1]['blockNumber']:
                print('Last block reached')
                break
            if module == 'logs':
                local_start = int(transactions[-1]['blockNumber'], 16)
            else:    
                local_start = int(transactions[-1]['blockNumber'])
            # if isinstance(transactions[-1]['blockNumber'], str)
            # last_block = int(transactions[-1]['blockNumber'], 16) if isinstance(transactions[-1]['blockNumber'], str) else transactions[-1]['blockNumber']
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

        results = [item for sublist in results for item in sublist]

        return results
