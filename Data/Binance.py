import requests
import logging
from typing import *

import pandas

logger = logging.getLogger()

class BinanceClient:
    def __init__(self, futures: Optional[bool] = True):
        
        self.futures = futures

        if self.futures:
            self._url = 'https://fapi.binance.com'
        else:
            self._url = 'https://api.binance.com'

        self.symbols = self._get_symbols()

    def _make_request(self, endpoint: str, query_parameters: Dict):

        try:
            response = requests.get(self._url + endpoint,  params = query_parameters)
        except Exception as e:
            logger.error('Connection error while making request to %s: %s', endpoint, e)
        
        if response.status_code == 200:
            return response.json()
        
        else:
            logger.error('Error while making a request to %s: %s (status code %s) ', endpoint, response, response.status_code)
            return None
        

    def _get_symbols(self):
        
        endpoint = '/fapi/v1/exchangeInfo' if self.futures else '/api/v3/exchangeInfo'
        params = dict()

        response = self._make_request(endpoint, params)
        symbols = response['symbols']

        return [symbol['symbol'] for symbol in symbols]
    
    def get_historical_data(self, symbol: str, startTime: Optional[int] = None, endTime: Optional[int] = None):

        params = dict()
        params['symbol'] = symbol
        params['interval'] = '1m'
        params['limit'] = 1500

        if startTime is not None:
            params['startTime'] = startTime
        if endTime is not None:
            params['endTime'] = endTime

        endpoint = '/fapi/v1/klines' if self.futures else '/api/v3/klines'

        response = self._make_request(endpoint, params)
        candles = []

        for c in response:
            candles.append((float(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5])))

        return candles


if __name__ == '__main__':
    client = BinanceClient()
    print(client.get_historical_data('BTCUSDT'))
    
    
