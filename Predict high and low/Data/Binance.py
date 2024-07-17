import requests
import logging
from typing import *

import pandas

logger = logging.getLogger()

class BinanceClient:
    def __init__(self, futures = False):
        
        self.futures = futures

        if futures:
            self._url = 'https://fapi.binance.com'
        else:
            self._url = 'https://api.binance.com'


    def _make_request(self, endpoint: str, query_parameters: Dict):

        try:
            response = requests.get(self._url + endpoint,  params = query_parameters)
        except Exception as e:
            logger.error('Connection error while making request to %s: %s', endpoint, e)
            return None
        
        if response.status_code == 200:
            return response.json()
        
        else:
            logger.error('Error while making a request to %s: %s (status code %s) ', endpoint, response.json(), response.status_code)
            return None
        



    
