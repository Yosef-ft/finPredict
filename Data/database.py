from typing import *
import logging

import pandas as pd
import numpy as np 
import h5py


logger = logging.getLogger()

class Hdf5Client:
    def __init__(self):

        self.hdf = h5py.File('Binance.h5', 'a')
        self.hdf.flush()

    def create_data(self, symbol: str):
        if symbol not in self.hdf.keys():
            self.hdf.create_dataset(symbol,shape=(0,6),maxshape=(None, 6), dtype='float64')
            self.hdf.flush()
    
    def write_data(self, symbol: str, data: List[Tuple]):

        min_ts, max_ts = self.get_first_last_timestamp(symbol)

        if min_ts is None:
            min_ts = float('inf')
            max_ts = 0

        filtered_data = []

        for candle in data:
            if candle[0] < min_ts:
                filtered_data.append(candle)
            elif candle[0] > max_ts:
                filtered_data.append(candle)

        if len(filtered_data) == 0:
            logger.info('No Additioal data exist of %s', symbol)
            return 

        data_array = np.array(data)

        self.hdf[symbol].resize(self.hdf[symbol].shape[0] + data_array.shape[0], axis=0)
        self.hdf[symbol][-data_array.shape[0]:] = data_array

        self.hdf.flush()

    def get_first_last_timestamp(self, symbol: str) -> Union[Tuple[None, None], Tuple[float, float]]:
        print('data>>>>>>>> ',self.hdf[symbol])

        existing_data = self.hdf[symbol][:]

        if len(existing_data) == 0:
            return None, None
        
        first_ts = min(existing_data, key= lambda x: x[0])[0]
        last_ts = max(existing_data, key= lambda x: x[0])[0]

        return first_ts, last_ts
    
    