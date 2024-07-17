from typing import *

import pandas as pd
import numpy as np 
import h5py

class Hdf5Client:
    def __init__(self):

        self.hdf = h5py.File('Binance.h5', 'a')
        self.hdf.flush()

    def create_data(self, symbol: str):
        if symbol not in self.hdf.keys():
            self.hdf.create_dataset(symbol, shape= (None, 6), dtype='float64')
            self.flush()
    
    