from typing import * 
import logging
import time


from Binance import BinanceClient
from database import Hdf5Client
from utils import *


logger = logging.getLogger()


def collect_all(client: BinanceClient, symbol: str):

    hdf = Hdf5Client()
    hdf.create_data(symbol)

    oldest_ts, most_recent_ts = hdf.get_first_last_timestamp(symbol)


    # Initial request

    if oldest_ts is None:
        data = client.get_historical_data(symbol, endTime=int((time.time() * 1000) - 60000))

        if len(data) == 0:
            logger.warning('%s: No additional recent data found', symbol)
            return
        else:
            logger.info('%s: Collected %s initial data from %s to %s', symbol, len(data), 
                        ms_to_dt(data[0][0]), ms_to_dt(data[-1][0]))
            
        oldest_ts = data[0][0]
        most_recent_ts = data[-1][0]

        hdf.write_data(data)

    # Most recent request


    # Older reqest