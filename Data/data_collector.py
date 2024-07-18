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

        hdf.write_data(symbol, data)

    data_to_insert = []

    # Most recent request

    while True:
        data = client.get_historical_data(symbol, startTime= int(most_recent_ts + 60000))

        if data is None:
            time.sleep(4)
            continue

        if len(data) < 2:
            break

        data = data[:-1] 

        data_to_insert = data_to_insert + data

        if len(data_to_insert) > 5000:
            hdf.write_data(symbol, data_to_insert)
            data_to_insert.clear()

        if data[-1][0] > most_recent_ts:
            most_recent_ts = data[-1][0]

        logger.info('%s: Collected %s initial data from %s to %s', symbol, len(data), 
                        ms_to_dt(data[0][0]), ms_to_dt(data[-1][0]))        
        
        time.sleep(1.1)

    data_to_insert.clear()

    
    # Older reqest

    while True:
        data = client.get_historical_data(symbol, endTime= int(oldest_ts - 60000))

        if data is None:
            time.sleep(4)
            continue

        if len(data) == 0:
            logger.info('%s: stopped becasues no older data was found before %s', symbol, ms_to_dt(oldest_ts))
            break

        data_to_insert = data_to_insert + data

        if len(data_to_insert) > 5000:
            hdf.write_data(symbol, data_to_insert)
            data_to_insert.clear()
        
        if data[0][0] < oldest_ts:
            oldest_ts = data[0][0]

        logger.info('%s: collected %s older data from %s to %s', symbol, len(data), 
                    ms_to_dt(data[0][0]), ms_to_dt(data[-1][0]))            
        
        time.sleep(1.1)

    data_to_insert.clear()