
# Code to load the Current S&P500 Constituents from Quandl
# https://www.quandl.com/api/v3/datatables/SHARADAR/SP500.csv
# requires the python Quandl package, and the 
# Quandl API key to be set as an ENV variable QUANDL_API_KEY.

import quandl

from alphacompiler.util.zipline_data_tools import get_ticker_sid_dict_from_bundle
from alphacompiler.util.sparse_data import pack_sparse_data
from alphacompiler.util import quandl_tools
from logbook import Logger
import datetime
from os import listdir
import os
import pandas as pd
import numpy as np

from alphacompiler.util.database_util  import populate_raw_data_from_database_SP500mem

from sqlalchemy import create_engine 
from tqdm import tqdm

BASE = os.path.dirname(os.path.realpath(__file__))
DS_NAME = 'SHARADAR/SF1'   # quandl DataSet code
RAW_FLDR = "mem"  # folder to store the raw text file
VAL_COL_NAME = "Value"
START_DATE = '1999-01-01'
END_DATE = datetime.datetime.today().strftime('%Y-%m-%d')

# create the engine for the database
#engine = create_engine('mysql+mysqlconnector://root:root@localhost/securities_master')


ZIPLINE_DATA_DIR = '/Users/carstenfreek/.zipline/data/'  # TODO: get this from Zipline api
FN = "SP500mem.npy"

Init_DATE = '1999-01-01' # First date in database

log = Logger('load_quandl_sf1.py')


ACTION_CODING = {'removed': 0,
                   'added': 1,
                 'current': 1,
                'outside' : 0}



def all_tickers_for_bundle(fields, dims, bundle_name, raw_path=os.path.join(BASE,RAW_FLDR)):
    tickers = get_ticker_sid_dict_from_bundle(bundle_name)
    #populate_raw_data(tickers, fields, dims, raw_path)
    populate_raw_data_from_database_SP500mem(tickers, fields, dims, raw_path, ACTION_CODING,Init_DATE )


def num_tkrs_in_bundle(bundle_name):
    return len(get_ticker_sid_dict_from_bundle(bundle_name))


if __name__ == '__main__':

    fields = ['member']
    dimensions = ['SP500']

    from zipline.data.bundles.core import register
    from alphacompiler.data.loaders.sep_quandl import from_sep_dump

    BUNDLE_NAME = 'sep'
    register(BUNDLE_NAME, from_sep_dump('.'), )
    num_tickers = num_tkrs_in_bundle(BUNDLE_NAME)
    print('number of tickers: ', num_tickers)

    all_tickers_for_bundle(fields, dimensions, 'sep')  # downloads the data to /raw
    pack_sparse_data(num_tickers + 1,  # number of tickers in buldle + 1
                    os.path.join(BASE, RAW_FLDR),
                    fields,
                    ZIPLINE_DATA_DIR + FN)  # write directly to the zipline data dir


    print("this worked master")
