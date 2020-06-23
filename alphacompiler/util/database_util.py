# this file includes the functions to read the 
# fundamental data from a database https://github.com/carstenf/Security-Master

# this file should be coppied to: 
# /Users/carsten/opt/anaconda3/envs/test35/lib/python3.5/site-packages/alphacompiler/util


# active changes to the alphacompiler library
#
# 1) in load_quandl_sf1.py
# 
#    add:
#    from alphacompiler.util.database_util  import populate_raw_data_from_database
#    
# 
#    add:
#    in -> all_tickers_for_bundle line around 119
#    -> populate_raw_data_from_database(tickers, fields, dims, raw_path)
# 
#    like this:
#    def all_tickers_for_bundle(fields, dims, bundle_name, raw_path=os.path.join(BASE,RAW_FLDR)):
#        tickers = get_ticker_sid_dict_from_bundle(bundle_name)
#        #populate_raw_data(tickers, fields, dims, raw_path) <- comment this one
#        populate_raw_data_from_database(tickers, fields, dims, raw_path)  <- add this one !!!!!
#
#
# 2) in SHARADAR_sector_code_loader.py
#
#    add:
#    from alphacompiler.util.database_util  import create_static_table_from_database
#
#    add:
#    in main -> create_static_table_from_database(ZIPLINE_DATA_DIR, STATIC_FILE, BUNDLE_NAME, SECTOR_CODING, EXCHANGE_CODING, CATEGORY_CODING)
#
#     like this:
#     if __name__ == '__main__':
#
#        #create_static_table_from_file(TICKER_FILE)  <- comment this one
#        #create_sid_table_from_file(TICKER_FILE)  # only SID sectors <- comment this one
#        create_static_table_from_database(ZIPLINE_DATA_DIR, STATIC_FILE, BUNDLE_NAME, SECTOR_CODING, EXCHANGE_CODING, CATEGORY_CODING) # from data_base    <- add this one !!!!!
#
#    and call the sector codes with:
#
#    class SHARADARStatic_siccode(CustomFactor)
#
#    my version has 4 items, same as original plus siccode
#
#
#

import pandas as pd
import numpy as np
from sqlalchemy import create_engine 
from zipline.pipeline.factors import CustomFactor
from alphacompiler.util.zipline_data_tools import get_ticker_sid_dict_from_bundle
from alphacompiler.util.sparse_data import SparseDataFactor

from zipline.utils.paths import zipline_root


from zipline.data.bundles.core import register

from tqdm import tqdm

import os
import glob

# create the engine for the database
engine = create_engine('mysql+mysqlconnector://root:root@localhost/securities_master')

ZIPLINE_DATA_DIR = zipline_root() + '/data/'

STATIC_FILE = "SHARDAR_static.npy"  # persisted np.array

RAW_FLDR = "raw"  # folder to store the raw text file

def clear_raw_folder(raw_folder_path):
    # removes all the files in the raw folder
    print('   **   clearing the raw/ folder   **')
    files = glob.glob(raw_folder_path + '/*')
    for f in files:
        os.remove(f)


def available_stocks():

    # this function creates a list of ticker symbols and security_id / ticker_id from the security table
    # the security_id will later point to the content which will be ingestet
    # the list of ticker symbols and security_id can be a mixed of
    # different data_vendor, exchanges and qtables (need this as quandl offer different tables)
    
    # BE CAREFULL, DO NOT MIX TWO SOURCES WITH THE SAME TICKER, 

    # DEFINE VENDOR, MARKET, ASSET CLASS
    # decide which data_vender you want
    data_vendor1 = 'Quandl'
    
    # decide which markets to trade -> NYSE, NASDAQ
    exchange1= 'NYSE'
    exchange2= 'NASDAQ'
    exchange3= 'NYSEMKT'
    exchange4= 'NYSEARCA'
    exchange5= 'BATS'

   
    # decide which asset class, with Quandl table SFP only stocks are included, just adding index with yahoo
    qtable1= 'SF1' # hey carsten, don't change, otherwise no fundamentel will be pulled, as sec_id is wrong!!!! dont't touch this! specialy not late at night!!!

    # define which data vender to use 
    query = """SELECT id FROM securities_master.data_vendor WHERE name = '{}' ; """.format(data_vendor1)
    value = pd.read_sql_query(query, engine)
    data_vendor_id1 = value.id[0].astype(int)
    
    # define which markets to use 
    query = """SELECT id FROM securities_master.exchange WHERE name = '{}' ; """.format(exchange1)
    value = pd.read_sql_query(query, engine)
    exchange_id1 = value.id[0].astype(int)

    # define which markets to use
    query = """SELECT id FROM securities_master.exchange WHERE name = '{}' ; """.format(exchange2)
    value = pd.read_sql_query(query, engine)
    exchange_id2 = value.id[0].astype(int)

    # define which markets to use
    query = """SELECT id FROM securities_master.exchange WHERE name = '{}' ; """.format(exchange3)
    value = pd.read_sql_query(query, engine)
    exchange_id3 = value.id[0].astype(int)

    # define which markets to use
    query = """SELECT id FROM securities_master.exchange WHERE name = '{}' ; """.format(exchange4)
    value = pd.read_sql_query(query, engine)
    exchange_id4 = value.id[0].astype(int)

    # define which markets to use
    query = """SELECT id FROM securities_master.exchange WHERE name = '{}' ; """.format(exchange5)
    value = pd.read_sql_query(query, engine)
    exchange_id5 = value.id[0].astype(int)


    # GET the TICKER SYMBOLS
    # Main query for ticker and ticker_id / security_id
    # query ticker symbols and the ticker_id from the security table
    
    query_1 = """SELECT distinct ticker, id FROM security WHERE """

    query_2 = """  ( ttable = '{}'  ) """.format( qtable1)

    query_3 = """ and (data_vendor_id = {} )""".format( data_vendor_id1)

    query_4 = """ and ( exchange_id = {} or exchange_id = {} or exchange_id = {} or exchange_id = {} or exchange_id = {} 
    ) """.format( exchange_id1, exchange_id2, exchange_id3, exchange_id4, exchange_id5)

    #query_4 = """ and ( exchange_id = {} or exchange_id = {}  
    #) """.format( exchange_id1, exchange_id2)

    
    query_5 = """ and ticker not like '$%' order by ticker """
    
    symbol_query = query_1 + query_2 + query_3 + query_4 + query_5
        
    symbol_df = pd.read_sql_query(symbol_query, engine)

    return symbol_df


def listToString(s):  
    # initialize an empty string 
    str1 = " , "   
    return (str1.join(s)) 


def populate_raw_data_from_database(tickers, fields, dimensions, raw_path):
    """tickers is a dict with the ticker string as the key and the SID
    as the value.
    For each field a dimension is required, so dimensions should be a list
    of dimensions for each field.
    """
    clear_raw_folder(RAW_FLDR)
    
    ticker_sec_id = available_stocks()

    assert len(fields) == len(dimensions)
 
    print('will take 5 min....')
    for ticker, sid in tqdm(tickers.items(), total=len(tickers)): 
        
        if not ticker_sec_id.loc[ticker_sec_id.ticker == ticker ].id.empty:

            security_id = ticker_sec_id.loc[ticker_sec_id.ticker == ticker ].id.iloc[0]  

            fields_str=listToString(fields)

            query =  """SELECT dimension , datekey , %s FROM fundamental WHERE
                      security_id = %s order by datekey """ % (fields_str, security_id)

            df_tkr = pd.read_sql_query(query, engine, parse_dates=['datekey']) 

            df_tkr = df_tkr[[ 'dimension', 'datekey'] + fields]  # remove columns not in fields
            df_tkr = df_tkr.loc[:, ~df_tkr.columns.duplicated()]  # drop any columns with redundant names
    
            df_tkr = df_tkr.rename(columns={'datekey': 'Date'}).set_index('Date')            
            
            # loop over the fields and dimensions
            series = []
            for i, field in enumerate(fields):
                #print(i, field)
                s = df_tkr[df_tkr.dimension == dimensions[i]][field]        
                new_name = '{}_{}'.format(field, dimensions[i])
                s = s.rename(new_name)
                series.append(s)
            df_tkr = pd.concat(series, axis=1)
            df_tkr.index.names = ['Date']  # ensure that the index is named Date
            #print("AFTER reorganizing")
            #print(df_tkr.head(5))

            # write raw file: raw/
            df_tkr.to_csv(os.path.join(raw_path, "{}.csv".format(sid)))

        else:
            # to much errors, not use it anymore
            print("error with ticker: {}, did not find security_id from database".format(ticker))   


def get_name_exchange_id(): 
    query = """SELECT id, name FROM exchange;"""
    result = pd.read_sql_query(query, engine)
    return result


def create_static_table_from_database(ZIPLINE_DATA_DIR, STATIC_FILE, BUNDLE_NAME, SECTOR_CODING, EXCHANGE_CODING, CATEGORY_CODING):
    """Stores static items to a persisted np array.
    The following static fields are currently persisted.
    -Sector
    -exchange
    -category
    -code ->siccode
    """

    register(BUNDLE_NAME, int, )

    query =  """SELECT ticker, code, sector, exchange_id, category FROM security WHERE ttable = 'SF1' """ 
    df = pd.read_sql_query(query, engine) 
    
    # add the exchange based on the exchange and exchange_id relation
    # get the exchange and exchange_id relation
    name_ex_id=get_name_exchange_id()
    my_EXCHANGE_CODING =  name_ex_id.set_index('id')['name'].to_dict()
    # add the exchange based on the exchange and exchange_id relation
    df['exchange'] = df['exchange_id'].map(my_EXCHANGE_CODING)

    df['sectors_'] = df['sector'].map(SECTOR_CODING)
    df['exchange_'] = df['exchange'].map(EXCHANGE_CODING)
    df['category_'] = df['category'].map(CATEGORY_CODING)
    df['code_']  = df['code'].astype(int) # just multiply the siccode by 10 and get integer
    

    df = df.fillna(-1)

    ae_d = get_ticker_sid_dict_from_bundle(BUNDLE_NAME)
    N = max(ae_d.values()) + 1

    # create 2-D array to hold data where index = SID
    sectors = np.full((4, N), -1, np.dtype('int64'))
    # sectors = np.full(N, -1, np.dtype('int64'))

    # iterate over Assets in the bundle, and fill in static fields
    for ticker, sid in tqdm(ae_d.items(), total=len(ae_d)): 
    #for ticker, sid in ae_d.items():
        #sector_coded = coded_sectors_for_ticker.get(ticker)
        if not df[df['ticker']==ticker].empty:
            #sector_   = df.sectors_[df['ticker']==ticker].iloc[0]
            #exchange_ = df.exchange_[df['ticker']==ticker].iloc[0]
            #category_ = df.category_[df['ticker']==ticker].iloc[0]
            #code_     = df.code_[df['ticker']==ticker].iloc[0]        
            #print(ticker, sid, sector_coded, exchange_, category_, code_ ,'<-end')
            sectors[0, sid] = df.sectors_[df['ticker']==ticker].iloc[0]
            sectors[1, sid] = df.exchange_[df['ticker']==ticker].iloc[0]
            sectors[2, sid] = df.category_[df['ticker']==ticker].iloc[0]
            sectors[3, sid] = df.code_[df['ticker']==ticker].iloc[0]
        else:
            sectors[0,sid] =  -1
            sectors[1,sid] =  -1
            sectors[2,sid] =  -1
            sectors[3,sid] =  -1

            #print('ticker missing but filled with -1, everthing under control keep cool= ',ticker) 
    print(sectors)
    print(sectors[:, -10:])

    # finally save the file to disk
    np.save(ZIPLINE_DATA_DIR + STATIC_FILE, sectors)
    print("this worked master")



def populate_raw_data_from_database_SP500mem(tickers, fields, dimensions, raw_path, ACTION_CODING, Init_DATE ):
    """tickers is a dict with the ticker string as the key and the SID
    as the value.
    For each field a dimension is required, so dimensions should be a list
    of dimensions for each field.
    """
    ticker_sec_id = available_stocks()

    assert len(fields) == len(dimensions)

    print('will take 5 min....')
    for ticker, sid in tqdm(tickers.items(), total=len(tickers)): 
    
        fields_str=listToString(fields)
        
        if not ticker_sec_id.loc[ticker_sec_id.ticker == ticker ].id.empty:

            security_id = ticker_sec_id.loc[ticker_sec_id.ticker == ticker ].id.iloc[0]  

            #query_str = "%s %s" % (security_id, ticker)
            #print("fetching data for: {}".format(query_str))

            query =  """SELECT action , date FROM SP500_const WHERE
                      security_id = %s order by date """ % ( security_id)

            df = pd.read_sql_query(query, engine, parse_dates=['date']) 
            
            if df['action'].empty:
                df = pd.DataFrame(data={'action': ['outside'], 'date': [Init_DATE]})              

            #print(df.head())
    
            df = df.rename(columns={'date': 'Date'}).set_index('Date')


            df['member'] = df['action'].map(ACTION_CODING)
            
            # loop over the fields and dimensions
            series = []
            for i, field in enumerate(fields):
                s = df[field]
                #print(s)
                series.append(s)
            df = pd.concat(series, axis=1)

            # write raw file: raw/
            df.to_csv(os.path.join(raw_path, "{}.csv".format(sid)))
        else:
            print("error with ticker: {}, did not find security_id from database".format(ticker))  

    print("this worked master")        

    

class SHARADARStatic_siccode(CustomFactor):
    """Returns static values for an SID.
    This holds static data (does not change with time) like: exchange, sector, etc."""
    inputs = []
    window_length = 1
    outputs = ['sector', 'exchange', 'category','code']

    def __init__(self, *args, **kwargs):
        self.data = np.load(ZIPLINE_DATA_DIR + STATIC_FILE)

    def compute(self, today, assets, out):
        # out[:] = self.data[assets]
        out['sector'][:]   = self.data[0, assets]
        out['exchange'][:] = self.data[1, assets]
        out['category'][:] = self.data[2, assets]   
        out['code'][:]     = self.data[3, assets]       


class sp500member(SparseDataFactor):
    outputs = ['member']

    def __init__(self, *args, **kwargs):
        super(sp500member, self).__init__(*args, **kwargs)
        self.N = len(get_ticker_sid_dict_from_bundle("sep")) + 1  # max(sid)+1 get this from the bundle

        self.data_path = ZIPLINE_DATA_DIR + 'SP500mem.npy'