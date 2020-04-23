import pandas as pd
from datetime import datetime


def create_database(engine):
    pass


def remove_future_dates(engine):
    pass


def get_start_date(engine, symbol, table):
    start_date_query = 'SELECT MAX(Date) AS Date FROM {} WHERE Symbol="{}"'.format(table, symbol)
    return pd.read_sql_query(start_date_query, engine)['Date'][0]


def get_symbols(engine, not_symbols=""):
    symbol_query = 'SELECT Symbol, Type FROM symbols WHERE type NOT IN ({})'.format(not_symbols)
    return pd.read_sql_query(symbol_query, engine)['Symbol']


def get_data(engine, start_date, symbol, fetch_type, length=0):
    bank = 1000000

    if start_date is None:
        if fetch_type is 'technical_indicators':
            data_query = 'SELECT Date, Symbol, Close FROM data WHERE Symbol="{}"'.format(symbol)
            data = pd.read_sql_query(data_query, engine)
        elif fetch_type is 'trading_signals':
            data_query = 'SELECT * FROM technical_indicators WHERE Symbol="{}"'.format(symbol)
            data = pd.read_sql_query(data_query, engine)
        elif fetch_type is 'trade_sim':
            data_query = 'SELECT A.ID, A.Symbol, A.Date, A.Close, B.EMA_Cross FROM data AS A, trading_signals AS ' \
                         'B WHERE A.Symbol = B.Symbol AND A.Date=B.Date AND A.Symbol="{}"'.format(symbol)
            data = pd.read_sql_query(data_query, engine)
            data['Position'] = [0] * len(data)
            data['Shares'] = [0] * len(data)
            data['Bank'] = [0] * len(data)
            data.loc[0, 'Bank'] = bank
    else:
        if fetch_type is 'technical_indicators':
            delete_query = 'DELETE FROM technical_indicators WHERE Symbol="{}" AND Date="{}"'.format(symbol, start_date)
            engine.execute(delete_query)
            count_query = 'SELECT COUNT(*) AS Count FROM data WHERE Symbol="{}" AND Date>="{}"'.format(symbol,
                                                                                                       start_date)
            count = pd.read_sql_query(count_query, engine)['Count'][0]
            count += length
            data_query = 'SELECT * FROM (SELECT Date, Symbol, Close FROM data WHERE Symbol="{}" ORDER BY Date ' \
                         'DESC LIMIT {}) SUB ORDER BY Date ASC '.format(symbol, count)
            data = pd.read_sql_query(data_query, engine)
        elif fetch_type is 'trading_signals':
            delete_query = 'DELETE FROM trading_signals WHERE Symbol="{}" AND Date="{}"'.format(symbol, start_date)
            engine.execute(delete_query)
            count_query = 'SELECT COUNT(*) AS Count FROM technical_indicators WHERE Symbol="{}" AND Date>="{}"'.format(
                symbol,
                start_date)
            count = pd.read_sql_query(count_query, engine)['Count'][0]
            data_query = 'SELECT * FROM (SELECT * FROM technical_indicators WHERE Symbol="{}" ' \
                         'ORDER BY Date DESC LIMIT {}) SUB ORDER BY Date ASC '.format(symbol, count)
            data = pd.read_sql_query(data_query, engine)
        elif fetch_type is 'trade_sim':
            delete_query = 'DELETE FROM trading_simulation WHERE Symbol="{}" AND Date="{}"'.format(symbol,
                                                                                                   start_date)
            engine.execute(delete_query)
            count_query = 'SELECT COUNT(*) AS Count FROM trading_signals WHERE Symbol="{}" AND Date>="{}"' \
                .format(symbol, start_date)
            count = pd.read_sql_query(count_query, engine)['Count'][0]
            count += 1
            data_query = 'SELECT L.Symbol, L.Date, L.Close, L.EMA_Cross, R.Position, R.Shares, R.Bank  FROM ' \
                         '(SELECT A.ID, A.Symbol, A.Date, A.Close, B.EMA_Cross FROM data AS A, trading_signals ' \
                         'AS B WHERE A.Symbol = B.Symbol AND A.Date = B.Date AND A.Symbol="{}" ORDER BY Date ' \
                         'DESC LIMIT {}) AS L LEFT JOIN trading_simulation AS R ON L.Symbol = R.Symbol AND ' \
                         'L.Date=R.Date ORDER BY L.Date ASC '.format(symbol, count)
            data = pd.read_sql_query(data_query, engine)

    return data
