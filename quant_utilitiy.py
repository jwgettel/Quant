import pandas as pd


def get_start_date(engine, symbol):
    start_date_query = 'SELECT MAX(Date) AS Date FROM data WHERE Symbol="{}"'.format(symbol)
    return pd.read_sql_query(start_date_query, engine)['Date'][0]


def get_symbols(engine, not_symbols=""):
    symbol_query = 'SELECT symbol FROM symbols WHERE type NOT IN ({})'.format(not_symbols)
    return pd.read_sql_query(symbol_query, engine)['symbol']
