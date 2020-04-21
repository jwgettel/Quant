import pandas_datareader.data as dr
import pandas as pd
from datetime import datetime
from quant_utilitiy import get_start_date, get_symbols


class DataFetch:
    def __init__(self, engine):
        self.engine = engine
        self.datasource = 'yahoo'
        self.start = '2000-01-01'
        self.end = datetime.now().strftime('%Y-%m-%d')

    def get_equities(self):

        symbols = get_symbols(self.engine, not_symbols='"FUT"')

        for symbol in symbols:
            start_date = get_start_date(self.engine, symbol)

            if start_date is None:
                start = self.start
            else:
                delete_query = 'DELETE FROM data WHERE Symbol="{}" AND Date="{}"'.format(symbol, start_date)
                self.engine.execute(delete_query)
                start = start_date

            data = dr.DataReader(symbol, self.datasource, start, self.end)
            symbol = [symbol] * len(data)
            data['Symbol'] = symbol

            data.to_sql('data', self.engine, if_exists='append')

    def get_derivatives(self):
        ticker_query = 'SELECT symbol, expiration FROM symbols WHERE type in ("FUT","OPT")'
        symbols = pd.read_sql_query(ticker_query, self.engine)
        for symbol in symbols['symbol']:
            for year in range(19, 20):
                for month in symbols['expiration'][0]:
                    contract = symbol+month+str(year)+'.CME'
                    data = dr.DataReader(contract, self.datasource, self.start, self.end)
                    print(data)


