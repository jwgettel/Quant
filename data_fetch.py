import pandas_datareader.data as dr
import pandas as pd
from datetime import datetime, date

class DataFetch:
    def __init__(self, engine):
        self.engine = engine
        self.datasource = 'yahoo'
        self.start = '2000-01-01'
        self.end = datetime.now().strftime('%Y-%m-%d')

    def get_equities(self):
        ticker_query = 'SELECT symbol FROM symbols WHERE type <> "FUT"'
        symbols = pd.read_sql_query(ticker_query, self.engine)

        for symbol in symbols['symbol']:
            start_date_query = 'SELECT MAX(Date) AS Date FROM data WHERE Symbol="{}"'.format(symbol)
            start_date = pd.read_sql_query(start_date_query, self.engine)['Date'][0]

            if start_date is None:
                start = self.start
            else:
                delete_query = 'DELETE FROM data WHERE Symbol="{}" AND Date="{}"'.format(symbol, start_date)
                self.engine.execute(delete_query)
                start = start_date

            data = dr.DataReader(symbol, self.datasource, start, self.end)
            symbol = [symbol] * len(data)
            data['Symbol'] = symbol
            #data = data.reset_index()

            data.to_sql('data', self.engine, if_exists='append')






    def get_derivatives(self):
        ticker_query = 'SELECT symbol, expiration FROM symbols WHERE type in ("FUT","OPT")'
        symbols = pd.read_sql_query(ticker_query, self.engine)
        for symbol in symbols['symbol']:
            for year in range(20, 21):
                for month in symbols['expiration'][0]:
                    contract = symbol+month+str(year)+'.CME'
                    data = dr.DataReader(contract, self.datasource, self.start, self.end)
                    print(data)


