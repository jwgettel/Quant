import pandas_datareader.data as dr
from quant_utilitiy import *


class DataFetch:
    def __init__(self, engine):
        self.engine = engine
        self.data_source = 'yahoo'
        self.div_data_source = 'yahoo-dividends'
        self.start = '2000-01-01'
        self.end = datetime.now().strftime('%Y-%m-%d')

    def get_equities(self):

        symbols = get_symbols(self.engine, not_symbols='"FUT"')

        for symbol in symbols:
            start_date = get_start_date(self.engine, symbol, table='data')

            if start_date is None:
                start = self.start
            else:
                delete_query = 'DELETE FROM data WHERE Symbol="{}" AND Date="{}"'.format(symbol, start_date)
                self.engine.execute(delete_query)
                start = start_date

            data = dr.DataReader(symbol, self.data_source, start, self.end)
            symbol_list = [symbol] * len(data)
            data['Symbol'] = symbol_list
            data.to_sql('data', self.engine, if_exists='append')

    def get_dividends(self):

        symbols = get_symbols(self.engine, not_symbols='"FUT", "IND"')

        for symbol in symbols:
            start = get_start_date(self.engine, symbol, table='data', dividend=True)
            if start is None:
                start = self.start

            dividends = dr.DataReader(symbol, self.div_data_source, start, self.end)
            symbol_list = [symbol] * len(dividends)
            dividends['Symbol'] = symbol_list
            dividends = dividends.drop(columns=['action']).sort_index()
            dividends = dividends.reset_index()

            for i in range(len(dividends)):
                date = dividends['index'][i].strftime('%Y-%m-%d')
                symbol = dividends['Symbol'][i]
                dividend = dividends['value'][i]
                dividend_insert_query = 'UPDATE data SET Dividend={} WHERE Date="{}" AND Symbol="{}"'.format(dividend, date, symbol)
                self.engine.execute(dividend_insert_query)

    def get_derivatives(self):
        # THIS DOESN'T WORK
        ticker_query = 'SELECT symbol, expiration FROM symbols WHERE type in ("FUT","OPT")'
        symbols = pd.read_sql_query(ticker_query, self.engine)
        for symbol in symbols['symbol']:
            for year in range(19, 20):
                for month in symbols['expiration'][0]:
                    contract = symbol+month+str(year)+'.CME'
                    data = dr.DataReader(contract, self.datasource, self.start, self.end)
                    print(data)
