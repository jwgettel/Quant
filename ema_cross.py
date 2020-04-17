import pandas_datareader.data as dr
import pandas as pd

class EMACross:
    def __init__(self, engine, short_ma, long_ma, short_mo, long_mo):
        self.engine = engine
        self.short_ma = short_ma
        self.long_ma = long_ma
        self.short_mo = short_mo
        self.long_mo = long_mo

    def calc_ema_stats(self):
        ticker_query = 'SELECT symbol FROM symbols WHERE type <> "FUT"'
        symbols = pd.read_sql_query(ticker_query, self.engine)

        for symbol in symbols['symbol']:
            data_query = 'SELECT Date, Symbol, Close FROM data WHERE Symbol="{}"'.format(symbol)
            data = pd.read_sql_query(data_query, self.engine)
            data['Short_MA'] = data['Close'].ewm(span=self.short_ma).mean()
            data['Long_MA'] = data['Close'].ewm(span=self.long_ma).mean()
            data['Short_MO'] = data['Close'].diff(self.short_mo)
            data['Long_MO'] = data['Close'].diff(self.long_mo)
            data['Signal'] = [0] * len(data)

            for i in range(len(data)):
                if data['Short_MA'][i] > data['Long_MA'][i]: #and data['Short_MO'][i] > data['Long_MO'][i]:
                    data.loc[i, 'Signal'] = 1
                elif data['Short_MA'][i] < data['Long_MA'][i]: # and data['Short_MO'][i] < data['Long_MO'][i]:
                    data.loc[i, 'Signal'] = -1

            data.to_sql('ema_cross', self.engine, if_exists='append', index=False)
