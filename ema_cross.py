import pandas as pd


class EMACross:
    def __init__(self, engine, short_ma, long_ma, short_mo, long_mo):
        self.engine = engine
        self.short_ma = short_ma
        self.long_ma = long_ma
        self.short_mo = short_mo
        self.long_mo = long_mo
        self.calculation_length = max(self.short_ma, self.long_ma, self.short_mo, self.long_mo)

    def calc_ema_stats(self):
        ticker_query = 'SELECT symbol FROM symbols WHERE type <> "FUT"'
        symbols = pd.read_sql_query(ticker_query, self.engine)

        for symbol in symbols['symbol']:

            start_date_query = 'SELECT MAX(Date) AS Date FROM ema_cross WHERE Symbol="{}"'.format(symbol)
            start_date = pd.read_sql_query(start_date_query, self.engine)['Date'][0]

            if start_date is None:
                data_query = 'SELECT Date, Symbol, Close FROM data WHERE Symbol="{}"'.format(symbol)
                calc_length = 0
            else:
                delete_query = 'DELETE FROM ema_cross WHERE Symbol="{}" AND Date="{}"'.format(symbol, start_date)
                self.engine.execute(delete_query)
                count_query = 'SELECT COUNT(*) AS Count FROM data WHERE Symbol="{}" AND Date>="{}"'.format(symbol,
                                                                                                           start_date)
                count = pd.read_sql_query(count_query, self.engine)['Count'][0]
                count += self.calculation_length
                data_query = 'SELECT * FROM (SELECT Date, Symbol, Close FROM data WHERE Symbol="{}" ORDER BY Date ' \
                             'DESC LIMIT {}) SUB ORDER BY Date ASC '.format(symbol, count)
                calc_length = self.calculation_length

            data = pd.read_sql_query(data_query, self.engine)
            data['Short_MA'] = data['Close'].ewm(span=self.short_ma).mean()
            data['Long_MA'] = data['Close'].ewm(span=self.long_ma).mean()
            data['Short_MO'] = data['Close'].diff(self.short_mo)
            data['Long_MO'] = data['Close'].diff(self.long_mo)
            data['Signal'] = [0] * len(data)
            for i in range(len(data)):
                if data['Short_MA'][i] > data['Long_MA'][i] and data['Short_MO'][i] > data['Long_MO'][i]:
                    data.loc[i, 'Signal'] = 1
                elif data['Short_MA'][i] < data['Long_MA'][i] and data['Short_MO'][i] < data['Long_MO'][i]:
                    data.loc[i, 'Signal'] = -1

            data = data.drop(data.index[:calc_length])
            data.to_sql('ema_cross', self.engine, if_exists='append', index=False)
