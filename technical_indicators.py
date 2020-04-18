import pandas as pd


class TechnicalIndicators:
    def __init__(self, engine):
        self.engine = engine
        self.short_ma = 9
        self.long_ma = 21
        self.short_mo = 5
        self.long_mo = 12
        self.calculation_length = max(self.short_ma, self.long_ma, self.short_mo, self.long_mo)

    def calc_tech_indicators(self):
        ticker_query = 'SELECT symbol FROM symbols WHERE type NOT IN ("FUT")' #change this when Derivs are added
        symbols = pd.read_sql_query(ticker_query, self.engine)

        for symbol in symbols['symbol']:

            start_date_query = 'SELECT MAX(Date) AS Date FROM technical_indicators WHERE Symbol="{}"'.format(symbol)
            start_date = pd.read_sql_query(start_date_query, self.engine)['Date'][0]

            if start_date is None:
                data_query = 'SELECT Date, Symbol, Close FROM data WHERE Symbol="{}"'.format(symbol)
                calc_length = 0
            else:
                delete_query = 'DELETE FROM technical_indicators WHERE Symbol="{}" AND Date="{}"'.format(symbol, start_date)
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
            data = data.drop(data.index[:calc_length])
            data.to_sql('technical_indicators', self.engine, if_exists='append', index=False)
