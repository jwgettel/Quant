import pandas as pd


class TradingSignals:

    def __init__(self, engine):
        self.engine = engine

    def calc_signals(self):
        ticker_query = 'SELECT symbol FROM symbols WHERE type NOT IN ("FUT", "INDX")'
        symbols = pd.read_sql_query(ticker_query, self.engine)

        for symbol in symbols['symbol']:

            start_date_query = 'SELECT MAX(Date) AS Date FROM trading_signals WHERE Symbol="{}"'.format(symbol)
            start_date = pd.read_sql_query(start_date_query, self.engine)['Date'][0]

            if start_date is None:
                data_query = 'SELECT * FROM technical_indicators WHERE Symbol="{}"'.format(symbol)
                calc_length = 0
            else:
                delete_query = 'DELETE FROM trading_signals WHERE Symbol="{}" AND Date="{}"'.format(symbol, start_date)
                self.engine.execute(delete_query)
                count_query = 'SELECT COUNT(*) AS Count FROM technical_indicators WHERE Symbol="{}" AND Date>="{}"'.format(
                    symbol,
                    start_date)
                count = pd.read_sql_query(count_query, self.engine)['Count'][0]
                count += self.calculation_length
                data_query = 'SELECT * FROM (SELECT Date, Symbol, Close FROM technical_indicators WHERE Symbol="{}" ORDER BY Date ' \
                             'DESC LIMIT {}) SUB ORDER BY Date ASC '.format(symbol, count)
                calc_length = self.calculation_length

            data = pd.read_sql_query(data_query, self.engine)
            data['EMA_Cross'] = [None] * len(data)
            for i in range(len(data)):
                if data['Short_MA'][i] > data['Long_MA'][i] and data['Short_MO'][i] > data['Long_MO'][i]:
                    data.loc[i, 'EMA_Cross'] = 10
                elif data['Close'][i] < data['Short_MA'][i]:
                    data.loc[i, 'EMA_Cross'] = 1
                elif data['Short_MA'][i] < data['Long_MA'][i] and data['Short_MO'][i] < data['Long_MO'][i]:
                    data.loc[i, 'EMA_Cross'] = -10
                elif data['Close'][i] > data['Short_MA'][i]:
                    data.loc[i, 'EMA_Cross'] = -1

            print(data)
