import pandas as pd


class TradeSimulation:
    def __init__(self, engine):
        self.engine = engine
        self.bank = 1000000

    def run_simulation(self):
        ticker_query = 'SELECT symbol FROM symbols WHERE type NOT IN ("FUT", "INDX")'  # change this when Derivs are added
        symbols = pd.read_sql_query(ticker_query, self.engine)

        for symbol in symbols['symbol']:

            start_date_query = 'SELECT MAX(Date) AS Date FROM trading_simulation WHERE Symbol="{}"'.format(symbol)
            start_date = pd.read_sql_query(start_date_query, self.engine)['Date'][0]

            if start_date is None:
                data_query = 'SELECT A.ID, A.Symbol, A.Date, A.Close, B.EMA_Cross FROM data AS A, trading_signals AS ' \
                             'B WHERE A.Symbol = B.Symbol AND A.Date=B.Date AND A.Symbol="{}"'.format(symbol)
                drop_length = 0
                data = pd.read_sql_query(data_query, self.engine)
                data['Position'] = [0] * len(data)
                data['Shares'] = [0] * len(data)
                data['Bank'] = [0] * len(data)
                data.loc[0, 'Bank'] = self.bank

            else:
                delete_query = 'DELETE FROM trading_simulation WHERE Symbol="{}" AND Date="{}"'.format(symbol,
                                                                                                       start_date)
                self.engine.execute(delete_query)
                count_query = 'SELECT COUNT(*) AS Count FROM trading_signals WHERE Symbol="{}" AND Date>="{}"'\
                    .format(symbol, start_date)
                count = pd.read_sql_query(count_query, self.engine)['Count'][0]
                count += 1
                data_query = 'SELECT L.Symbol, L.Date, L.Close, L.EMA_Cross, R.Position, R.Shares, R.Bank  FROM (SELECT A.ID, A.Symbol, A.Date, A.Close, B.EMA_Cross FROM data AS A, ' \
                             'trading_signals AS B WHERE A.ID = B.ID AND A.Symbol="{}" ORDER BY Date DESC LIMIT {}) ' \
                             'AS L LEFT JOIN trading_simulation AS R ON L.Symbol = R.Symbol AND L.Date=R.Date ORDER BY L.Date ASC '.format(symbol, count)
                drop_length = 1
                data = pd.read_sql_query(data_query, self.engine)

            for i in range(1, len(data)):
                if data['EMA_Cross'][i] == 10 and data['Position'][i-1] == 0 or \
                        data['EMA_Cross'][i] == -10 and data['Position'][i-1] == 0:
                    data.loc[i, 'Position'] = data['EMA_Cross'][i]
                    data.loc[i, 'Shares'] = data['Bank'][i-1] // data['Close'][i]
                    data.loc[i, 'Bank'] = data['Bank'][i-1] - (data['Shares'][i] * data['Close'][i])

                elif data['EMA_Cross'][i] == 1 and data['Position'][i-1] == 10 or \
                        data['EMA_Cross'][i] == -1 and data['Position'][i-1] == -10:
                    data.loc[i, 'Position'] = 0
                    data.loc[i, 'Shares'] = 0
                    data.loc[i, 'Bank'] = data['Bank'][i - 1] + (data['Shares'][i-1] * data['Close'][i])

                elif data['EMA_Cross'][i] == 10 and data['Position'][i - 1] == -10 or \
                        data['EMA_Cross'][i] == -10 and data['Position'][i - 1] == 10:
                    data.loc[i, 'Position'] = data['EMA_Cross'][i]
                    bank = data['Bank'][i - 1] + (data['Shares'][i - 1] * data['Close'][i])
                    data.loc[i, 'Shares'] = bank // data['Close'][i]
                    data.loc[i, 'Bank'] = bank - (data['Shares'][i] * data['Close'][i])

                else:
                    data.loc[i, 'Position'] = data.loc[i-1, 'Position']
                    data.loc[i, 'Shares'] = data.loc[i-1, 'Shares']
                    data.loc[i, 'Bank'] = data.loc[i-1, 'Bank']

            data = data.drop(data.index[:drop_length])
            data = data.drop(columns=['Close', 'EMA_Cross'])
            print(data)
#            data.to_sql('trading_simulation', self.engine, if_exists='append', index=False)
