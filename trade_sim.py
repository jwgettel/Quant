import pandas as pd


class TradeSimulation:
    def __init__(self, engine):
        self.engine = engine
        self.bank = 1000000

    def run_simulation(self):
        ticker_query = 'SELECT symbol FROM symbols WHERE type NOT IN ("FUT", "INDX")'  # change this when Derivs are added
        symbols = pd.read_sql_query(ticker_query, self.engine)

        for symbol in symbols['symbol']:
            data_query = 'SELECT A.ID, A.Symbol, A.Date, A.Close, B.EMA_Cross FROM data AS A, trading_signals AS B ' \
                         'WHERE A.ID = B.ID AND A.Symbol="{}"'.format(symbol)
            data = pd.read_sql_query(data_query, self.engine)

            data['Position'] = [0] * len(data)
            data['Shares'] = [0] * len(data)
            data['Bank'] = [0] * len(data)
            data.loc[0, 'Bank'] = self.bank

            print(data['Bank'][0])
            for i in range(1, len(data)):
                if data['EMA_Cross'][i] == 10 and data['Position'][i-1] == 0 or data['EMA_Cross'][i] == -10 and data['Position'][i-1] == 0:
                    data['Position'][i] = data['EMA_Cross'][i]
                    data['Shares'][i] = data['Bank'][i-1] // data['Close'][i]
                    data['Bank'][i] = data['Bank'][i-1] - (data['Shares'][i] * data['Close'][i])

                elif data['EMA_Cross'][i] == 1 and data['Position'][i-1] == 10 or data['EMA_Cross'][i] == -1 and data['Position'][i-1] == -10:
                    data['Position'][i] = 0
                    data['Shares'][i] = 0
                    data['Bank'][i] = data['Bank'][i - 1] + (data['Shares'][i-1] * data['Close'][i])

                elif data['EMA_Cross'][i] == 10 and data['Position'][i - 1] == -10 or data['EMA_Cross'][i] == -10 and data['Position'][i - 1] == 10:
                    data['Position'][i] = data['EMA_Cross'][i]
                    bank = data['Bank'][i - 1] + (data['Shares'][i - 1] * data['Close'][i])
                    data['Shares'][i] = bank // data['Close'][i]
                    data['Bank'][i] = bank - (data['Shares'][i] * data['Close'][i])

                else:
                    data.loc[i, 'Position'] = data.loc[i-1, 'Position']
                    data.loc[i, 'Shares'] = data.loc[i-1, 'Shares']
                    data.loc[i, 'Bank'] = data.loc[i-1, 'Bank']
