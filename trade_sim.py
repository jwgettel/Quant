import pandas as pd


class TradeSimulation:
    def __init__(self, engine):
        self.engine = engine
        self.bank = 1000000

    def run_simulation(self):
        ticker_query = 'SELECT symbol FROM symbols WHERE type NOT IN ("FUT", "INDX")'  # change this when Derivs are added
        symbols = pd.read_sql_query(ticker_query, self.engine)

        for symbol in symbols['symbol']:
            data_query = 'SELECT * FROM trading_signals WHERE Symbol="{}"'.format(symbol)
            data = pd.read_sql_query(data_query, self.engine)

            for i in range(len(data)):
                bank = self.bank
                if data['EMA_Cross'][i] == 10:
                    pass








            print(data)

