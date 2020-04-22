from quant_utilitiy import get_start_date, get_symbols, get_data


class TradeSimulation:
    def __init__(self, engine):
        self.engine = engine
        self.bank = 1000000

    def run_simulation(self):
        symbols = get_symbols(self.engine, not_symbols='"FUT", "IND"')

        for symbol in symbols:
            start_date = get_start_date(self.engine, symbol, table='trading_simulation')

            data = get_data(self.engine, start_date, symbol, fetch_type='trade_sim')

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

            if start_date is None:
                drop_length = 0
            else:
                drop_length = 1
            data = data.drop(data.index[:drop_length])
            data = data.drop(columns=['Close', 'EMA_Cross'])
            data.to_sql('trading_simulation', self.engine, if_exists='append', index=False)
