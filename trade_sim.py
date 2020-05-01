from quant_utilitiy import *


class TradeSimulation:
    def __init__(self, engine):
        self.engine = engine
        self.bank = 1000000

    def run_simulation(self):
        symbols = get_symbols(self.engine, not_symbols='"FUT", "IND"')
        strategies = get_strategies(self.engine)

        for symbol in symbols:
            for strategy in strategies:

                '''Signal and Position are messed up... fix these'''

                start_date = get_start_date(self.engine, symbol, table='trading_simulation', strategy=strategy)

                data = get_data(self.engine, start_date, symbol, fetch_type='trade_sim', strategy=strategy)

                for i in range(1, len(data)):
                    dividend_payment = 0
                    if not pd.isnull(data['Dividend'][i - 1]):
                        dividend_payment = data['Shares'][i - 1] * data['Dividend'][i - 1]

                    if data['Signal'][i] == 10 and data['Position'][i - 1] == 0 or \
                            data['Signal'][i] == -10 and data['Position'][i - 1] == 0:
                        data.loc[i, 'Position'] = data['Signal'][i]
                        data.loc[i, 'Shares'] = data['Bank'][i - 1] // data['Close'][i]
                        data.loc[i, 'Bank'] = data['Bank'][i - 1] - (
                                    data['Shares'][i] * data['Close'][i]) + dividend_payment

                    elif data['Signal'][i] == 1 and data['Position'][i - 1] == 10 or \
                            data['Signal'][i] == -1 and data['Position'][i - 1] == -10:
                        data.loc[i, 'Position'] = 0
                        data.loc[i, 'Shares'] = 0
                        data.loc[i, 'Bank'] = data['Bank'][i - 1] + (
                                    data['Shares'][i - 1] * data['Close'][i]) + dividend_payment

                    elif data['Signal'][i] == 10 and data['Position'][i - 1] == -10 or \
                            data['Signal'][i] == -10 and data['Position'][i - 1] == 10:
                        data.loc[i, 'Position'] = data['Signal'][i]
                        bank = data['Bank'][i - 1] + (data['Shares'][i - 1] * data['Close'][i])
                        data.loc[i, 'Shares'] = bank // data['Close'][i]
                        data.loc[i, 'Bank'] = bank - (data['Shares'][i] * data['Close'][i]) + dividend_payment

                    else:
                        data.loc[i, 'Position'] = data['Position'][i]
                        new_bank = data.loc[i - 1, 'Bank'] + dividend_payment
                        new_shares = new_bank // data['Close'][i]
                        data.loc[i, 'Shares'] = data['Shares'][i - 1] + new_shares
                        data.loc[i, 'Bank'] = new_bank - (new_shares * data['Close'][i])

                    '''elif data['Signal'][i] == -1 and data['Position'][i - 1] == 10 or \
                            data['Signal'][i] == 0 and data['Position'][i - 1] == 10 or \
                            data['Signal'][i] == 1 and data['Position'][i - 1] == -10 or \
                            data['Signal'][i] == 10 and data['Position'][i - 1] == 10 or \
                            data['Signal'][i] == -10 and data['Position'][i - 1] == -10 or \
                            data['Signal'][i] == 0 and data['Position'][i - 1] == -10:
                        data.loc[i, 'Position'] = data.loc[i - 1, 'Position']
                        data.loc[i, 'Shares'] = data.loc[i - 1, 'Shares']
                        data.loc[i, 'Bank'] = data.loc[i - 1, 'Bank'] + dividend_payment'''

                if start_date is None:
                    drop_length = 0
                else:
                    drop_length = 1
                data = data.drop(data.index[:drop_length])
                data = data.drop(columns=['Close', 'Dividend', 'Signal'])
                data.to_sql('trading_simulation', self.engine, if_exists='append', index=False)

    def run_buy_hold_simulation(self):
        pass

