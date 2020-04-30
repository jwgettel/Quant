from quant_utilitiy import *


class TradingSignals:

    def __init__(self, engine):
        self.engine = engine

    def calc_signals(self):
        symbols = get_symbols(self.engine, not_symbols='"FUT", "IND"')
        strategies = get_strategies(self.engine)

        for symbol in symbols:
            for stragey in strategies:
                start_date = get_start_date(self.engine, symbol, table='trading_signals')

                data = get_data(self.engine, start_date, symbol, fetch_type='trading_signals')

                data['Signal'] = [None] * len(data)
                data['Strategy'] = [stragey] * len(data)
                for i in range(len(data)):
                    if data['Short_MA'][i] > data['Long_MA'][i] and data['Short_MO'][i] > data['Long_MO'][i]:
                        data.loc[i, 'Signal'] = 10
                    elif data['Close'][i] < data['Short_MA'][i]:
                        data.loc[i, 'Signal'] = 1
                    elif data['Short_MA'][i] < data['Long_MA'][i] and data['Short_MO'][i] < data['Long_MO'][i]:
                        data.loc[i, 'Signal'] = -10
                    elif data['Close'][i] > data['Short_MA'][i]:
                        data.loc[i, 'Signal'] = -1

                data = data.drop(columns=['ID', 'Close', 'Short_MA', 'Long_MA', 'Short_MO', 'Long_MO'])
                data.to_sql('trading_signals', self.engine, if_exists='append', index=False)
