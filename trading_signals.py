from quant_utilitiy import get_start_date, get_symbols, get_data


class TradingSignals:

    def __init__(self, engine):
        self.engine = engine

    def calc_signals(self):
        symbols = get_symbols(self.engine, not_symbols='"FUT", "IND"')

        for symbol in symbols:
            start_date = get_start_date(self.engine, symbol, table='trading_signals')

            data = get_data(self.engine, start_date, symbol, fetch_type='trading_signals')

            data['EMA_Cross'] = [None] * len(data)
            data['Strategy'] = ['EMA_Cross'] * len(data)
            for i in range(len(data)):
                if data['Short_MA'][i] > data['Long_MA'][i] and data['Short_MO'][i] > data['Long_MO'][i]:
                    data.loc[i, 'EMA_Cross'] = 10
                elif data['Close'][i] < data['Short_MA'][i]:
                    data.loc[i, 'EMA_Cross'] = 1
                elif data['Short_MA'][i] < data['Long_MA'][i] and data['Short_MO'][i] < data['Long_MO'][i]:
                    data.loc[i, 'EMA_Cross'] = -10
                elif data['Close'][i] > data['Short_MA'][i]:
                    data.loc[i, 'EMA_Cross'] = -1

            data = data.drop(columns=['ID', 'Close', 'Short_MA', 'Long_MA', 'Short_MO', 'Long_MO'])
            data.to_sql('trading_signals', self.engine, if_exists='append', index=False)
