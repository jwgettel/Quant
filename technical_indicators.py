from quant_utilitiy import *


class TechnicalIndicators:
    def __init__(self, engine):
        self.engine = engine
        self.short_ma = 9
        self.long_ma = 21
        self.short_mo = 5
        self.long_mo = 12
        self.calculation_length = max(self.short_ma, self.long_ma, self.short_mo, self.long_mo)

    def calc_tech_indicators(self):
        symbols = get_symbols(self.engine, not_symbols='"FUT"')

        for symbol in symbols:
            start_date = get_start_date(self.engine, symbol, table='technical_indicators')

            data = get_data(self.engine, start_date, symbol, fetch_type='technical_indicators', length=self.
                            calculation_length)

            data['Short_MA'] = data['Close'].ewm(span=self.short_ma).mean()
            data['Long_MA'] = data['Close'].ewm(span=self.long_ma).mean()
            data['Short_MO'] = data['Close'].diff(self.short_mo)
            data['Long_MO'] = data['Close'].diff(self.long_mo)

            if start_date is None:
                calc_length = 0
            else:
                calc_length = self.calculation_length
            data = data.drop(data.index[:calc_length])
            data.to_sql('technical_indicators', self.engine, if_exists='append', index=False)
