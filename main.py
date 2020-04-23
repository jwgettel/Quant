from db_connection import DBConnection
from data_fetch import DataFetch
from technical_indicators import TechnicalIndicators
from trading_signals import TradingSignals
from trade_sim import TradeSimulation
from datetime import datetime

start = datetime.now()
mysql_conn = DBConnection().mysql_engine()

data_fetch = DataFetch(mysql_conn)
data_fetch.get_equities()
data_fetch.get_dividends()
#data_fetch.get_derivatives()

tech_ind = TechnicalIndicators(mysql_conn)
tech_ind.calc_tech_indicators()

trade_sig = TradingSignals(mysql_conn)
trade_sig.calc_signals()

simulation = TradeSimulation(mysql_conn)
simulation.run_simulation()

print(datetime.now()-start)

