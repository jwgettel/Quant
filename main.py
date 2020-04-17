from db_connection import DBConnection
from data_fetch import DataFetch
from ema_cross import EMACross

mysql_conn = DBConnection().mysql_engine()

data_fetch = DataFetch(mysql_conn)
data_fetch.get_equities()

ema = EMACross(mysql_conn, 9, 21, 5, 12)
ema.calc_ema_stats()

#data_fetch.get_derivatives()
