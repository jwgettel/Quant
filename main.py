from db_connection import DBConnection
from data_fetch import DataFetch

mysql_conn = DBConnection().mysql_engine()

data_fetch = DataFetch(mysql_conn)
data_fetch.get_equities()
#data_fetch.get_derivatives()