import sqlalchemy as sal


class DBConnection:

    def mysql_engine(self):

        conn_str = 'mysql+pymysql://root:@localhost:3306/quant'
        engine = sal.create_engine(conn_str)
        return engine
