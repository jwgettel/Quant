CREATE TABLE symbols (ID BIGINT NOT NULL AUTO_INCREMENT, Symbol VACHCHAR(100), Type VARCHAR(100), Expiration VARCHAR(100), PRIMARY KEY(ID));


CREATE TABLE data (ID BIGINT NOT NULL AUTO_INCREMENT, Symbol VARCHAR(100), Date DATETIME, High FLOAT, Low FLOAT, Open FLOAT, Close FLOAT, Volume INT, `Adj Close` Float, PRIMARY KEY(ID));


CREATE TABLE technical_indicators (ID BIGINT NOT NULL AUTO_INCREMENT, Symbol VARCHAR(100), Date DATETIME, Close FLOAT, Short_MA FLOAT, Long_MA FLOAT, Short_MO FLOAT, Long_MO FLOAT, PRIMARY KEY(ID));


CREATE TABLE trading_signals (ID BIGINT NOT NULL AUTO_INCREMENT, Symbol VARCHAR(100), Date DATETIME, EMA_Cross INT, PRIMARY KEY(ID));


CREATE TABLE trading_simulation (ID BIGINT NOT NULL AUTO_INCREMENT, Symbol VARCHAR(100), Date DATETIME, Position INT, Shares INT, Bank FLOATE, PRIMARY KEY(ID));


INSERT INTO symbols (Symbol, Type) VALUES ('SPY', 'ETF')
INSERT INTO symbols (Symbol, Type) VALUES ('AAPL', 'EQU')
INSERT INTO symbols (Symbol, Type) VALUES ('^VIX', 'IND')
INSERT INTO symbols (Symbol, Type) VALUES ('GBPUSD=X', 'CUR')