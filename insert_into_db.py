import pandas as pd

from config import engine

df = pd.read_csv("indices.csv")

masi = pd.DataFrame(columns=["trading_date","close", "index_id", "exchange_id"])
dsi = pd.DataFrame(columns=["trading_date","close", "index_id", "exchange_id"])
fsi = pd.DataFrame(columns=["trading_date","close", "index_id", "exchange_id"])

masi.trading_date = df["Trade Date"]
masi.close = df["MASI"]
masi.index_id = 1
masi.exchange_id = 1

print("[+] Inserting MASI values")
masi.to_sql("index_prices", con=engine, if_exists='append', chunksize=1000, index=False)
print("[+] Inserting MASI values Done.")

dsi.trading_date = df["Trade Date"]
dsi.close = df["DSI"]
dsi.index_id = 3
dsi.exchange_id = 1

print("[+] Inserting DSI values")
dsi.to_sql("index_prices", con=engine, if_exists='append', chunksize=1000, index=False)
print("[+] Inserting DSI values Done.")

fsi.trading_date = df["Trade Date"]
fsi.close = df["FSI"]
fsi.index_id = 2
fsi.exchange_id = 1

print("[+] Inserting FSI values")
fsi.to_sql("index_prices", con=engine, if_exists='append', chunksize=1000, index=False)
print("[+] Inserting FSI values Done.")