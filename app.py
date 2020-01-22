#!/usr/bin/python3

from scrapy.crawler import CrawlerProcess
from mse_indices import MseIndicesSpider
import os, sys
import pandas as pd
from config import engine

process = CrawlerProcess({
    'USER AGENT' : 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

process.crawl(MseIndicesSpider)
process.start()

# get the close status of the spider...if closed then quit
with open(os.path.join(sys.path[0], 'status.txt'), 'r') as file:
    status = file.readline().splitlines()[0]
if status == 'CLOSED':
    sys.exit(-1)

# load into data frame
file_path = os.path.join(sys.path[0], status)
# remove spaces
os.system("sed -i 's/,\ /,/g' {}".format(file_path))
df = pd.read_csv(file_path)
with open(os.path.join(sys.path[0], 'latest.txt'), 'r') as file:
    last_added_date = file.readline().splitlines()[0]

# date same as last added?
if df['Trade Date'][0] ==  last_added_date:
    sys.exit(-1)

# insert into database
df = df.head(n=1)

masi = pd.DataFrame(columns=["trading_date","close", "index_id", "exchange_id"])
dsi = pd.DataFrame(columns=["trading_date","close", "index_id", "exchange_id"])
fsi = pd.DataFrame(columns=["trading_date","close", "index_id", "exchange_id"])

masi.trading_date = df["Trade Date"]
masi.close = df["MASI"]
masi.index_id = 1 # TODO: get this from db instead of hardcoding
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
fsi.close = df["FSI "]
fsi.index_id = 2
fsi.exchange_id = 1 

print("[+] Inserting FSI values")
fsi.to_sql("index_prices", con=engine, if_exists='append', chunksize=1000, index=False)
print("[+] Inserting FSI values Done.")

# save last added date
with open(os.path.join(sys.path[0], 'latest.txt'), 'w') as file:
    file.write(df['Trade Date'][0])

