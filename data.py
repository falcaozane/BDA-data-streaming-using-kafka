import yfinance as yf
import json
from time import sleep
import sys
import time


if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

from kafka import KafkaProducer

ticker = "ZOMATO.NS"
# stock = yf.Ticker(ticker)
# hist = stock.history(period="1d" ,interval='1m')
# hist["Timestamp"] = hist.index

producer  = KafkaProducer(bootstrap_servers="172.18.149.74:9092",value_serializer=lambda x: json.dumps(x).encode("utf-8"))
counter = 0
while counter<=25:
    
    zomato = {}
    stock = yf.Ticker(ticker)
    hist = stock.history(period="1d" ,interval='1m')
    hist["Timestamp"] = hist.index
    ## If random data is required of the stock of latest market date
    zomato["Price"] = json.loads(hist.sample().to_json(orient="records"))[0]["High"]
    
    #return latest price
    
    #zomato["Price"] = stock.fast_info.last_price
    
    zomato["Name"] = "ZOMATO.NS"
    zomato["Timestamp"] = round(time.time() * 1000)
    print(zomato)
    producer.send("market",value=zomato)
    sleep(12)
    counter+=1