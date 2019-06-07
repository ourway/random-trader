import datetime
from influxdb import InfluxDBClient 
import rfc3339
import random
import utils

influx = InfluxDBClient('localhost', 8086, 'root', 'root', 'stock')

entities = list(influx.query('show tag values on stock with key = "entityId"').get_points())

entity_size = (len(entities))

start_date = datetime.datetime.now() - datetime.timedelta(days = 365)

wallet = 10000000
stock = {}
for i in range(1,100) :

    print("%d : %s Started" % (i, start_date))
    sell = ( random.randint(0,2) == 0)
    open_list = utils.open_list(start_date.year, start_date.month, start_date.day)
    if len(stock) != 0 and sell :
        _random = random.randint(0, len(stock)-1)
        print("%d : %s Selling : %s" % (i, start_date, stock[list(stock.keys())[_random]]))
    elif len(open_list) != 0 :
        #print("%d : %s Opens : %s" % (i, start_date, open_list))
        _random = random.randint(0, len(open_list)-1)
        print(_random)
        print("%d : %s Buying : %s" % (i, start_date, open_list[_random]))
        entity_id = open_list[_random]['id']
        price = utils.price(start_date.year, start_date.month, start_date.day, entity_id)
        if wallet > price[0]['high'] :
            cache = random.randint(0, wallet - 1)
            count = (int)(cache / price[0]['high'])
            print("%d : %s Price : %s , Count : %d" % (i, start_date, price, count))
            wallet = wallet - count * price[0]['high']
            if entity_id not in stock:
                    stock[entity_id] = 0
            stock[entity_id] = count
    start_date = start_date + datetime.timedelta(days = 1)
print("Stock : %s" % stock)
print("Wallet : %d" % wallet)
wealth = 0
for entityId in stock.keys():
    wealth += stock[entityId] * utils.last_price(entityId)[0]['low']

print("Wealth : %d" % wealth)