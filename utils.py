import datetime
from influxdb import InfluxDBClient
import rfc3339

influx = InfluxDBClient('localhost', 8086, 'root', 'root', 'stock')
def price(year, month, day, entityId):
    start_date = datetime.datetime(year, month,day, 0, 0, 0)
    end_date = datetime.datetime(year, month,day, 23, 59, 59)
    result = list(influx.query("SELECT symbol, volume, title, high, low, open, close FROM stock_detail WHERE time > '%s' AND time < '%s' AND entityId = '%d'" % (rfc3339.rfc3339(start_date), rfc3339.rfc3339(end_date), entityId )).get_points())
    return result

def last_price(entityId):
    result = list(influx.query("SELECT last(sym) as symbol, volume, title, high, low, open, close FROM stock_detail WHERE entityId = '%d'" % (entityId )).get_points())
    return result


def open_list(year, month, day):
    start_date = datetime.datetime(year, month,day, 0, 0, 0)
    end_date = datetime.datetime(year, month,day, 23, 59, 59)
    result = list(influx.query("SELECT id FROM stock_detail WHERE time > '%s' AND time < '%s'" % (rfc3339.rfc3339(start_date), rfc3339.rfc3339(end_date) )).get_points())
    return result
    
def test():
    entities = influx.query('show tag values on stock with key = "entityId"').get_points()
    for entity in entities:
        entityId = entity['value']
        end_date = datetime.datetime.now()
        for i in range(1,2000):
            start_date = end_date
            end_date = end_date - datetime.timedelta(days = 1)

            count = influx.query("SELECT count(volume) FROM stock_detail WHERE time > '%s' AND time < '%s' AND entityId = '810'" % (rfc3339.rfc3339(end_date), rfc3339.rfc3339(start_date) )).raw
            if 'series' in count:
                _count = count['series'][0]['values'][0][1]
                if _count != 1:
                    print("Stock : %s => %s" % (entityId, count))