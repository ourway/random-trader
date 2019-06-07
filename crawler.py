import urllib.request, json
import datetime
from influxdb import InfluxDBClient

influx = InfluxDBClient('localhost', 8086, 'root', 'root', 'stock')


req = urllib.request.Request(url='https://rahavard365.com/api/search/items?type=asset', method='GET')
res = urllib.request.urlopen(req, timeout=5)
res_body = res.read()
# https://docs.python.org/3/library/json.html
data = json.loads(res_body.decode("utf-8"))
for stock in data['data'] : 
	if stock['type'] == 'سهام':
		try :
			#{'type': 'اختیار معامله', 'entity_id': '5042', 'entity_type': 'asset', 'trade_symbol': 'طشنا7028', 'title': 'اختیارف شپنا-9500-1397/07/22', 'exchange': 'بورس تهران'}
			#print("id=%s,type=%s,symbol=%s,title=%s" % (stock['entity_id'], stock['entity_type'],stock['trade_symbol'], stock['title']))
			now = datetime.datetime.now()
			start_epoch = ((int)((now - datetime.timedelta(days = 730)).timestamp()))
			end_epoch = ((int)((now - datetime.timedelta(days = 365)).timestamp()))
			detail_url = "https://rahavard365.com/api/chart/bars?ticker=exchange.asset:%s:real_close&resolution=D&startDateTime=%d&endDateTime=%d&firstDataRequest=true" %(stock['entity_id'], start_epoch, end_epoch)
			detail_req = urllib.request.Request( url = detail_url, method= 'GET')
			detail_res = urllib.request.urlopen(detail_req, timeout= 10)
			detail_res_body = detail_res.read()
			detail_data = json.loads(detail_res_body.decode("utf-8"))
			for detail in detail_data : 
				detail_time = datetime.datetime.fromtimestamp( (int)(detail['time'] / 1000))
				detail_iso_time = detail_time.strftime("%Y%m%dT%H%M%SZ")
				detail['id'] = (int)(stock['entity_id'])
				detail['sym'] = stock['trade_symbol']	
				detail['title'] = stock['title']	
				detail['date'] = detail_time.strftime("%Y-%m-%d")
				json_body = [
					{
						"measurement": "stock_detail",
						"tags": {
							"symbol": stock['trade_symbol'],
							"entityId": (int)(stock['entity_id'])
						},
						"time": detail_iso_time,
						"fields": detail
						}
				]
				influx.write_points(json_body)
		except : 
			print(detail_url)