# customer-api

### In order to create conda enviroment type the following commnad

#### conda create --name envname --file requirements.txt

### In order to run the api type the following command

#### python api.py

### After you the api is running you can load cache which takes about 25 minutes
#### conda install -c anaconda requests 
#### python loader.py

### Statistics endpoints
#### /api/statistics/clicks/event/device/timestamp
#### /api/statistics/clicks/event/locale/timestamp
#### /api/statistics/time/event/device/timestamp
#### /api/statistics/time/event/locale/timestamp
#### event = ['share', 'convert']
#### timestamp = ['lastday', 'thisweek', 'thismonth']

```json
{
{ "Mobile":{
    "filtered":{
        "mean": 12.1,
        "values": [123, 31, 10, 12, 123]
        }
    "notFiltered":{
        "mean": 11.1,
        "values": [32, 31, 10, 12, 32]
        }
   }
   "Desktop":{
    "filtered":{
        "mean": 12.1,
        "values": [123, 31, 10, 12, 123]
        }
    "notFiltered":{
        "mean": 11.1,
        "values": [32, 31, 10, 12, 32]
        }
   }
}
```
#### You can plot filtered and notFiltered values in the same graph
#### Example
![image](https://user-images.githubusercontent.com/64483300/204863439-833b2ef0-cabc-48e3-ab3c-dd4c0aa1f25b.png)


### Metrics endpoints
#### /api/metrics/<click_type>/device
#### click_type = [totalClicks, totalConversions, totalShares]
#### ?timeframe= [day, week, month] default is day
#### Example /api/metrics/totalClicks/device?timeframe=day

```json
{
    "Mobile" : [{"period": "12AM", "value":12}, {"period": "8AM", "value":13}]
    "Desktop" : [{"period": "12AM", "value":12}, {"period": "8AM", "value":13}]
}
```
