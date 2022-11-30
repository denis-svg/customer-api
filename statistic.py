from flask import Blueprint, request, jsonify
from cache import cache
from connection import getConnection
import azure.filters as flt
from statistics import mean

statistic = Blueprint('statistics', __name__)

@statistic.route('/api/statistics/clicks/<event>/device/<timestamp>')
@cache.cached(timeout=100000, query_string=True)
def statisticsClicksDeviceView(event: str, timestamp: str):
    if request.method == 'GET':
        available_events = ["Convert", "Share"]
        event = event.lower()
        if event not in list(map(lambda c : c.lower(), available_events)):
            return "Invalid click", 400

        available_timestamps = ["lastday", "thisweek", "thismonth"]
        timestamp = timestamp.lower()
        if timestamp not in available_timestamps:
            return "Invalid timestamp", 400

        events = {
            "convert" : "conversion",
             "share" : "share_experience",
        }

        e_name = events[event]

        timestamps = {
            "lastday": 0,
            "thisweek": 6,
            "thismonth": 29,
        }
        
        day = timestamps[timestamp]

        cnxn = getConnection()
        cursor = cnxn.cursor()
        
        devices = ["Desktop", "Mobile"]

        out = {} 

        for device in devices:
            events = cursor.execute(f"""DECLARE @day AS INT
										SET @day = (SELECT DATEPART(dy, clicked_date)
													FROM Events
													WHERE clicked_date = (SELECT MAX(clicked_date)
													FROM Events))
										SELECT Events.person_id,
												Events.event_name
										FROM Events
										INNER JOIN Persons
										ON Persons.person_id = Events.person_id
										WHERE DATEPART(dy, clicked_date) >= @day - ? AND device=?
										ORDER BY clicked_date ASC""", day, device).fetchall()
            
            person_dict = {}

            for event in events:
                event_name = event[1]
                person_id = event[0]

                if person_id not in person_dict:
                    person_dict[person_id] = [event_name]
                else:
                    person_dict[person_id].append(event_name)
            
            values = []
            
            for person_id in person_dict.keys():
                counter = 0
                for event_name in person_dict[person_id]:
                    if event_name == e_name:
                        values.append(counter)
                        break
                    counter += 1
            
            out[device] = {"notFiltered":{"values":values, "mean":mean(values)}, 
                            "filtered":{"values":flt.filter3(values), "mean":mean(flt.filter3(values))}}

        cnxn.close()

        return jsonify(out)

@statistic.route('/api/statistics/clicks/<event>/locale/<timestamp>')
@cache.cached(timeout=100000, query_string=True)
def statisticsClicksLocaleView(event: str, timestamp: str):
    if request.method == 'GET':
        available_events = ["Convert", "Share"]
        event = event.lower()
        if event not in list(map(lambda c : c.lower(), available_events)):
            return "Invalid click", 400

        available_timestamps = ["lastday", "thisweek", "thismonth"]
        if timestamp.lower() not in available_timestamps:
            return "Invalid timestamp", 400

        events = {
            "convert" : "conversion",
             "share" : "share_experience",
        }

        e_name = events[event]

        timestamps = {
            "lastday": 0,
            "thisweek": 6,
            "thismonth": 29,
        }
        
        day = timestamps[timestamp]

        cnxn = getConnection()
        cursor = cnxn.cursor()
        
        locales = cursor.execute(f"""SELECT 	TOP(?)
												locale
										FROM Persons
										GROUP BY locale
										ORDER BY COUNT(locale) DESC""", 6).fetchall()

        out = {} 

        for locale in locales:
            events = cursor.execute(f"""DECLARE @day AS INT
										SET @day = (SELECT DATEPART(dy, clicked_date)
													FROM Events
													WHERE clicked_date = (SELECT MAX(clicked_date)
													FROM Events))
										SELECT Events.person_id,
												Events.event_name
										FROM Events
										INNER JOIN Persons
										ON Persons.person_id = Events.person_id
										WHERE DATEPART(dy, clicked_date) >= @day - ? AND locale=?
										ORDER BY clicked_date ASC""", day, locale[0]).fetchall()
            
            person_dict = {}

            for event in events:
                event_name = event[1]
                person_id = event[0]

                if person_id not in person_dict:
                    person_dict[person_id] = [event_name]
                else:
                    person_dict[person_id].append(event_name)
            
            values = []
            
            for person_id in person_dict.keys():
                counter = 0
                for event_name in person_dict[person_id]:
                    if event_name == e_name:
                        values.append(counter)
                        break
                    counter += 1
            
            if len(values) > 0:
                out[locale[0]] = {"notFiltered":{"values":values, "mean":mean(values)}, 
                                "filtered":{"values":flt.filter3(values), "mean":mean(flt.filter3(values))}}

        cnxn.close()

        return jsonify(out)

@statistic.route('/api/statistics/time/<event>/device/<timestamp>')
@cache.cached(timeout=100000, query_string=True)
def statisticsTimeDeviceView(event: str, timestamp: str):
    if request.method == 'GET':
        available_events = ["Convert", "Share"]
        event = event.lower()
        if event not in list(map(lambda c : c.lower(), available_events)):
            return "Invalid click", 400

        available_timestamps = ["lastday", "thisweek", "thismonth"]
        timestamp = timestamp.lower()
        if timestamp not in available_timestamps:
            return "Invalid timestamp", 400

        events = {
            "convert" : "conversion",
             "share" : "share_experience",
        }

        e_name = events[event]

        timestamps = {
            "lastday": 0,
            "thisweek": 6,
            "thismonth": 29,
        }
        
        day = timestamps[timestamp]

        cnxn = getConnection()
        cursor = cnxn.cursor()
        
        devices = ["Desktop", "Mobile"]

        out = {} 

        for device in devices:
            events = cursor.execute(f"""DECLARE @day AS INT
										SET @day = (SELECT DATEPART(dy, clicked_date)
													FROM Events
													WHERE clicked_date = (SELECT MAX(clicked_date)
													FROM Events))
										SELECT Events.person_id,
												Events.event_name,
												Events.clicked_date
										FROM Events
										INNER JOIN Persons
										ON Persons.person_id = Events.person_id
										WHERE DATEPART(dy, clicked_date) >= @day - ? AND device=?
										ORDER BY clicked_date ASC""", day, device).fetchall()
            
            person_dict = {}

            for event in events:
                event_name = event[1]
                person_id = event[0]
                date = event[2]

                if person_id not in person_dict:
                    person_dict[person_id] = [[event_name, date]]
                else:
                    person_dict[person_id].append([event_name, date])
            
            values = []
            
            for person_id in person_dict.keys():
                events = person_dict[person_id]
                start_date = events[0][1]
                for event in events:
                    event_name = event[0]
                    cur_date = event[1]
                    if event_name == e_name:
                        values.append(round((cur_date - start_date).total_seconds()))
                        break
            
            out[device] = {"notFiltered":{"values":values, "mean":mean(values)}, 
                            "filtered":{"values":flt.filter3(values), "mean":mean(flt.filter3(values))}}

        cnxn.close()

        return jsonify(out)

@statistic.route('/api/statistics/time/<event>/locale/<timestamp>')
@cache.cached(timeout=100000, query_string=True)
def statisticsTimeLocaleView(event: str, timestamp: str):
    if request.method == 'GET':
        available_events = ["Convert", "Share"]
        event = event.lower()
        if event not in list(map(lambda c : c.lower(), available_events)):
            return "Invalid click", 400

        available_timestamps = ["lastday", "thisweek", "thismonth"]
        timestamp = timestamp.lower()
        if timestamp not in available_timestamps:
            return "Invalid timestamp", 400

        events = {
            "convert" : "conversion",
             "share" : "share_experience",
        }

        e_name = events[event]

        timestamps = {
            "lastday": 0,
            "thisweek": 6,
            "thismonth": 29,
        }
        
        day = timestamps[timestamp]

        cnxn = getConnection()
        cursor = cnxn.cursor()
        
        locales = cursor.execute(f"""SELECT 	TOP(?)
												locale
										FROM Persons
										GROUP BY locale
										ORDER BY COUNT(locale) DESC""", 6).fetchall()

        out = {} 

        for locale in locales:
            events = cursor.execute(f"""DECLARE @day AS INT
										SET @day = (SELECT DATEPART(dy, clicked_date)
													FROM Events
													WHERE clicked_date = (SELECT MAX(clicked_date)
													FROM Events))
										SELECT Events.person_id,
												Events.event_name,
												Events.clicked_date
										FROM Events
										INNER JOIN Persons
										ON Persons.person_id = Events.person_id
										WHERE DATEPART(dy, clicked_date) >= @day - ? AND locale=?
										ORDER BY clicked_date ASC""", day, locale[0]).fetchall()
            
            person_dict = {}

            for event in events:
                event_name = event[1]
                person_id = event[0]
                date = event[2]

                if person_id not in person_dict:
                    person_dict[person_id] = [[event_name, date]]
                else:
                    person_dict[person_id].append([event_name, date])
            
            values = []
            
            for person_id in person_dict.keys():
                events = person_dict[person_id]
                start_date = events[0][1]
                for event in events:
                    event_name = event[0]
                    cur_date = event[1]
                    if event_name == e_name:
                        values.append(round((cur_date - start_date).total_seconds()))
                        break
            
            if len(values) > 0:
                out[locale[0]] = {"notFiltered":{"values":values, "mean":mean(values)}, 
                                "filtered":{"values":flt.filter3(values), "mean":mean(flt.filter3(values))}}

        cnxn.close()

        return jsonify(out)