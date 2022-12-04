from flask import Blueprint, request, jsonify
from cache import cache
from connection import getConnection
import azure.filters as flt
from statistics import mean

metrics = Blueprint('metrics', __name__)

@metrics.route('/api/metric/userFlow')
@cache.cached(timeout=100000, query_string=True)
def metricUserFlow():
    pass

@metrics.route("/api/metrics/<click_type>/device", methods=["GET"])
@cache.cached(timeout=1000, query_string=True)
def getTotalClicksDevice(click_type: str):
    available_click_type = ["totalClicks", "totalConversions", "totalShares"]
    if click_type not in available_click_type:
        return "Invalid click type", 400
    
    event = None
    if click_type == 'totalConversions':
        event = 'conversion'
    elif click_type == 'totalShares':
        event = "share_experience"
    else:
        event = "total"

    timeframe = request.args.get("timeframe").lower(
    ) if request.args.get("timeframe") is not None else "day"
    timeframe = timeframe if timeframe in ["day", "week", "month"] else "day"
    days = 1 if timeframe == "day" else 7 if timeframe == "week" else 30
    grouping = "format(clicked_date, 'hh tt')" if timeframe == "day"\
        else "format(clicked_date, 'yyyy-MM-dd')" if timeframe == "week"\
        else "format(clicked_date, 'yyyy-MM-dd')"

    cnxn = getConnection()
    cursor = cnxn.cursor()
    
    devices = ["Mobile", "Desktop"]
    out = {}
    for device in devices:
        if event == "total":
            res = cursor.execute(f"""
                                declare @latest datetime = (select max(clicked_date) from Events)
                                select count(*),
                                    {grouping}
                                from Events
                                inner join Persons
                                on Persons.person_id = Events.person_id
                                where clicked_date > dateadd(day, {-days}, @latest) and Persons.device = '{device}'
                                group by {grouping}
                                order by parse({grouping} as datetime) asc
                                """).fetchall()
        else:
            res = cursor.execute(f"""
                                declare @latest datetime = (select max(clicked_date) from Events)
                                select count(*),
                                    {grouping}
                                from Events
                                inner join Persons
                                on Persons.person_id = Events.person_id
                                where clicked_date > dateadd(day, {-days}, @latest) and Persons.device = '{device}' and Events.event_name = '{event}'
                                group by {grouping}
                                order by parse({grouping} as datetime) asc
                                """).fetchall()
        out[device] = list(map(lambda x: {"period": x[1], "value": x[0]}, res))
    return jsonify(out)

@metrics.route("/api/metrics/<click_type>/locale", methods=["GET"])
@cache.cached(timeout=1000, query_string=True)
def getTotalClicksLocale(click_type: str):
    available_click_type = ["totalClicks", "totalConversions", "totalShares"]
    if click_type not in available_click_type:
        return "Invalid click type", 400
    
    event = None
    if click_type == 'totalConversions':
        event = 'conversion'
    elif click_type == 'totalShares':
        event = "share_experience"
    else:
        event = "total"

    timeframe = request.args.get("timeframe").lower(
    ) if request.args.get("timeframe") is not None else "day"
    timeframe = timeframe if timeframe in ["day", "week", "month"] else "day"
    days = 1 if timeframe == "day" else 7 if timeframe == "week" else 30
    grouping = "format(clicked_date, 'hh tt')" if timeframe == "day"\
        else "format(clicked_date, 'yyyy-MM-dd')" if timeframe == "week"\
        else "format(clicked_date, 'yyyy-MM-dd')"

    cnxn = getConnection()
    cursor = cnxn.cursor()
    
    locales = cursor.execute(f"""SELECT 	TOP(?)
												locale
										FROM Persons
										GROUP BY locale
										ORDER BY COUNT(locale) DESC""", 6).fetchall()
    out = {}
    for locale in locales:
        locale = locale[0]
        if event == "total":
            res = cursor.execute(f"""
                                declare @latest datetime = (select max(clicked_date) from Events)
                                select count(*),
                                    {grouping}
                                from Events
                                inner join Persons
                                on Persons.person_id = Events.person_id
                                where clicked_date > dateadd(day, {-days}, @latest) and Persons.locale = '{locale}'
                                group by {grouping}
                                order by parse({grouping} as datetime) asc
                                """).fetchall()
        else:
            res = cursor.execute(f"""
                                declare @latest datetime = (select max(clicked_date) from Events)
                                select count(*),
                                    {grouping}
                                from Events
                                inner join Persons
                                on Persons.person_id = Events.person_id
                                where clicked_date > dateadd(day, {-days}, @latest) and Persons.locale = '{locale}' and Events.event_name = '{event}'
                                group by {grouping}
                                order by parse({grouping} as datetime) asc
                                """).fetchall()
        out[locale] = list(map(lambda x: {"period": x[1], "value": x[0]}, res))
    return jsonify(out)

@metrics.route("/api/metrics/average/<metric>", methods=["GET"])
@cache.cached(timeout=1000, query_string=True)
def getAverageMetric(metric: str):
    valid_metrics = ["clicksToConvert",
                     "clicksToShare", "timeToConvert", "timeToShare"]
    if metric.lower() not in map(str.lower, valid_metrics):
        return "Invalid metric", 400

    timeframe = request.args.get("timeframe").lower(
    ) if request.args.get("timeframe") is not None else "day"
    timeframe = timeframe if timeframe in ["day", "week", "month"] else "day"
    days = 1 if timeframe == "day" else 7 if timeframe == "week" else 30
    grouping = "format(clicked_date, 'hh tt')" if timeframe == "day"\
        else "format(clicked_date, 'yyyy-MM-dd')" if timeframe == "week"\
        else "format(clicked_date, 'yyyy-MM-dd')"

    cnxn = getConnection()
    cursor = cnxn.cursor()

    res = cursor.execute(f"""
                        declare @latest datetime = (select max(clicked_date) from Events)
                        select avg(cast({metric} as float)),
                            {grouping}
                        from persons_metric
                        left join Events
                        on persons_metric.person_id = Events.person_id
                        where {metric} is not null
                        and clicked_date > dateadd(day, {-days}, @latest)
                        group by {grouping}
                        order by parse({grouping} as datetime) asc
                        """).fetchall()

    return jsonify(list(map(lambda x: {"period": x[1], "value": x[0]}, res)))

