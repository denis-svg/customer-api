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

@metrics.route("/api/metrics/totalClicks", methods=["GET"])
@cache.cached(timeout=1000, query_string=True)
def getTotalClicks():
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
                        select count(*),
                            {grouping}
                        from Events
                        where clicked_date > dateadd(day, {-days}, @latest)
                        group by {grouping}
                        order by parse({grouping} as datetime) asc
                        """).fetchall()

    return jsonify(list(map(lambda x: {"period": x[1], "value": x[0]}, res)))

@metrics.route("/api/metrics/conversionClicks", methods=["GET"])
@cache.cached(timeout=1000, query_string=True)
def getTotalConversion():
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
                        select count(*),
                            {grouping}
                        from Events
                        where clicked_date > dateadd(day, {-days}, @latest) and event_name = 'conversion'
                        group by {grouping}
                        order by parse({grouping} as datetime) asc
                        """).fetchall()

    return jsonify(list(map(lambda x: {"period": x[1], "value": x[0]}, res)))

@metrics.route("/api/metrics/shareClicks", methods=["GET"])
@cache.cached(timeout=1000, query_string=True)
def getTotalShares():
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
                        select count(*),
                            {grouping}
                        from Events
                        where clicked_date > dateadd(day, {-days}, @latest) and event_name = 'share_experience'
                        group by {grouping}
                        order by parse({grouping} as datetime) asc
                        """).fetchall()

    return jsonify(list(map(lambda x: {"period": x[1], "value": x[0]}, res)))

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

