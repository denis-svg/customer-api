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