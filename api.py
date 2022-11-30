from flask import Flask
from flask_cors import CORS
from statistic import statistic
from cache import cache


app = Flask(__name__)
app.config["CACHE_TYPE"] = 'simple'
cache.init_app(app)
app.register_blueprint(statistic)

CORS(app)

app.run()