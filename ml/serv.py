"""
.. module:: ft_server.server
"""

import json

import os
from flask import Flask
from flask import g
from flask import jsonify
from flask import request
from waitress import serve
import pickle
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.pipeline import Pipeline, FeatureUnion
from sklearn.metrics import accuracy_score
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
import functools


app = Flask(__name__)

@app.route("/")
def index():
    res = {'cache': cache_predict.cache_info()}
    return jsonify(res)


@app.route("/pred", methods=["POST"])
def predictions():
    content = request.get_json(silent=True)
    sp = content["data"].split(" ")
    l = [(i, v) for i, v in enumerate(sp) if len(v) >= 6]
    f = [{'idx': i, 'word': v} for i, v in l if cache_predict(v)]
    return jsonify(f)

@functools.lru_cache(maxsize=1048576)
def cache_predict(word) -> bool:
    return g.model.predict(pd.Series([word])).tolist()[0] == 1


@app.before_request
def before_request():
    g.model = pickle.load( open( "model.logreg", "rb" ) )


if __name__ == '__main__':
    serve(app, host='0.0.0.0', port=9999)
