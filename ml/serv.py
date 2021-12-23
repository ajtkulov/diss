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
from model import model
from model import pred

app = Flask(__name__)

@app.route("/")
def index():
    res = {}
    return jsonify(res)


@app.route("/pred", methods=["POST"])
def predictions():
    content = request.get_json(silent=True)
    sp = content["data"].split(" ")
    res = g.model.predict(pd.Series(sp)).tolist()
    return jsonify(res)


@app.before_request
def before_request():
    g.model = pickle.load( open( "model.logreg", "rb" ) )


if __name__ == '__main__':
    serve(app, host='0.0.0.0', port=9999)
