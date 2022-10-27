import json
from keras.models import load_model

from threading import Thread

import numpy as np

import pandas
# from django.core import se]rializers
from django.core.paginator import *
# from django.conf import settings
# from django.core.serializers.json import DjangoJSONEncoder
# from django.core.serializers import serialize
# from django.http import HttpResponse, JsonResponse
# from rest_framework import *
from flask import *
from flask_cors import *

from storeDetailsToDb import *
from symptoms import *

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'


@app.route('/expert-system/get-symptoms', methods=['GET'])
@cross_origin()
def get_symptoms():
    column = request.args.get('symptom')
    dataYes = get_symptoms_alf('true', column)
    dataNo = get_symptoms_alf('false', column)
    resp = jsonify(
        {'payload': {"symptom": column, "yes": int(dataYes[column].values), "no": int(dataNo[column].values)}})

    return resp


@app.route('/expert-system/get-symptoms-ratio', methods=['GET'])
@cross_origin()
def get_symptoms_ratio():
    isAlf = request.args.get('isAlf', type=bool)
    data = symptoms_ratio()
    result = data.to_json(orient='records')
    resp = jsonify({'payload': json.loads(result)})

    return resp


@app.route('/expert-system/get-symptoms-corr', methods=['GET'])
@cross_origin()
def get_symptoms_corr():
    data = symptoms_corr()
    result = data.to_json(orient='values')
    resp = jsonify({'payload': json.loads(result)})

    return resp


@app.route('/expert-system/predict', methods=['POST'])
@cross_origin()
def predict():
    request_data = request.get_json()
    inp = np.asarray(list(request_data.values())).astype('float32')
    inp = inp.reshape(1, -1)

    model = load_model("alf_dnn.h5")
    result = (model.predict(inp) > 0.5).astype(int)

    message = ""
    condition = "Liver Failure"

    if int(result[0] == int(0)):
        message = "NO"
    else:
        message = "YES"

    resp = jsonify({'payload': {"condition": condition, "result": message}})

    return resp


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
