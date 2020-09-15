from flask import Flask, render_template, url_for, request
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import pickle

# load the model from disk
loaded_model = pickle.load(open('aqi_rf_reg_model.pkl', 'rb'))
app = Flask(__name__)


@app.route('/')
def home():
    return render_template('home.html')


@app.route('/predict', methods=['POST'])
def predict():
    Ozone = float(request.form['Ozone conc'])
    PM_2p5 = float(request.form['PM-2.5 conc'])
    NO2 = float(request.form['NO2 conc'])
    CO = float(request.form['CO conc'])
    PM_10 = float(request.form['PM-10 conc'])
    input_values = [[Ozone, PM_2p5, NO2, CO, PM_10]]

    aqi_prediction = loaded_model.predict(input_values)
    aqi_prediction = round(aqi_prediction[0])
    # my_prediction = my_prediction.tolist()
    return render_template('result.html', prediction=aqi_prediction)


if __name__ == '__main__':
    app.run(debug=True)
