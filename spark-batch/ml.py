from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import mean_squared_error, r2_score

import matplotlib.pyplot as plt

import numpy as np 
import os # accessing directory structure
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import plotly.express as px

names_col = ["ORIGIN","DEST","CANCELLED"]

nRowsRead = 10000 # specify 'None' if want to read whole file

df = pd.read_csv('data/2018-example.csv', delimiter=',', nrows = nRowsRead)
df.dataframeName = '2018-example.csv'

# CLEANED AND CATEGORIC COLUMNS

# -------- ESTUDIO DE VER SI LAS CANCELACIONES ESTAN ASOCIADAS A ORIGEN-DESTINO---------

cat_df_flights = df.select_dtypes(include=['object']).copy()

cat_df_flights = cat_df_flights[["ORIGIN","DEST"]]

labels = cat_df_flights['ORIGIN'].astype('category').cat.categories.tolist()

dicLabels = dict()

num = 0
for val in labels:
    dicLabels.setdefault(val, num)
    num = num + 1

df_cleaned = cat_df_flights.copy()

df_cleaned.replace( dicLabels, inplace=True)

df_cleaned['CANCELLED'] = df['CANCELLED']

# REGRESION PLOT ALL STUDY

fig = px.scatter_matrix(df)
fig.show()

# REGRESION PLOT SPECIFIC

fig = px.scatter_matrix(df[names_col])
fig.show()

# MODEL

modelo = LogisticRegression()

X = df_cleaned.loc[:,["ORIGIN","DEST"]]
y = df_cleaned["CANCELLED"]

print(X.shape)
print(y.shape)

# Entreno el modelo con los datos (X,Y)
modelo.fit(X, y)

# Ahora puedo obtener el coeficiente b_1
print u'Coeficiente beta1: ', modelo.coef_[0]
 
# Podemos predecir usando el modelo
y_pred = modelo.predict(X)
 
print u'Error cuadratico medio: %.2f' % mean_squared_error(y, y_pred)
print u'Estadistico R_2: %.2f' % r2_score(y, y_pred)


# -------- ESTUDIO DE VARIABLES SOBRE DEP_TIME---------

print("----------------------------")

modelo = LogisticRegression()

df_cleaned = df[["CRS_DEP_TIME","WHEELS_ON","WHEELS_OFF"]].dropna()

X = df_cleaned.loc[:,["WHEELS_ON","WHEELS_OFF"]]
y = df_cleaned["CRS_DEP_TIME"]

print(X.shape)
print(y.shape)

# Entreno el modelo con los datos (X,Y)
modelo.fit(X, y)

# Ahora puedo obtener el coeficiente b_1
print u'Coeficiente beta1: ', modelo.coef_[0]
 
# Podemos predecir usando el modelo
y_pred = modelo.predict(X)
 
print u'Error cuadratico medio: %.2f' % mean_squared_error(y, y_pred)
print u'Estadistico R_2: %.2f' % r2_score(y, y_pred)