from pyspark import SparkContext
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as fn
from pyspark.sql.functions import col

#
# TODO: Estudio de retrasos de los vuelos por origen y destino
#
# Uso pandas para mayor facilidad a la hora de tratar la extraccion y tratamiento
# de los datos

import pandas as pd

# --------- DATA EXTRACTION ---------

# Get SparkSession with SQL in Dataframe and SparkContext
sc = SparkContext()

spark = SparkSession.builder.appName("Aircraft-Delay-Cancelation").getOrCreate()

# Concatena todos los csvs uno detras de otro
# df = pd.read_csv("../data/*.csv")
df = pd.read_csv("2018-example.csv")

print(df.info())

num_rows = df.count()

print("Numero de lineas extraidas en el dataset:")
print(num_rows)

print("Numero de NA y null que tienen cada columna:")
print(df.isna().sum())

print("Ejemplos de estos valores que no nos interesan:")
print(df[df['DEP_DELAY'].isna()])

missing_values = ["n/a", "na", "", " "]
df_cleaned = pd.read_csv("2018-example.csv", na_values = missing_values)

df_cleaned = df[df['DEP_DELAY'].notna()]

print("Revisamos el numero de NA y null que tienen cada columna:")
print(df_cleaned.isna().sum())

df_airports = spark.createDataFrame(df_cleaned[['ORIGIN', 'DEST', 'DEP_DELAY']])

# Comprobamos los tipos de las columnas para no tener error de typos en el sum
print(df_airports.dtypes)

# --------- DATA TRANSFORMATION ---------

# Cogemos las columnas que nos interesan y las casteamos a Integer
df_airports = df_airports.withColumn('DEP_DELAY', col("DEP_DELAY").cast(IntegerType()))

# Agrupamos por Origen y destino
df_gb_airports = df_airports.groupby(['ORIGIN', 'DEST'])

# Hacemos la suma por del retraso a partir del grupo
df_grouped = df_gb_airports.agg(fn.sum(col('DEP_DELAY')).alias('DEP_DELAY_SUM'))

df_grouped_desc = df_grouped.sort(fn.desc("DEP_DELAY_SUM"))
df_grouped_asce = df_grouped.sort(fn.asc("DEP_DELAY_SUM"))

# Comparativa por ambos lados, tanto los que tienen menos como los que mas.
print(df_grouped_asce.show())
print(df_grouped_desc.show())

# Estudiamos ahora el motivo de esos tiempos, por secciones agruparemos segun zona

# CRS_ELAPSED_TIME = Tiempo planeado del vuelo
# ACTUAL_ELAPSED_TIME = AIR_TIME + TAXI_IN + TAXI_OUT
df_airports_with_fly_time = spark.createDataFrame(df_cleaned[['ORIGIN', 'DEST', 'CRS_ELAPSED_TIME', 'ACTUAL_ELAPSED_TIME', 'TAXI_IN', 'TAXI_OUT']])

df_airports_with_fly_time = df_airports_with_fly_time.withColumn('TIME_FLY', 
    df_airports_with_fly_time['ACTUAL_ELAPSED_TIME'] - (df_airports_with_fly_time['TAXI_IN'] + df_airports_with_fly_time['TAXI_OUT']))

df_airports_with_fly_time = df_airports_with_fly_time.withColumn('DIF_TIME_FLY',
    df_airports_with_fly_time['CRS_ELAPSED_TIME'] - df_airports_with_fly_time['TIME_FLY'])

df_group_with_fly_time = df_airports_with_fly_time.groupby(['ORIGIN', 'DEST'])

df_fly_time_agruped = df_group_with_fly_time.agg(fn.sum(col('CRS_ELAPSED_TIME')).alias('ELAPSED_TIME'), 
                                                fn.sum(col('TIME_FLY')).alias('TIME_FLY'),
                                                fn.sum(col('DIF_TIME_FLY')).alias('DIF_TIME_FLY'))

df_fly_time_agruped_desc = df_fly_time_agruped.sort(fn.asc("DIF_TIME_FLY"))

print(df_fly_time_agruped_desc.show())

## --------- DATA LOAD ---------

# VALORACION

# El tiempo de vuelo no suele ser un problema por los que se tiene mucho delay en los vuelos, incluso todo lo contrario.
# Suele estimarse demasiado y llega antes normalmente de los esperado

#_grouped_pandas.to_csv('../data/load.csv')
