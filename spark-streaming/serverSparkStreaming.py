from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as fn
from pyspark.sql.functions import col
import socket
from io import StringIO

import pandas as pd

names_col = ["FL_DATE","OP_CARRIER","OP_CARRIER_FL_NUM","ORIGIN","DEST","CRS_DEP_TIME","DEP_TIME","DEP_DELAY", \
"TAXI_OUT","WHEELS_OFF","WHEELS_ON","TAXI_IN","CRS_ARR_TIME","ARR_TIME","ARR_DELAY","CANCELLED","CANCELLATION_CODE", \
"DIVERTED","CRS_ELAPSED_TIME","ACTUAL_ELAPSED_TIME","AIR_TIME","DISTANCE","CARRIER_DELAY","WEATHER_DELAY","NAS_DELAY", \
"SECURITY_DELAY","LATE_AIRCRAFT_DELAY", "Unnamed"]

def proccessSpark(sc, ssc, spark, line, df_top, today_date):

    df_line = pd.read_csv(StringIO(line), names=names_col)

    # Obtener el top 10 de delays por dia

    if today_date != df_line["FL_DATE"].item():
        today_date = df_line["FL_DATE"].item()
        df_top = pd.DataFrame(columns=names_col)

    df_top = df_top.append(df_line)

    df_airports = spark.createDataFrame(df_top[['ORIGIN', 'DEST', 'DEP_DELAY']])

    # Cogemos las columnas que nos interesan y las casteamos a Integer
    df_airports = df_airports.withColumn('DEP_DELAY', col("DEP_DELAY").cast(IntegerType()))

    # Agrupamos por Origen y destino
    df_gb_airports = df_airports.groupby(['ORIGIN', 'DEST'])

    # Hacemos la suma por del retraso a partir del grupo
    df_grouped = df_gb_airports.agg(fn.sum(col('DEP_DELAY')).alias('DEP_DELAY_SUM'))

    df_top_ten = df_grouped.sort(fn.desc("DEP_DELAY_SUM"))

    print(df_top_ten.show(10))

    # Actualizar ese top 10 en tiempo real

    return df_top, today_date

def Main():
    host = "localhost"
    port = 9999

    today_date = "1995-01-01"
    data_top = pd.DataFrame(columns=names_col)
    
    mySocket = socket.socket()
    mySocket.bind((host,port))

    # Create a local StreamingContext with two working thread and batch interval of 1 second
    sc = SparkContext("local[2]", "NetworkWordCount")
    ssc = StreamingContext(sc, 3)

    spark = SparkSession.builder.appName("Aircraft-Delay-Streaming").getOrCreate()
     
    mySocket.listen(1)
    conn, addr = mySocket.accept()
    print ("Connection from: " + str(addr))

    while True:
        line = conn.recv(1024).decode()

        if not line:
            break
            
        data_top, today_date = proccessSpark(sc, ssc, spark, line, data_top, today_date)

        conn.send(data_top.to_string().encode())
             
    conn.close()
     
if __name__ == '__main__':
    Main()




