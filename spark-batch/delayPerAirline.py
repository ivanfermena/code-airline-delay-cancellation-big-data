from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import date
import string

conf = SparkConf().setMaster('local[4]').setAppName('airports3')
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

df = sqlContext.read.load("2009-2018.csv", 
                          format='com.databricks.spark.csv', 
                          header='true', 
                          inferSchema='true')

df.registerTempTable("df")

results = sqlContext.sql("""
    SELECT COUNT(OP_CARRIER) as Delays, OP_CARRIER as Airline
    FROM df
    WHERE ARR_DELAY > 0.0
    GROUP BY OP_CARRIER
    ORDER BY Airline ASC
""")
results.show()
#results.repartition(1).write.format('com.databricks.spark.csv').option("header", "true").save("delayPerAirline.csv")
