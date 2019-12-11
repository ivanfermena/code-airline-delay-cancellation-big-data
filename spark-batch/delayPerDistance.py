from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import date
import string

conf = SparkConf().setMaster('local[4]').setAppName('airports4')
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

df = sqlContext.read.load("2009-2018.csv", 
                          format='com.databricks.spark.csv', 
                          header='true', 
                          inferSchema='true')

df.registerTempTable("df")

results = sqlContext.sql("""
    SELECT AVG(ARR_DELAY) as avg_delay, DISTANCE as distance 
    FROM df
    GROUP BY DISTANCE
    ORDER BY distance DESC
""")

results.show()
#results.repartition(1).write.format('com.databricks.spark.csv').option("header", "true").save("delayDistance")

