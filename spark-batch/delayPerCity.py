
from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import date
import string

conf = SparkConf().setMaster('local[4]').setAppName('airports1')
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

df = sqlContext.read.load("2009-2018.csv", 
                          format='com.databricks.spark.csv', 
                          header='true', 
                          inferSchema='true')

df.registerTempTable("df")

results = sqlContext.sql("""
    SELECT COUNT(ORIGIN) as Delays, ORIGIN as Origin
    FROM df
    WHERE ARR_DELAY > 0.0
    GROUP BY ORIGIN
    ORDER BY Origin ASC
""")

results.show()
#results.repartition(1).write.format('com.databricks.spark.csv').option("header", "true").save("delayPerCity.csv")
