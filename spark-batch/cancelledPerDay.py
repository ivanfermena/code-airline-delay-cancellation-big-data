from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import date
import string

conf = SparkConf().setMaster('local[4]').setAppName('airports2')
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

df = sqlContext.read.load("2009-2018.csv", 
                          format='com.databricks.spark.csv', 
                          header='true', 
                          inferSchema='true')

df.registerTempTable("df")

results = sqlContext.sql("""
    SELECT COUNT(FL_DATE) as cancelations, FL_DATE as date 
    FROM df
    WHERE CANCELLED = 1.0
    GROUP BY FL_DATE
    ORDER BY date ASC
""")
results.show()
#results.repartition(1).write.format('com.databricks.spark.csv').option("header", "true").save("cancelledPerDay")
