from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import date
import string

conf = SparkConf().setMaster('local[4]').setAppName('airports5')
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

df = sqlContext.read.load("2009-2018.csv", 
                          format='com.databricks.spark.csv', 
                          header='true', 
                          inferSchema='true')

df.registerTempTable("df")

results = sqlContext.sql("""
    SELECT YEAR(FL_DATE) as Year, MONTH(FL_DATE) as Month, COUNT(FL_DATE) as Cancelations
    FROM df
    WHERE CANCELLED = 1.0
    GROUP BY YEAR(FL_DATE), MONTH(FL_DATE)
    ORDER BY Month DESC
""")

results.show()
#results.repartition(1).write.format('com.databricks.spark.csv').option("header", "true").save("delayPerMonth")
