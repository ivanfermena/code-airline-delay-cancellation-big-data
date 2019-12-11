from __future__ import division
from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Row
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

dfD = sqlContext.sql("""
    SELECT MONTH(FL_DATE) as Month, COUNT(ARR_DELAY) as Delays
    FROM df
    WHERE ARR_DELAY > 0.0
    GROUP BY MONTH(FL_DATE)
    ORDER BY Month DESC
""")

dfC = sqlContext.sql("""
    SELECT Month(FL_DATE) as Month, COUNT(CANCELLED) as Cancelations
    FROM df
    WHERE CANCELLED = 1.0
    GROUP BY MONTH(FL_DATE)
    ORDER BY Month DESC
""")

partial1 = dfD.join(dfC,['Month'],"outer")

partial2 = partial1.rdd.map(lambda x: (x[0], x[1], x[2], x[1] + x[2]))
partial3 = partial2.map(lambda x: (x[0], x[3]))


partial3.toDF(["MONTH", "INCIDENTS"]).registerTempTable("df2")

min =  sqlContext.sql("""
    SELECT MONTH as Month, INCIDENTS as Incidents
    FROM df2
    WHERE INCIDENTS = (SELECT MIN(INCIDENTS) FROM df2)
""")

max = sqlContext.sql("""
    SELECT MONTH as Month, INCIDENTS as Incidents
    FROM df2
    WHERE INCIDENTS = (SELECT MAX(INCIDENTS) FROM df2)
""")

results = min.join(max,['Incidents','Month'],"outer")
results.show()
#results.toDF(["Month", "Total incidents"]).repartition(1).write.format('com.databricks.spark.csv').option("header", "true").save("worstAndBestMonthToFlight")



