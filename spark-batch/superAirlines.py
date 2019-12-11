
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
    SELECT OP_CARRIER as Airlines, COUNT(OP_CARRIER) as veces
    FROM df
    WHERE DEP_DELAY > 0.0 AND ARR_DELAY < 0.0
    GROUP BY OP_CARRIER
    ORDER BY veces DESC
""")

results.show()
#results.repartition(1).write.format('com.databricks.spark.csv').option("header", "true").save("superA")
