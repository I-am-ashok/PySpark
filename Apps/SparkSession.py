from pyspark.sql import SparkSession

def Spark():
    spark=SparkSession.builder.config("spark.hadoop.home.dir",r"H:\Hadoop").appName("SparkSession").master("local").getOrCreate()
    spark.sparkContext.setLogLevel(logLevel="ERROR" ) 
    #ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    return spark
