def dataframe():
    from Apps.SparkSession import Spark
    from Apps.Logfile import log
    import logging
    log('Dataframe')
    df=Spark().read.csv("Data\Financial Sample.csv",header=True,inferSchema=True)
    logging.info(f"Number of partitions are {df.rdd.getNumPartitions()}")
    df=df.repartition(10)
    logging.info(f"Number of partitions are {df.rdd.getNumPartitions()}")
    logging.info(f"Data frame is {df}")
    return df