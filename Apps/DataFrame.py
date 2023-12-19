def dataframe():
    from Apps.SparkSession import Spark
    import logging
    from Apps.Logfile import log
    log()
    df=Spark().read.csv("Data\Financial Sample.csv",header=True,inferSchema=True)
    logging.info(f"Number of partitions are {df.rdd.getNumPartitions()}")
    logging.info(f"Data frame is {df}")
    return df