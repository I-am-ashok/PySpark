def savedf(fileformat):
    from Apps.Transform import tx
    from Apps.DateCalculations import datecalc
    import logging
    tx().repartition(1).write.format(fileformat).mode("overwrite").save(f"H:\\GitHub\\PySpark\\OutPut\\{fileformat}")
    logging.info(f"The df loaded as parquet file, check the file at H:\\GitHub\\PySpark\\OutPut\\superstore.{fileformat} !!!!")
    datecalc().write.partitionBy("year","Month").format(fileformat).mode("overwrite").save(f"H:\\GitHub\\PySpark\\OutPut\\{fileformat}")
    logging.info(f"The tdf2 loaded as parquet file, check the file at H:\\GitHub\\PySpark\\OutPut\\superstore.{fileformat} !!!!")