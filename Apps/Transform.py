def tx():
    from Apps.DataFrame import dataframe
    from Apps.Logfile import log
    from pyspark.sql import functions as f
    log("Trasform")
    t_df= dataframe().select(*[(f.trim(f.regexp_replace(c,'\\$',' ')).alias(c)) for c in dataframe().columns])
    return t_df