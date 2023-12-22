from Apps.Transform import *
from Apps.Logfile import log
from pyspark.sql.types import *
from pyspark.sql import functions as f

import logging
log("FindDataSkewNess")
def dataskewness():
    thresholdvalue=0.50
    df=tx()
    skewnescolumns={}
    for col_name in df.columns:
        logging.info(f"the column name is {col_name} and the data type is {df.schema[col_name].dataType}")
        try:
            #print(df.select(col_name).schema[0].dataType)
            if df.select(col_name).schema[0].dataType in [IntegerType(),FloatType()]:
                skewnessvalue= df.withColumn("skewness",f.col(col_name).cast('float')).\
                    select(f.abs(f.skewness(col_name)).alias("skewness")).\
                    select(f.round(f.signum(f.col('skewness'))*f.col('skewness'),2)).collect()[0][0]
                #skewnessvalue=float(skewnessvalue.asDict()[col_name])
                #print(skewnessvalue)
                #print(thresholdvalue)
                #print(type(skewnessvalue))
                #print(type(thresholdvalue))
                logging.info(f"Skewness value of {col_name} is {skewnessvalue} threshold value is {thresholdvalue}")
                skew_label = "skewed" if skewnessvalue >= 0.5 else "Non-Skewed"
                #print(skew_label)
                #print("thereshold condition pass")
                skewnescolumns[col_name]=(skew_label,skewnessvalue)
        except TypeError as e:
            logging.info(e)
        else:
            logging.info("condition not satisfied !!")
        finally:
            logging.info(f"{col_name} skewness checking is completed. ")
    print("="*100)
    print("The columns skewness is as follows  ")
    return skewnescolumns
    
