from Apps.DataFrame import *
from Apps.Transform import *
from Apps.Logfile import *
from Apps.Load import *
from Apps.SparkSession import *
from Apps.DateCalculations import *

fileforms=["parquet","orc","json"]
if __name__=="__main__":
   # dataframe()
   # tx().show()
   # tx().printSchema()
    for filetype in fileforms:
        savedf(filetype)
    #datecalc().show()
   # Spark().stop()

