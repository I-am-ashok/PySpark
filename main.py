from Apps.DataFrame import *
from Apps.Transform import *
from Apps.Logfile import *
from Apps.Load import *
from Apps.SparkSession import *
from Apps.DateCalculations import *
from Apps.FindDataSkewNess import *
from Apps.Window import *

fileforms=["parquet","orc","json"]
if __name__=="__main__":
   # dataframe()
   # tx().show()
    #tx().printSchema()
    #for filetype in fileforms:pip
    #    savedf(filetype)
    #datecalc().show()
    #print(dataskewness())
    #print("="*100)
    windowfun(df=tx()).show()    
    #datecalc().show()

   # Spark().stop()