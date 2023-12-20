from Apps.Transform import tx
from pyspark.sql import functions as f
def datecalc():
    tdf2=tx().select("Segment","Country"," Product ","Units Sold"," Sale Price ","Date").\
        withColumnRenamed(" Product ","Product").\
        withColumnRenamed(" Sale Price ","SalePrice").withColumn("Formatted_Date",f.date_format(f.to_date("Date","dd-MM-yy"),'dd-MMM-yyyy')).\
        withColumn("Month",f.month(f.to_date("Formatted_Date",'dd-MMM-yyyy'))).\
        withColumn("year",f.year(f.to_date("Formatted_Date",'dd-MMM-yyyy')))
    return tdf2