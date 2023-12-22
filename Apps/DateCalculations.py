from Apps.Transform import tx
from pyspark.sql import functions as f
def datecalc():
    df=tx().withColumn("Formatted_Date",f.date_format(f.to_date("Date","dd-MM-yy"),"dd-MMM-yyyy"))
    tdf2=df.select("Segment","Country","Product","Units Sold","Sale Price","Formatted_Date").\
        withColumn("Current_Date",f.current_date()).\
        withColumn("Month",f.month(f.to_date("Formatted_Date",'dd-MMM-yyyy'))).\
        withColumn("year",f.year(f.to_date("Formatted_Date",'dd-MMM-yyyy'))).\
        withColumn("Quarter",f.quarter(f.to_date("Formatted_Date",'dd-MMM-yyyy'))).\
        withColumn("addmonths",f.add_months(f.to_date("Formatted_Date",'dd-MMM-yyyy'),1)).\
        withColumn("NextDay",f.date_add(f.to_date("Formatted_Date",'dd-MMM-yyyy'),1)).\
        withColumn("previousday",f.date_sub(f.to_date("Formatted_Date",'dd-MMM-yyyy'),1)).\
        withColumn("yearstart",f.date_trunc('year',f.to_date("Formatted_Date",'dd-MMM-yyyy'))).\
        withColumn("Monthstart",f.date_trunc('months',f.to_date("Formatted_Date",'dd-MMM-yyyy'))).\
        withColumn("Daystart",f.date_trunc('day',f.to_date("Formatted_Date",'dd-MMM-yyyy'))).\
        withColumn("datediff",f.datediff(f.current_date(),f.to_date("Formatted_Date",'dd-MMM-yyyy'))).\
        withColumn("Monthsbetween",f.months_between(f.current_date(),f.to_date("Formatted_Date",'dd-MMM-yyyy'))).\
        withColumn("lastdayofmonth",f.last_day(f.to_date("Formatted_Date",'dd-MMM-yyyy'))).\
        withColumn("nextdaydate",f.next_day(f.to_date("Formatted_Date",'dd-MMM-yyyy'),'Fri'))
    return tdf2

        #withColumnRenamed(" Product ","Product").\
        #withColumnRenamed(" Sale Price ","SalePrice").withColumn("Formatted_Date",f.date_format(f.to_date("Date","dd-MM-yy"),'dd-MMM-yyyy')).\
