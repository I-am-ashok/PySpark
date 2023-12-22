def tx():
    from Apps.DataFrame import dataframe
    from Apps.Logfile import log
    from pyspark.sql import functions as f
    log("Trasform")
    t_df= dataframe().select(*[(f.trim(f.regexp_replace(c,'\\$',' ')).alias(c)) for c in dataframe().columns])
    t_df=t_df.withColumnRenamed(" Product ","Product").\
    withColumnRenamed(" Discount Band ","Discount Band").\
    withColumnRenamed(" Manufacturing Price ","Manufacturing Price").\
    withColumnRenamed(" Sale Price ","Sale Price").\
    withColumnRenamed(" Gross Sales ","Gross Sales").\
    withColumnRenamed(" Discounts ","Discounts").\
    withColumnRenamed("  Sales ","Sales").\
    withColumnRenamed(" COGS ","COGS").\
    withColumnRenamed(" Profit ","Profit").\
    withColumnRenamed(" Month Name ","Month Name")

    t_df=t_df.select("segment","Country","Product","Discount Band",
                f.col("Units Sold").cast("int"),
                f.col("Manufacturing Price").cast("float"),
                f.col("Sale Price").cast("float"),
                f.col("Gross Sales").cast("float"),
                f.col("Discounts").cast("float"),
                f.col("Sales").cast("float"),
                f.col("COGS").cast("float"),
                f.col("Profit").cast("float"),
                "Date",
                f.col("Month Number").cast("int"),"Month Name",
                f.col("Year").cast('int')
                )
    return t_df