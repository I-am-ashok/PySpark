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
                f.when(f.col("Manufacturing Price").rlike("^[-+]?[0-9]*\\.?[0-9]+$"),f.col("Manufacturing Price").cast("float")).otherwise(f.col("Manufacturing Price")).alias("Manufacturing Price"),
                f.col("Sale Price").cast("float"),
                f.when(f.col("Gross Sales").rlike("^[-+]?[0-9]*\\.?[0-9]+$"),f.col("Gross Sales").cast("float")).otherwise(f.col("Gross Sales")).alias("Gross Sales"),
                f.when(f.col("Discounts").rlike("^[-+]?[0-9]*\\.?[0-9]+$"),f.col("Discounts").cast("float")).otherwise(f.col("Discounts")).alias("Discounts"),
                f.when(f.col("Sales").rlike("^[-+]?[0-9]*\\.?[0-9]+$"),f.col("Sales").cast("float")).otherwise(f.col("Sales")).alias("Sales"),
                f.when(f.col("COGS").rlike("^[-+]?[0-9]*\\.?[0-9]+$"),f.col("COGS").cast("float")).otherwise(f.col("COGS")).alias("COGS"),
                f.when(f.col("Profit").rlike("^[-+]?[0-9]*\\.?[0-9]+$"),f.col("Profit").cast("float")).otherwise(f.col("Profit")).alias("Profit"),
                "Date",
                f.col("Month Number").cast("int"),"Month Name",
                f.col("Year").cast('int')
                )
    t_df=t_df.select("segment","Country","Product","Discount Band","Units Sold",
                     f.when(f.col("Manufacturing Price").isNotNull(),f.col("Manufacturing Price")).otherwise(f.lit(0)).alias("Manufacturing Price"),
                     f.when(f.col("Sale Price").isNotNull(),f.col("Sale Price")).otherwise(f.lit(0)).alias("Sale Price"),
                     f.when(f.col("Gross Sales").isNotNull(),f.col("Gross Sales")).otherwise(f.lit(0)).alias("Gross Sales"),
                     f.when(f.col("Discounts").isNotNull(),f.col("Discounts")).otherwise(f.lit(0)).alias("Discounts"),
                     f.when(f.col("Sales").isNotNull(),f.col("Sales")).otherwise(f.lit(0)).alias("Sales"),
                     f.when(f.col("COGS").isNotNull(),f.col("COGS")).otherwise(f.lit(0)).alias("COGS"),
                     f.when(f.col("Profit").isNotNull(),f.col("Profit")).otherwise(f.lit(0)).alias("Profit"),
                     "Date","Month Number","Month Name","Year"
                     )
    return t_df