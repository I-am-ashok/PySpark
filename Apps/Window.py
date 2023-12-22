from Apps.Transform import *
from pyspark.sql import functions as f
from pyspark.sql import Window
def windowfun(df):
    df=df.select("Country","Product","Discount Band","Units Sold","Sale Price","Sales","Profit","Date")
    df=df.select("Product","Country",(f.round(f.col("Units Sold")*f.col("Sale Price"),2)/1000000).alias("SaleAmount"),"Profit").groupBy("Product").agg(f.round(f.sum("SaleAmount"),2).alias("SaleAmount"),f.round(f.sum("Profit"),2).alias("Profit"))
    wdf=df.select("Product","SaleAmount","Profit",
                 f.dense_rank().over(Window.orderBy(df.SaleAmount.desc())).alias("DenseRank"),
                 f.rank().over(Window.orderBy(df.SaleAmount.desc())).alias("Rank"),
                 f.percent_rank().over(Window.orderBy(df.SaleAmount.desc())).alias("%Rank"),
                 f.row_number().over(Window.orderBy(df.SaleAmount.desc())).alias("RowNum"),
                 f.lag("SaleAmount").over(Window.orderBy(df.SaleAmount.desc())).alias("Lag"),
                 f.lead("SaleAmount").over(Window.orderBy(df.SaleAmount.desc())).alias("Lead"),
                 f.first("SaleAmount").over(Window.orderBy(df.SaleAmount.desc())).alias("first"),
                 f.nth_value("SaleAmount",3).over(Window.orderBy(df.SaleAmount.desc())).alias("NthValue"),
                 f.ntile(2).over(Window.orderBy(df.SaleAmount.desc())).alias("Ntile"),
                 f.round(f.cume_dist().over(Window.orderBy(df.SaleAmount.desc())),2).alias("cume_dist")
                 )
    return wdf