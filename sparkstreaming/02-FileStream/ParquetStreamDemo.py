from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("File Streaming Demo") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

    logger = Log4j(spark)

    raw_df = spark.readStream \
        .format("parquet") \
        .option("path", "input") \
        .option("maxFilesPerTrigger", 1) \
        .load()

    # raw_df.printSchema()

    flattened_df = raw_df.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID",
                                     "CustomerType", "PaymentMethod", "DeliveryType", "City",
                                     "State", "PinCode", "ItemCode", "ItemDescription", "ItemPrice", "ItemQty", "TotalValue")

    invoiceWriterQuery = flattened_df.writeStream \
        .format("json") \
        .queryName("Flattened Invoice Writer") \
        .outputMode("append") \
        .option("path", "output") \
        .option("checkpointLocation", "chk-point-dir") \
        .start()
    # .trigger(processingTime="1 minute") \

    logger.info("Flattened Invoice Writer started")
    invoiceWriterQuery.awaitTermination()
