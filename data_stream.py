import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
import pyspark.sql.functions as psf


# Create a schema for incoming resources
schema = StructType([
    StructField("crime_id", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", TimestampType(), True),
    StructField("call_date", TimestampType(), True),
    StructField("offense_date", TimestampType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", TimestampType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True)
])

radio_schema = StructType([
        StructField("disposition_code", StringType(), True),
        StructField("description", StringType(), True)
    ])


def run_spark_job(spark):
    #Set WARN after stdout warning
    spark.sparkContext.setLogLevel("WARN")

    BOOTSTRAP_SERVERS = "DESKTOP-B2QMGU6:9092"
    # Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("subscribe", "police.calls.service") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetPerTrigger", 200) \
        .load()

    print(SparkConf().getAll())

    # Show schema for the incoming resources for checks
    df.printSchema()

    # extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df.select(
        psf.from_json(
                  kafka_df.value,
                  schema).alias("main_df")
            ).select("main_df.*")

    # select original_crime_type_name and disposition
    distinct_table = service_table \
        .select(
            "original_crime_type_name",
            "disposition",
            "call_date_time"
            ).withWatermark("call_date_time", "10 minutes")

    # count the number of original crime type
    agg_df = distinct_table.groupBy(
            "original_crime_type_name"
        ).count().sort("count", ascending=False)

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    logger.info('Stream of crime count by type')
    query = agg_df.writeStream \
        .format("console") \
        .outputMode("complete") \
        .start()

    # TODO attach a ProgressReporter
    query.awaitTermination()

    # get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    # Needs option multiline,True or output will be an empty df
    radio_code_df = spark.read.option(
            "multiline",
            "true"
        ).json(
            radio_code_json_filepath,
            schema=radio_schema
        )

    # rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # join on disposition column
    join_query = distinct_table.join(
            radio_code_df, "disposition"
        ).writeStream\
        .format("console")\
        .queryName("join")\
        .start()

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.ui.port", 4040) \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
