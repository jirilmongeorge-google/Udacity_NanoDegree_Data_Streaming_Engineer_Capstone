import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
import pathlib


# Schema for incoming resources
schema = StructType([
    StructField("crime_id", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", StringType(), True),
    StructField("call_date", StringType(), True),
    StructField("offense_date", StringType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", StringType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True)
])

def run_spark_job(spark):

    # Spark Configuration
    df = spark \
        .readStream.format('kafka') \
        .option("kafka.bootstrap.servers",'localhost:9099')\
        .option("subscribe",'police.calls') \
        .option("maxOffsetsPerTrigger",200) \
        .option("startingOffsets", "latest") \
        .load()
        

    # Show schema for the incoming resources for checks
    df.printSchema()
 
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value as STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # Select distinct original_crime_type_name and disposition
    distinct_table = service_table.select("original_crime_type_name", "disposition").distinct()
    distinct_table.printSchema()

    # count the number of original crime type
    agg_df = distinct_table.groupby(psf.col("original_crime_type_name")).count()
    
    # Write output stream
    query = agg_df.writeStream \
        .format('console') \
        .outputMode('complete') \
        .option('truncate', "false") \
        .trigger(processingTime='30 seconds') \
        .start()

    # Attach a ProgressReporter
    query.awaitTermination()

    # Get the right radio code json path
    radio_code_json_filepath = pathlib.Path(__file__).parent.joinpath("radio_code.json")
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code
    # Rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # Join on disposition column
    join_query = distinct_table.join(radio_code_df, agg_df.disposition == radio_code_df.disposition, 'inner') \
                .writeStream \
                .format('console') \
                .outputMode('complete') \
                .trigger(once=True) \
                .option('truncate', "false") \
                .start()

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark Session started")

    run_spark_job(spark)

    spark.stop()
