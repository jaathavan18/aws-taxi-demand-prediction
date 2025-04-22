import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lit, to_timestamp, unix_timestamp
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window

def fill_missing_rides_full_range(sdf, hour_col, location_col, rides_col, spark):
    """
    Fills in missing rides for all hours in the range and all unique locations.

    Parameters:
    - sdf: PySpark DataFrame with columns [hour_col, location_col, rides_col]
    - hour_col: Name of the column containing hourly timestamps
    - location_col: Name of the column containing location IDs
    - rides_col: Name of the column containing ride counts
    - spark: Existing SparkSession

    Returns:
    - PySpark DataFrame with missing hours and locations filled in with 0 rides
    """
    # Convert hour column to timestamp type
    sdf = sdf.withColumn(hour_col, F.to_timestamp(hour_col))

    # Get the min and max timestamps for the full hour range
    min_hour = sdf.agg(F.min(hour_col)).collect()[0][0]
    max_hour = sdf.agg(F.max(hour_col)).collect()[0][0]

    # Create a sequence of numbers for the hour range
    num_hours = int((max_hour - min_hour).total_seconds() // 3600) + 1
    hour_range = spark.range(num_hours)

    # Convert to timestamps by adding hours to min_hour
    hours_df = hour_range.withColumn(
        hour_col,
        F.from_unixtime(
            F.unix_timestamp(F.lit(min_hour)) + (F.col("id") * 3600)
        )
    ).select(hour_col)

    # Create DataFrame with all unique locations
    locations_df = sdf.select(location_col).distinct()

    # Create the full combination of hours and locations
    full_combinations = hours_df.crossJoin(locations_df)

    # Left join with original data
    filled_df = full_combinations.join(sdf, on=[hour_col, location_col], how='left')

    # Fill missing ride counts with 0
    filled_df = filled_df.withColumn(rides_col, F.coalesce(filled_df[rides_col], F.lit(0)).cast(T.IntegerType()))

    return filled_df


def transform_raw_data_into_ts_data(rides_sdf, spark):
    """
    Transform raw ride data into time series format using PySpark.

    Args:
        rides_sdf: PySpark DataFrame with pickup_datetime and location columns.
        spark: Existing SparkSession

    Returns:
        PySpark DataFrame: Time series data with filled gaps.
    """

    # Floor the datetime to the nearest hour
    rides_sdf = rides_sdf.withColumn('pickup_hour', F.date_trunc('hour', 'pickup_datetime'))

    # Aggregate data by pickup_hour and pickup_location_id
    agg_rides = (
        rides_sdf
        .groupBy('pickup_hour', 'pickup_location_id')
        .agg(F.count('*').alias('rides'))
    )

    # Fill missing data using the previously defined function
    agg_rides_all_slots = fill_missing_rides_full_range(
        agg_rides,
        'pickup_hour',
        'pickup_location_id',
        'rides',
        spark
    ).orderBy('pickup_location_id', 'pickup_hour')

    # Convert data types to reduce memory usage
    agg_rides_all_slots = (
        agg_rides_all_slots
        .withColumn('pickup_location_id', F.col('pickup_location_id').cast(T.ShortType()))
        .withColumn('rides', F.col('rides').cast(T.ShortType()))
    )

    return agg_rides_all_slots


# Get job arguments
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "year", "month", "bucket_name", "s3_key_prefix"]
)

# Initialize Glue context and Spark session
spark = SparkSession.builder.appName("iadFilterTransformInClass").getOrCreate()
glueContext = GlueContext(spark)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read arguments
year = int(args["year"])
month = int(args["month"])
bucket_name = args["bucket_name"]
s3_key_prefix = args["s3_key_prefix"]

# Define S3 paths
input_s3_key = f"s3://{bucket_name}/{s3_key_prefix}/year={year}/month={month:02}/yellow_tripdata_{year}-{month:02}.parquet"
filtered_s3_key = f"s3://{bucket_name}/{s3_key_prefix.replace('raw', 'glue-filtered')}/year={year}/month={month:02}/"
transformed_s3_key = f"s3://{bucket_name}/{s3_key_prefix.replace('raw', 'glue-transformed')}/year={year}/month={month:02}/"

# Read the Parquet file into a Spark DataFrame
rides = spark.read.parquet(input_s3_key)

# Total records before any filtering
total_records = rides.count()

# Define the start and end date for filtering
start_date = f"{year}-{month:02}-01"
end_date = f"{year + (month // 12)}-{(month % 12) + 1:02}-01"

# Filter and clean the data
rides_clean = rides.filter(
    (col("tpep_pickup_datetime").isNotNull())
    & (col("tpep_dropoff_datetime").isNotNull())
    & (col("total_amount").isNotNull())
    & (col("PULocationID").isNotNull())
    & (col("DOLocationID").isNotNull())
    & (col("trip_distance").isNotNull())
    & (col("passenger_count").isNotNull())
)

# Records dropped due to missing values
dropped_missing = total_records - rides_clean.count()
count_after_clean = rides_clean.count()

rides_clean = rides_clean.filter(
    (col("tpep_pickup_datetime") >= start_date)
    & (col("tpep_pickup_datetime") < end_date)
)

# Records dropped by date range filter
count_after_date_filter = rides_clean.count()
dropped_date_range = count_after_clean - count_after_date_filter

# Convert duration to seconds for compatibility with approxQuantile
rides_clean = rides_clean.withColumn(
    "duration",
    (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")),
)

# Calculate quantiles for filtering
quantiles = rides_clean.approxQuantile(
    ["duration", "total_amount", "trip_distance"], [0.999], 0.01
)
max_duration, max_total_amount, max_distance = (
    quantiles[0][0],
    quantiles[1][0],
    quantiles[2][0],
)

# Apply additional filters
count_before_duration = rides_clean.count()
rides_filtered = rides_clean.filter(
    (col("duration") > lit(0)) & (col("duration") <= lit(max_duration))
)
dropped_duration = count_before_duration - rides_filtered.count()

count_before_amount = rides_filtered.count()
rides_filtered = rides_filtered.filter(
    (col("total_amount") >= lit(2.5)) & (col("total_amount") <= lit(max_total_amount))
)
dropped_total_amount = count_before_amount - rides_filtered.count()

count_before_distance = rides_filtered.count()
rides_filtered = rides_filtered.filter(
    (col("trip_distance") > lit(0)) & (col("trip_distance") <= lit(max_distance))
)
dropped_distance = count_before_distance - rides_filtered.count()

count_before_location = rides_filtered.count()
rides_filtered = rides_filtered.filter(~col("PULocationID").isin([1, 264, 265]))
dropped_location = count_before_location - rides_filtered.count()

count_before_passenger = rides_filtered.count()
rides_filtered = rides_filtered.filter(
    (col("passenger_count") >= lit(1)) & (col("passenger_count") <= lit(5))
)
dropped_passenger_count = count_before_passenger - rides_filtered.count()

# Final record count
valid_records = rides_filtered.count()
records_dropped = total_records - valid_records
percent_dropped = (records_dropped / total_records) * 100

# Combine all logging information into one output string
stats_msg = (
    f"Total records: {total_records:,}\n"
    f"Valid records: {valid_records:,}\n"
    f"Records dropped: {records_dropped:,} ({percent_dropped:.2f}%)\n"
    f"Records dropped due to missing values: {dropped_missing:,}\n"
    f"Records dropped by date range filter: {dropped_date_range:,}\n"
    f"Records dropped by duration filter: {dropped_duration:,}\n"
    f"Records dropped by total amount filter: {dropped_total_amount:,}\n"
    f"Records dropped by distance filter: {dropped_distance:,}\n"
    f"Records dropped by NYC location filter: {dropped_location:,}\n"
    f"Records dropped by passenger count filter: {dropped_passenger_count:,}"
)
print(stats_msg)

# Rename columns and cast to the required data types
rides_final = (
    rides_filtered.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
    .withColumnRenamed("PULocationID", "pickup_location_id")
    .withColumnRenamed("DOLocationID", "dropoff_location_id")
    .select(
        col("pickup_datetime").cast("timestamp"),
        col("dropoff_datetime").cast("timestamp"),
        col("pickup_location_id").cast("int"),
        col("dropoff_location_id").cast("int"),
        col("trip_distance").cast("double"),
        col("fare_amount").cast("double"),
        col("tip_amount").cast("double"),
        col("payment_type").cast("int"),
        col("passenger_count").cast("int"),
    )
)

# Write the filtered data back to S3
rides_final.write.mode("overwrite").parquet(filtered_s3_key)

print(f"Filtered data written to {filtered_s3_key}")

# Transform raw data into time series format
rides_transformed = transform_raw_data_into_ts_data(rides_final, spark)

# Write the transformed data to the "transformed" folder in S3
rides_transformed.write.mode("overwrite").parquet(transformed_s3_key)
print(f"Transformed data written to {transformed_s3_key}")

# Commit the job
job.commit()