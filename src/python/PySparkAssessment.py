from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast
from pyspark.sql.types import *



MIN_SUM_THRESHOLD = 10000000
F_FACTOR_MULTI = 2

def has_columns(data_frame, column_name_list):
    for column_name in column_name_list:
        if not data_frame.columns.contains(column_name):
            raise Exception('Column is missing: ' + column_name)


def column_count(data_frame):
    return data_frame.columns.size


def _get_large_df_type() -> StructType:
    return StructType([
        StructField("A", StringType(), False),
        StructField("B", StringType(), False),
        StructField("C", DecimalType(5, 2), False),
        StructField("D", DecimalType(5, 2), False),
        StructField("E", DecimalType(5, 2), False),
        StructField("F", DecimalType(5, 2), True),
        StructField("G", DecimalType(5, 2), True),
        StructField("H", DecimalType(5, 2), True),
        StructField("I", DecimalType(5, 2), True),
        StructField("J", DecimalType(5, 2), True)
    ])


def _get_geolocation_df_type() -> StructType:
    return StructType([
        StructField("A", StringType(), False),
        StructField("K", StringType(), False),
        StructField("L", StringType(), False),
        StructField("M", IntegerType(), False)
    ])


def _get_prod_df_type() -> StructType:
    return StructType([
        StructField("B", StringType(), False),
        StructField("N", StringType(), False),
        StructField("O", IntegerType(), False),
        StructField("P", IntegerType(), False)
    ])


def _checks_of_large_df(very_large_dataframe: DataFrame):
    # validate column count
    if column_count(very_large_dataframe) != 10:
        raise Exception('Incorrect column count: ' + column_count(very_large_dataframe))

    # validate that dataframe has all required columns
    has_columns(very_large_dataframe, ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'])

    # Remove duplicates in column [A]
    # 50% of records in column [A] could potentially be duplicates
    very_large_dataframe = very_large_dataframe.dropDuplicates(['A'])

    # Get count of column F and ensure it is above MIN_SUM_THRESHOLD
    total_sum_of_column_F = very_large_dataframe.agg(sum('F')).collect()[0][0]

    if total_sum_of_column_F * F_FACTOR_MULTI < MIN_SUM_THRESHOLD:
        raise Exception('total_sum_of_column_A: ' + total_sum_of_column_F + ' is below threshold: ' + MIN_SUM_THRESHOLD)


def process(spark: SparkSession):

    # very_large_dataframe
    # 250 GB of CSV files from client which must have only 10 columns [A, B, C, D, E, F, G, H, I, J]
    # [A, B] contains string data
    # [C, D, E, F, G, H, I, J] contains decimals with precision 5, scale 2 (i.e. 125.75)
    # [A, B, C, D, E] should not be null
    # [F, G, H, I, J] may be null

    very_large_dataset_location = '/Sourced/location_1'
    very_large_dataframe = spark.read\
        .schema(_get_large_df_type())\
        .option("mode", "DROPMALFORMED")\
        .csv(very_large_dataset_location, header=True, sep="\t")

    # Perform checks for very_large_dataframe
    _checks_of_large_df(very_large_dataframe)

    # Column F: Need to apply conversion factor of 2.5 i.e. Value 2, conversion factor 2.5 = 5
    very_large_dataframe = very_large_dataframe\
        .withColumn("F", very_large_dataframe['F'] * F_FACTOR_MULTI)

    # small_geography_dimension_dataframe
    # 25 MB of parquet, 4 columns [A, K, L, M]
    # Columns [A, K, L] contain only string data
    # Column [M] is an integer
    # Columns [A, K, L, M] contain all non nullable data. Assume this is the case
    small_geography_dimension_dataset = '/location_2'
    small_geography_dimension_dataframe = spark.read\
        .schema(_get_geolocation_df_type())\
        .parquet(small_geography_dimension_dataset)

    # Join very_large_dataframe to small_geography_dimension_dataframe on column [A]
    # Include only column [M] from small_geography_dimension_dataframe on new very_large_dataframe
    # No data (row count) loss should occur from very_large_dataframe

    # Get the minimum number of columns from small_geography_dimension_dataframe
    small_geography_dimension_dataframe = small_geography_dimension_dataframe.select("A", "M")

    very_large_dataframe = very_large_dataframe.join(broadcast(small_geography_dimension_dataframe),
                                                     ["A"],
                                                     "left_outer")

    # small_product_dimension_dataframe
    # 50 MB of parquet, 4 columns [B, N, O, P]
    # Columns [B, N] contain only string data
    # Columns [O, P] contain only integers
    # Columns [B, N, O, P] contain all non nullable data. Assume this is the case
    small_product_dimension_dataset = './location_3'  # 50 MB of parquet
    small_product_dimension_dataframe = spark.read \
        .schema(_get_prod_df_type()) \
        .parquet(small_product_dimension_dataset)

    # Join very_large_dataframe to small_product_dimension_dataframe on column [B]
    # Only join records to small_product_dimension_dataframe where O is greater then 10
    # Keep only Column [P]
    small_product_dimension_dataframe = small_product_dimension_dataframe.where('O > 10').select('B', 'P')
    very_large_dataframe = very_large_dataframe.join(broadcast(small_product_dimension_dataframe),
                                                     ['B'],
                                                     'inner')

    # Write very_large_dataframe to next stage.
    # Columns selected should be: [C, D, E, F, G, H, I, J, M, P]
    very_large_dataframe = very_large_dataframe.select(['C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'M', 'P'])

    (very_large_dataframe.coalesce(1)
    .write
    .mode('overwrite')
    .format('com.databricks.spark.csv')
    .option('header', 'true')
    .option('sep', '\t').csv('./location_3'))

    # The next stage will involve presenting the data in Azure SQL Synpase for reporting by our team

    # Assume external table has been created in Azure SQL Synpase which point to location_3
    # This data is then loaded (Create Table AS - CTAS) into [assessment].[FactVeryLarge] for performance reasons

    # TODO
    # Summarise benefits of the Create Table AS (CTAS) operation prior to giving access to reporting team.
    # SUMMARY: The benefits of creating the table first is that you will have a stable format and place where the
    # characteristics of it are displayed.
    # Also someone can query it and it will not fail

    # The reporting team will join this newly created Fact (very_large_dataframe) back to Geography and Product Dimensions
    # for further analysis




if __name__ == '__main__':
    # Create spark session
    spark = SparkSession.builder.getOrCreate()

    try:
        process(spark)
    finally:
        spark.stop()

