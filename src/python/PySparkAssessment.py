

MIN_SUM_THRESHOLD = 10, 000, 000


def has_columns(data_frame, column_name_list):
    for column_name in column_name_list:
        if not data_frame.columns.contains(column_name):
            raise Exception('Column is missing: ' + column_name)


def column_count(data_frame):
    return data_frame.columns.size


def process():
    # Create spark session
    spark = SparkSession.builder.getOrCreate()

    # very_large_dataframe
    # 250 GB of CSV files from client which must have only 10 columns [A, B, C, D, E, F, G, H, I, J]
    # [A, B] contains string data
    # [C, D, E, F, G, H, I, J] contains decimals with precision 5, scale 2 (i.e. 125.75)
    # [A, B, C, D, E] should not be null
    # [F, G, H, I, J] should may be null

    very_large_dataset_location = '/Sourced/location_1'
    very_large_dataframe = spark.read.csv(very_large_dataset_location, header=True, sep="\t")

    # validate column count
    if column_count(very_large_dataframe) != 10:
        raise Exception('Incorrect column count: ' + column_count(very_large_dataframe))

    # validate that dataframe has all required columns
    has_columns(very_large_dataframe, ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'])

    # TODO
    # Column F: Need to apply conversion factor of 2.5 i.e. Value 2, conversion factor 2.5 = 5

    # Remove duplicates in column [A]
    # 50% of records in column [A] could potentially be duplicates
    very_large_dataframe = very_large_dataframe.dropDuplicates(['A'])

    # Get count of column F and ensure it is above MIN_SUM_THRESHOLD
    total_sum_of_column_F = very_large_dataframe.agg(sum('F')).collect()[0][0]

    if total_sum_of_column_F < MIN_SUM_THRESHOLD:
        raise Exception('total_sum_of_column_A: ' + total_sum_of_column_F + ' is below threshold: ' + MIN_SUM_THRESHOLD)

    # small_geography_dimension_dataframe
    # 25 MB of parquet, 4 columns [A, K, L, M]
    # Columns [A, K, L] contain only string data
    # Column [M] is an integer
    # Columns [A, K, L, M] contain all non nullable data. Assume this is the case
    small_geography_dimension_dataset = '/location_2'
    small_geography_dimension_dataframe = spark.read.parquet(small_geography_dimension_dataset)

    # Join very_large_dataframe to small_geography_dimension_dataframe on column [A]
    # Include only column [M] from small_geography_dimension_dataframe on new very_large_dataframe
    # No data (row count) loss should occur from very_large_dataframe

    very_large_dataframe = very_large_dataframe.join(small_geography_dimension_dataframe,
                                                     (very_large_dataframe.A == small_geography_dimension_dataframe.A))

    # small_product_dimension_dataframe
    # 50 MB of parquet, 4 columns [B, N, O, P]
    # Columns [B, N] contain only string data
    # Columns [O, P] contain only integers
    # Columns [B, N, O, P] contain all non nullable data. Assume this is the case
    small_product_dimension_dataset = './location_3'  # 50 MB of parquet
    small_product_dimension_dataframe = spark.read.parquet(small_product_dimension_dataset)

    # TODO
    # Join very_large_dataframe to small_product_dimension_dataframe on column [B]
    # Only join records to small_product_dimension_dataframe where O is greater then 10
    # Keep only Column [P]

    # Write very_large_dataframe to next stage.
    # Columns selected should be: [C, D, E, F, G, H, I, J, M, P]

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
    # Sumarise benefits of the Create Table AS (CTAS) operation prior to giving access to reporting team.

    # The reporting team will join this newly created Fact (very_large_dataframe) back to Geography and Product Dimensions
    # for futher analysis


process()
