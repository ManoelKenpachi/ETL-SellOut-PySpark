from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("customer_name", StringType(), True),
    # StructField("sale_date", TimestampType(), True),
    StructField("sale_date", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("sale_amount", DoubleType(), True),
    StructField("store_location", StringType(), True)
])
