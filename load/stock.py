from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# Definição do modelo de dados para Estoque
stock_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("store_location", StringType(), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("last_restock_date", DateType(), True)
])
