from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Definição do modelo de dados para Produto
product_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("supplier_id", IntegerType(), True),
    StructField("description", StringType(), True)
])
