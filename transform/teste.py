from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Iniciando uma SparkSession
spark = SparkSession.builder.appName("ProdutosVendasEstoque").getOrCreate()

# Definindo o Schema para Product
product_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), True),
    StructField("price", FloatType(), False)
])

# Definindo o Schema para Stock
stock_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False)
])

# Definindo o Schema para Sales
sales_schema = StructType([
    StructField("sale_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity_sold", IntegerType(), False),
    StructField("sale_date", StringType(), True)
])

# Criando DataFrames com dados fictícios
product_data = [
    ("P001", "Notebook", "Eletrônicos", 3500.00),
    ("P002", "Teclado Mecânico", "Acessórios", 250.00),
    ("P003", "Smartphone", "Eletrônicos", 2000.00),
    ("P004", "Camiseta", "Vestuário", 50.00)
]

stock_data = [
    ("P001", 10),
    ("P002", 50),
    ("P003", 30),
    ("P004", 100)
]

sales_data = [
    ("S001", "P001", 2, "2024-10-01"),
    ("S002", "P002", 5, "2024-10-02"),
    ("S003", "P003", 1, "2024-10-03"),
    ("S004", "P004", 3, "2024-10-04")
]

# Criando DataFrames
product_df = spark.createDataFrame(product_data, product_schema)
stock_df = spark.createDataFrame(stock_data, stock_schema)
sales_df = spark.createDataFrame(sales_data, sales_schema)

# Mostrando os DataFrames criados
print("Products DataFrame:")
product_df.show()

print("Stock DataFrame:")
stock_df.show()

print("Sales DataFrame:")
sales_df.show()
