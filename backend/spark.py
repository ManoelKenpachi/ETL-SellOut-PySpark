from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg

# Configurar a sessão do Spark
spark = SparkSession.builder \
    .appName("SelloutDataProcessing") \
    .config("spark.jars", "C:/Users/Ken-chan/Documents/Linqi/backend/postgresql-42.3.9.jar") \
    .getOrCreate()

# Configurações do banco de dados
db_properties = {
    "user": "postgres",
    "password": "1234",
    "driver": "org.postgresql.Driver"
}
db_url = "jdbc:postgresql://localhost:5432/sellout"

# Carregar os dados da tabela "sellout" do PostgreSQL
sellout_df = spark.read.jdbc(url=db_url, table="sellout", properties=db_properties)

# Visualizar as primeiras linhas do DataFrame
sellout_df.show()
