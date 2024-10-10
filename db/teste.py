from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType
from pyspark.sql.functions import col
import logging

# Configuração do SparkSession
spark = SparkSession.builder \
            .appName("PostgresConnection") \
            .config("spark.jars", "C:\\Users\\Ken-chan\\Documents\\Linqi\\backend\\postgresql-42.3.9.jar") \
            .config("spark.pyspark.python", "C:/Program Files/Python312/python.exe") \
            .getOrCreate()

# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("Iniciando script de atualização do banco de dados PostgreSQL")

# URL de conexão ao banco de dados PostgreSQL
jdbc_url = "jdbc:postgresql://localhost:5432/sellout"

# Propriedades de conexão
connection_properties = {
    "user": "postgres",
    "password": "1234",
    "driver": "org.postgresql.Driver"
}

# Definindo o esquema dos dados
df_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("sale_date", StringType(), True),  # Sale date inicialmente como String para evitar problemas
    StructField("quantity", IntegerType(), True),
    StructField("sale_amount", DoubleType(), True),
    StructField("store_location", StringType(), True)
])

# Carregar os dados da tabela PostgreSQL em um DataFrame do Spark

# Preencher o DataFrame com uma linha de dados falsos
data = [(9999, 'Produto Teste', 'Cliente Teste', '2024-10-10T12:00:00', 10, 100.0, 'Local Teste')]
df_fake = spark.createDataFrame(data, schema=df_schema)
df_fake.show()

try:
    df = spark.read.jdbc(url=jdbc_url, table="sellout_recebe", properties=connection_properties)
    df.show()  # Mostra os dados carregados no DataFrame
except Exception as e:
    logging.error(f"Erro ao conectar ao banco de dados: {e}")
    exit(1)

# Atualizar os dados no banco PostgreSQL
try:
    # Exemplo de atualização: vamos arredondar o valor de sale_amount e transformar sale_date em string novamente
    new_data = df.withColumn("sale_amount", col("sale_amount").cast(DoubleType()))
    new_data = new_data.withColumn("sale_date", col("sale_date").cast(StringType()))

    # Escrever de volta no banco de dados
    new_data.write.jdbc(url=jdbc_url, table="sellout_recebe", mode="overwrite", properties=connection_properties)
    logging.info("Tabela 'sellout_recebe' atualizada com sucesso.")
except Exception as e:
    logging.error(f"Erro ao atualizar a tabela no PostgreSQL: {e}")
    exit(1)

# Encerrar a sessão Spark
spark.stop()