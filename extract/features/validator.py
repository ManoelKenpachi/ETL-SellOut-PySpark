from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, max as spark_max, to_timestamp, round
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
import logging
import time

# Configurando o logger para reduzir logs desnecessários do Spark
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger("py4j").setLevel(logging.WARN)

class IncrementalQuery:
    def __init__(self, query_type: str, timestamp_column: str):
        """
        Inicializa a classe IncrementalQuery.

        :param query_type: Tipo da consulta (ex: 'sale').
        :param timestamp_column: Nome da coluna que indica a data/hora de inserção ou atualização.
        """
        self.spark = SparkSession.builder \
            .appName("PostgresConnection") \
            .config("spark.jars", "C:\\Users\\Ken-chan\\Documents\\Linqi\\backend\\postgresql-42.3.9.jar") \
            .config("spark.pyspark.python", "C:\\Program Files\\Python312\\python3.exe") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .getOrCreate()

        self.query_type = query_type
        self.timestamp_column = timestamp_column
        self.last_max_timestamp = None

    def load_existing_data(self) -> DataFrame:
        """
        Carrega os dados existentes da tabela 'sellout_recebe' dos últimos 30 dias.

        :return: DataFrame com os dados existentes dos últimos 30 dias.
        """
        logging.info("Carregando dados existentes dos últimos 30 dias do PostgreSQL.")

        jdbc_url = "jdbc:postgresql://localhost:5432/sellout"
        connection_properties = {
            "user": "postgres",
            "password": "1234",
            "driver": "org.postgresql.Driver"
        }

        try:
            # Subconsulta para pegar dados dos últimos 30 dias
            query = f"(SELECT * FROM sellout_recebe WHERE {self.timestamp_column} >= current_date - interval '30 days') as sellout"
            df = self.spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
            # df = df.withColumn("sale_date", to_timestamp("sale_date", "yyyy-MM-dd'T'HH:mm:ss"))
            df.show()  # Mostra os dados carregados no DataFrame
            return df

        except Exception as e:
            logging.warning(f"Erro ao carregar os dados existentes: {e}. Retornando DataFrame vazio.")
            
            # Definindo o esquema manualmente para criar um DataFrame vazio
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("product_name", StringType(), True),
                StructField("customer_name", StringType(), True),
                StructField("sale_date", TimestampType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("sale_amount", DoubleType(), True),
                StructField("store_location", StringType(), True)
            ])
            return self.spark.createDataFrame([], schema)

    def get_new_data(self, source_df: DataFrame) -> DataFrame:
        """
        Consulta dados novos da fonte.

        :param source_df: DataFrame da fonte de dados.
        :return: DataFrame contendo apenas dados novos.
        """
        if self.last_max_timestamp is None:
            # Primeiro carregamento: pega o máximo do timestamp_column dos dados existentes.
            existing_data = self.load_existing_data()

            if existing_data is None:
                logging.info("Nenhum dado existente encontrado. Carregando todos os dados da fonte.")
                self.last_max_timestamp = None
            else:

                max_timestamp_row = existing_data.agg(spark_max(col(self.timestamp_column))).collect()
     
                if max_timestamp_row:
                    self.last_max_timestamp = max_timestamp_row[0][0]
                    logging.info(f"Timestamp máximo dos dados existentes: {self.last_max_timestamp}")
                else:
                    logging.info("Não foi possível determinar o timestamp máximo. Carregando todos os dados da fonte.")
                    self.last_max_timestamp = None
        
        # Filtra apenas os dados novos com base no timestamp_column
        if self.last_max_timestamp:
            new_data = source_df.filter(col(self.timestamp_column) > lit(self.last_max_timestamp))
            logging.info("Filtrando dados novos com base no timestamp.")
        else:
            new_data = source_df
            logging.info("Carregando todos os dados da fonte, pois é o primeiro carregamento.")
        
        return new_data

    def append_new_data(self, new_data: DataFrame):
        """
        Adiciona os novos dados ao banco de dados PostgreSQL.

        :param new_data: DataFrame contendo apenas os dados novos.
        """
        logging.info("Adicionando novos dados ao banco de dados PostgreSQL.")

        jdbc_url = "jdbc:postgresql://localhost:5432/sellout"
        connection_properties = {
            "user": "postgres",
            "password": "1234",
            "driver": "org.postgresql.Driver"
        }

        # Corrigir os tipos de dados do DataFrame
        # new_data = new_data.withColumn("sale_date", to_timestamp(col("sale_date"), "yyyy-MM-dd'T'HH:mm:ss"))
        new_data = new_data.withColumn("sale_amount", round(col("sale_amount"), 2))
        new_data = new_data.withColumn("sale_date", col("sale_date").cast("string"))
        new_data = new_data.withColumn("sale_date", col("sale_date").cast("string"))
        new_data = new_data.withColumn("sale_date", to_timestamp("sale_date", "yyyy-MM-dd'T'HH:mm:ss"))

        new_data.repartition(10).write.jdbc(url=jdbc_url, table="sellout_recebe", mode="append", properties=connection_properties)

    def run_incremental_load(self, source_df: DataFrame):
        """
        Executa a lógica de carga incremental para adicionar novos dados.

        :param source_df: DataFrame da fonte de dados.
        """
        try:
            new_data = self.get_new_data(source_df)
            if new_data:
                self.append_new_data(new_data)
                print(new_data)
                print('new_data')
                # Atualiza o last_max_timestamp para manter a consistência
                self.last_max_timestamp = new_data.agg(spark_max(col(self.timestamp_column))).collect()[0][0]
                logging.info(f"Carga incremental concluída. Novo timestamp máximo: {self.last_max_timestamp}")
            else:
                logging.info("Nenhum dado novo para carregar.")
        except Exception as e:
            logging.error(f"Erro ao executar a carga incremental: {e}")
            # Adiciona um tempo de espera para evitar falhas consecutivas
            time.sleep(5)

# Exemplo de uso
def main():
    # Configurando a SparkSession e desativando logs desnecessários do Spark
    spark = SparkSession.builder.appName("IncrementalQueryExample").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    # Definindo o esquema esperado para validação dos dados
    sale_schema = StructType([
        StructField("sale_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("sale_date", TimestampType(), False),
        StructField("quantity", DoubleType(), False)
    ])
    
    # Busca os dados da API (exemplo de chamada)
    # Aqui você pode implementar a lógica para buscar novos dados, por exemplo, de uma API
    # source_df = fetch_data_from_api("https://api.exemplo.com/data")
    # if source_df is None:
    #     return

    # Criando um exemplo de DataFrame vazio para simulação
    source_df = spark.createDataFrame([], sale_schema)
    
    # Inicializa a classe IncrementalQuery com o tipo de consulta e a coluna de timestamp
    incremental_query = IncrementalQuery(query_type="sale", timestamp_column="sale_date")
    
    # Executa a carga incremental
    incremental_query.run_incremental_load(source_df)

if __name__ == "__main__":
    main()