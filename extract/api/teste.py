import requests
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import to_timestamp
import sys
import os
import logging


# Configurando o logger para escrever em arquivo e também no console
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler("erp_log.log"),
        logging.StreamHandler()  # Isso exibirá os logs no console também
    ]
)

logging.info("Iniciando script erp.py")

# Ajusta o caminho para importar a classe IncrementalQuery do diretório
try:
    logging.info("Ajustando o caminho para importar a classe IncrementalQuery...")
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../features')))
    from validator import IncrementalQuery  # Importa a classe IncrementalQuery do script existente
    logging.info("Classe IncrementalQuery importada com sucesso.")

    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../transform')))
    from sale import schema  
    logging.info("Schema importado com sucesso.")

except ImportError as e:
    logging.error(f"Erro ao importar: {e}")
    exit(1)

# Configurações da API
API_URL = "http://127.0.0.1:8000/sellout"

def fetch_data_from_api() -> DataFrame:
    """
    Consulta a API e retorna os dados como um DataFrame do Spark.
    """
    logging.info(f"Fazendo requisição à API: {API_URL}")
    try:
        response = requests.get(API_URL)
        response.raise_for_status()  # Levanta uma exceção para status de erro HTTP
        data = response.json()
        logging.info("Dados recebidos da API com sucesso.")
    except requests.exceptions.HTTPError as e:
        logging.error(f"Erro HTTP ao acessar a API: {e} - Código de status {response.status_code}")
        exit(1)
    except requests.exceptions.ConnectionError as e:
        logging.error(f"Erro de conexão ao acessar a API: {e}")
        exit(1)
    except requests.exceptions.Timeout as e:
        logging.error(f"Tempo limite de requisição excedido: {e}")
        exit(1)
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao acessar a API: {e}")
        exit(1)

    # Cria uma sessão Spark e transforma os dados em DataFrame
    logging.info("Criando sessão Spark...")
    spark = SparkSession.builder \
        .appName("DataframeGen") \
        .config("spark.pyspark.python", "C:\\Program Files\\Python312\\python3.exe") \
        .getOrCreate()

    # Definindo o nível de log do Spark como OFF
    spark.sparkContext.setLogLevel("OFF")
    logging.info("Sessão Spark criada com sucesso.")
    
    logging.info("Transformando dados em DataFrame do Spark...")

    try:
        # Cria o DataFrame com o esquema importado
        df = spark.createDataFrame(data, schema=schema)
        # Converte a coluna 'sale_date' para TimestampType no DataFrame
        df = df.withColumn("sale_date", to_timestamp("sale_date", "yyyy-MM-dd'T'HH:mm:ss"))

        logging.info("DataFrame criado e coluna 'sale_date' convertida com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao criar o DataFrame: {e}")
        exit(1)

    return df

def main():
    logging.info("Iniciando script principal...")
    # Faz a consulta à API e obtém os dados como DataFrame
    try:
        source_df = fetch_data_from_api()
        logging.info("Exibindo os dados do DataFrame:")
        logging.info(source_df.show())
    except Exception as e:
        logging.error(f"Erro ao buscar dados da API: {e}")
        exit(1)
    
    # Inicializa a classe IncrementalQuery para "sale"
    logging.info("Inicializando IncrementalQuery...")
    try:
        incremental_query = IncrementalQuery(query_type="sale", timestamp_column="sale_date")
        logging.info("IncrementalQuery inicializada com sucesso.")
        logging.info(incremental_query)
    except Exception as e:
        logging.error(f"Erro ao inicializar IncrementalQuery: {e}")
        exit(1)
    
    # Executa a carga incremental
    logging.info("Executando carga incremental...")
    try:
        incremental_query.run_incremental_load(source_df)
        logging.info("Carga incremental concluída com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao executar a carga incremental: {e}")
        exit(1)

if __name__ == "__main__":
    logging.info("Executando o script erp.py...")
    main()
