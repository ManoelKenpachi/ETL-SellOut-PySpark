import requests
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import sys
import os
from datetime import datetime

# Ajusta o caminho para importar a classe IncrementalQuery do diretório
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../features')))
from validator import IncrementalQuery  # Importa a classe IncrementalQuery do script existente

# Configurações da API
API_URL = "http://127.0.0.1:8000/sellout"

# Classe de modelo para "sale"
class Sale:
    def __init__(self, id, product_name, customer_name, sale_date, quantity, sale_amount, store_location):
        self.id = id
        self.product_name = product_name
        self.customer_name = customer_name
        self.sale_date = sale_date
        self.quantity = quantity
        self.sale_amount = sale_amount
        self.store_location = store_location

    @staticmethod
    def from_dict(data: dict):
        return Sale(
            id=data.get("id"),
            product_name=data.get("product_name"),
            customer_name=data.get("customer_name"),
            sale_date=datetime.strptime(data.get("sale_date"), "%Y-%m-%dT%H:%M:%S"),
            quantity=data.get("quantity"),
            sale_amount=float(data.get("sale_amount")),
            store_location=data.get("store_location")
        )

def fetch_data_from_api() -> DataFrame:
    """
    Consulta a API e retorna os dados como um DataFrame do Spark.
    """
    response = requests.get(API_URL)
    response.raise_for_status()
    data = response.json()
    
    # Cria uma sessão Spark e transforma os dados em DataFrame
    spark = SparkSession.builder.appName("ConsumeAPIExample").getOrCreate()
    df = spark.createDataFrame([Sale.from_dict(record).__dict__ for record in data])
    return df

def main():
    # Faz a consulta à API e obtém os dados como DataFrame
    source_df = fetch_data_from_api()
    print(source_df.show())  # Adiciona um print para verificar os dados
    print('manoeelelellelellelel')
    
    # Inicializa a classe IncrementalQuery para "sale"
    incremental_query = IncrementalQuery(query_type="sale", timestamp_column="sale_date")
    
    # Executa a carga incremental
    incremental_query.run_incremental_load(source_df)

if __name__ == "__main__":
    main()