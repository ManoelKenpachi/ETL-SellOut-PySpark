# Linqi
Simulando consumo de arquivos Sellout no Desenvolvimento ETL


Para iniciar o projeto precisamos criar um banco de dados para consumo:

db/generate psql.py

psql.py Gera um banco de dados postgres com dados de teste.

#API
Subimos uma api na porta http://127.0.0.1:8000/sellout para consumir os dados através do Spark
backend/api_erp.py

Execução:
python -m uvicorn api_erp:app --reload

Consumo:
O processo de consumo se tem origem na pasta extract
Em extract encontramos os métodos de consumo, e dentro deles um script para extrai as informações para o banco

por exemplo:
extract/api/teste.py

Execução:
spark-submit --jars C:\Users\Ken-chan\Documents\Linqi\backend\postgresql-42.3.9.jar teste.py

java: openjdk version "11.0.24"
python: Python 3.12.3 
hadoop: Hadoop 3.3.6
spark: 3.4.3
