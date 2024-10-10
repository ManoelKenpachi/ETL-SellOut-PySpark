import psycopg2
from psycopg2 import sql
from faker import Faker
import random

# Configurações do banco de dados
db_name = "sellout"
db_user = "postgres"
db_password = "1234"
db_host = "localhost"
db_port = "5432"

# Conectar ao banco de dados PostgreSQL
conn = psycopg2.connect(
    dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port,
    options='-c client_encoding=UTF8'
)
cursor = conn.cursor()

# Criar um gerador de dados falsos
fake = Faker()

tabela = 'sellout'
tabela = 'sellout_recebe'

# Definir a estrutura da tabela sellout
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {tabela} (
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    customer_name VARCHAR(100),
    sale_date TIMESTAMP,
    quantity INT,
    sale_amount DECIMAL(10, 2),
    store_location VARCHAR(100)
);
"""

cursor.execute(create_table_query)
conn.commit()
print(f"Tabela '{tabela}' criada com sucesso.")

# Função para gerar dados falsos e inserir na tabela
def insert_fake_data(num_rows):
    for _ in range(num_rows):
        product_name = fake.word().capitalize()
        customer_name = fake.name()
        sale_date = fake.date_time_this_year()
        quantity = random.randint(1, 20)
        sale_amount = round(random.uniform(10.0, 500.0), 2)
        store_location = fake.city()

        insert_query = sql.SQL(
            f"""
            INSERT INTO {tabela} (product_name, customer_name, sale_date, quantity, sale_amount, store_location)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
        )
        cursor.execute(insert_query, (product_name, customer_name, sale_date, quantity, sale_amount, store_location))

    conn.commit()
    print(f"{num_rows} linhas inseridas com sucesso na tabela '{tabela}'.")

# Gerar 10 mil linhas de dados falsos
insert_fake_data(1)

# Fechar a conexão
cursor.close()
conn.close()
print("Conexão com o banco de dados fechada.")
