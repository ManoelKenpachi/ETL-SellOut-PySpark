from pymongo import MongoClient

# Conectar ao MongoDB (ajuste 'localhost' e a porta conforme necessário)
client = MongoClient('localhost', 27017)

# Criar ou acessar o banco de dados 'sellout'
db = client['sellout']

# Criar ou acessar a coleção 'clientes'
clientes = db['clientes']

# Exemplo de documento para inserção
cliente_data = {
    "cliente_id": "cliente3",
    "nome": "Cliente C",
    "contato": "Manoel",
    "email": "Manoel@clienteb.com",
    "telefone": "1234-5678",
    "lojas": [
        {
            "loja_id": "loja3",
            "nome_loja": "Farmacia do Manoel",
            "endereco": "Avenida Manoel, 1010",
            "cidade": "São Paulo",
            "estado": "SP",
            "ERPS": ["ABC", "DFE"],
            "metodos": [
                {
                    "tipo_metodo": "ftp",
                    "configuracao": {
                        "host": "ftp.clientea.com",
                        "user": "usuario",
                        "password": "senha"
                    }
                },
                {
                    "tipo_metodo": "api",
                    "configuracao": {
                        "url": "https://api.clientea.com",
                        "token": "abc123"
                    }
                }
            ]
        }
    ]
}

# Inserir o documento na coleção 'clientes'
result = clientes.insert_one(cliente_data)
print(f"Cliente inserido com o ID: {result.inserted_id}")
