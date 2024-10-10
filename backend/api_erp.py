from fastapi import FastAPI, HTTPException
import psycopg2
import os
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime
decimal = psycopg2.extensions.new_type(psycopg2._psycopg.DECIMAL.values, 'DEC2FLOAT', lambda value, curs: float(value) if value is not None else None)
psycopg2.extensions.register_type(decimal)

app = FastAPI()

# Configurações do banco de dados
DB_USER = "postgres"
DB_PASSWORD = "1234"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "sellout"

class SelloutRecord(BaseModel):
    id: Optional[int]
    product_name: Optional[str]
    customer_name: Optional[str]
    sale_date: Optional[datetime]
    quantity: Optional[int]
    sale_amount: Optional[float]
    store_location: Optional[str]

# Função para conectar ao banco de dados
def get_db_connection():
    connection = psycopg2.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME
    )
    return connection

@app.get("/sellout", response_model=List[SelloutRecord])
async def get_sellout():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM sellout;")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        result = [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()
    return result