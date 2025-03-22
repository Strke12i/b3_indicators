from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from db import DatabaseConnection
from api import insert_data

app = FastAPI()

class FinancialData(BaseModel):
    company_name: str
    code_cvm: int
    data: dict  # Dados financeiros processados

@app.post("/insert-data")
async def insert_financial_data(financial_data: FinancialData):
    try:
        db_connection = DatabaseConnection()
        session = db_connection.get_session()

        print("Inserindo dados no banco de dados...")
        print(financial_data.company_name)

        # Insere os dados no banco de dados
        data = financial_data.data
        dates = sorted(data.keys())
        for date in dates:
            data_to_insert = data[date]
            print(f"Inserindo dados para a data {date}")
            insert_data(session, data_to_insert, financial_data.company_name, int(financial_data.code_cvm))
        return {"message": "Dados inseridos com sucesso!"}
    except Exception as e:
        print(f"Erro ao inserir dados no banco de dados: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/test")
async def test():
    return {"message": "Teste OK!"}