from fastapi import FastAPI, HTTPException
from database import DatabaseConnection
import uvicorn
import os
from dotenv import load_dotenv

# Carrega as variáveis de ambiente
load_dotenv('config.env')

app = FastAPI(title="Projeto TL - PNAD", description="API com conexão SQL Server")

# Instância da conexão com o banco
db_connection = DatabaseConnection()

@app.get("/")
async def root():
    return {"message": "Bem-vindo ao Projeto TL - PNAD!"}

@app.get("/health")
async def health_check():
    """Verifica a saúde da aplicação e conexão com o banco"""
    try:
        # Testa a conexão com o banco
        if db_connection.test_connection():
            return {
                "status": "healthy",
                "database": "connected",
                "message": "Aplicação funcionando normalmente"
            }
        else:
            return {
                "status": "unhealthy",
                "database": "disconnected",
                "message": "Problema na conexão com o banco de dados"
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@app.get("/database/test")
async def test_database():
    """Testa especificamente a conexão com o banco de dados"""
    try:
        success = db_connection.test_connection()
        if success:
            return {"status": "success", "message": "Conexão com o banco estabelecida"}
        else:
            return {"status": "error", "message": "Falha na conexão com o banco"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao testar banco: {str(e)}")

@app.get("/database/query")
async def execute_query(query: str = "SELECT @@version"):
    """Executa uma query no banco de dados (apenas SELECT por segurança)"""
    if not query.strip().upper().startswith("SELECT"):
        raise HTTPException(status_code=400, detail="Apenas queries SELECT são permitidas")
    
    try:
        # Tenta usar pymssql primeiro
        conn = db_connection.get_pymssql_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            conn.close()
            return {"status": "success", "results": results}
        
        # Se pymssql falhar, tenta pyodbc
        conn = db_connection.get_pyodbc_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            conn.close()
            return {"status": "success", "results": results}
        
        raise HTTPException(status_code=500, detail="Não foi possível conectar ao banco")
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao executar query: {str(e)}")

if __name__ == "__main__":
    port = int(os.getenv('PORT', 8000))
    uvicorn.run(app, host="0.0.0.0", port=port) 