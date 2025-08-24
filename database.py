import os
import pyodbc
from dotenv import load_dotenv

# Carrega as variáveis de ambiente
load_dotenv('config.env')

class DatabaseConnection:
    def __init__(self):
        self.server = os.getenv('DB_SERVER', 'localhost')
        self.port = os.getenv('DB_PORT', '1433')
        self.database = os.getenv('DB_DATABASE', 'LOPES')
        self.trusted_connection = os.getenv('DB_TRUSTED_CONNECTION', 'True').lower() == 'true'
        
    def get_connection_string(self, database='LOPES'):
        """Retorna a string de conexão para pyodbc com autenticação Windows"""
        if self.trusted_connection:
            # Tenta sem porta primeiro (instância padrão)
            return f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server};DATABASE={database};Trusted_Connection=yes;"
        else:
            # Fallback para autenticação SQL (se necessário)
            user = os.getenv('DB_USER', '')
            password = os.getenv('DB_PASSWORD', '')
            return f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server};DATABASE={database};UID={user};PWD={password}"
    
    def get_connection(self, database='LOPES'):
        """Cria conexão usando pyodbc com autenticação Windows"""
        try:
            connection = pyodbc.connect(self.get_connection_string(database))
            return connection
        except Exception as e:
            print(f"Erro ao conectar com pyodbc: {e}")
            return None
    
    def create_database(self):
        """Cria o banco de dados LOPES se não existir"""
        try:
            # Conecta no LOPES para criar o banco
            conn = self.get_connection('LOPES')
            if not conn:
                return False
            
            # Habilita autocommit para CREATE DATABASE
            conn.autocommit = True
                
            cursor = conn.cursor()
            
            # Verifica se o banco já existe
            cursor.execute("SELECT name FROM sys.databases WHERE name = ?", (self.database,))
            if cursor.fetchone():
                print(f"Banco de dados '{self.database}' já existe!")
                cursor.close()
                conn.close()
                return True
            
            # Cria o banco de dados (sem transação)
            cursor.execute(f"CREATE DATABASE [{self.database}]")
            cursor.close()
            conn.close()
            
            print(f"Banco de dados '{self.database}' criado com sucesso!")
            return True
            
        except Exception as e:
            print(f"Erro ao criar banco de dados: {e}")
            return False
    
    def test_connection(self):
        """Testa a conexão com o banco"""
        print("Tentando conectar com pyodbc...")
        try:
            conn = self.get_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("SELECT @@version")
                version = cursor.fetchone()
                cursor.close()
                conn.close()
                print(f"Conexão bem-sucedida! Versão do SQL Server: {version[0]}")
                return True
        except Exception as e:
            print(f"Erro com pyodbc: {e}")
            
        return False
    
    def create_tables(self):
        """Cria as tabelas necessárias"""
        try:
            conn = self.get_connection()
            if not conn:
                return False
                
            cursor = conn.cursor()
            
            # Tabela para dados PNAD
            cursor.execute('''
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='pnad_data' AND xtype='U')
                CREATE TABLE pnad_data (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    ano INT,
                    trimestre INT,
                    uf NVARCHAR(2),
                    sexo NVARCHAR(10),
                    idade INT,
                    raca NVARCHAR(20),
                    escolaridade NVARCHAR(50),
                    renda DECIMAL(10,2),
                    ocupacao NVARCHAR(100),
                    setor NVARCHAR(50),
                    created_at DATETIME DEFAULT GETDATE()
                )
            ''')
            
            # Tabela para metadados
            cursor.execute('''
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='pnad_metadata' AND xtype='U')
                CREATE TABLE pnad_metadata (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    ano INT,
                    trimestre INT,
                    uf NVARCHAR(2),
                    total_registros INT,
                    data_extracao DATETIME DEFAULT GETDATE()
                )
            ''')
            
            conn.commit()
            cursor.close()
            conn.close()
            print("Tabelas criadas com sucesso!")
            return True
            
        except Exception as e:
            print(f"Erro ao criar tabelas: {e}")
            return False

# Exemplo de uso
if __name__ == "__main__":
    db = DatabaseConnection()
    print("Testando conexão com SQL Server...")
    print(f"Servidor: {db.server}")
    print(f"Porta: {db.port}")
    print(f"Banco: {db.database}")
    print(f"Autenticação Windows: {db.trusted_connection}")
    print("-" * 50)
    
    # Primeiro testa conexão com LOPES
    print("Testando conexão com LOPES...")
    if db.get_connection('LOPES'):
        print("✅ CONEXÃO COM MASTER BEM-SUCEDIDA!")
        
        # Cria o banco LOPES se não existir
        print(f"Criando banco de dados '{db.database}'...")
        if db.create_database():
            print("✅ BANCO CRIADO COM SUCESSO!")
            
            # Agora testa conexão com o banco LOPES
            print(f"Testando conexão com banco '{db.database}'...")
            if db.test_connection():
                print("✅ CONEXÃO COM LOPES BEM-SUCEDIDA!")
                
                # Cria as tabelas
                print("Criando tabelas...")
                if db.create_tables():
                    print("✅ TABELAS CRIADAS COM SUCESSO!")
                else:
                    print("❌ ERRO AO CRIAR TABELAS")
            else:
                print("❌ ERRO AO CONECTAR COM LOPES")
        else:
            print("❌ ERRO AO CRIAR BANCO")
    else:
        print("❌ ERRO AO CONECTAR COM MASTER") 