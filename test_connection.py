#!/usr/bin/env python3
"""
Script para testar a conexão com SQL Server
Usa as configurações: Server=localhost;Database=master;Trusted_Connection=True;
"""

from database import DatabaseConnection
import sys

def test_master_connection():
    """Testa conexão com o banco master primeiro"""
    print("=" * 60)
    print("TESTE DE CONEXÃO COM BANCO MASTER")
    print("=" * 60)
    
    # Cria instância da conexão com banco master
    db = DatabaseConnection()
    db.database = "master"  # Força usar master
    
    print(f"Configurações:")
    print(f"  Servidor: {db.server}")
    print(f"  Banco: {db.database}")
    print(f"  Autenticação Windows: {db.trusted_connection}")
    print()
    
    print("Testando conexão com banco master...")
    print("-" * 40)
    
    # Testa a conexão
    if db.test_connection():
        print("✅ CONEXÃO COM MASTER BEM-SUCEDIDA!")
        return True
    else:
        print("❌ FALHA NA CONEXÃO COM MASTER!")
        return False

def list_databases():
    """Lista os bancos de dados disponíveis"""
    print("\n" + "=" * 60)
    print("LISTANDO BANCOS DISPONÍVEIS")
    print("=" * 60)
    
    db = DatabaseConnection()
    db.database = "master"
    
    try:
        # Tenta conectar com pyodbc
        conn = db.get_pyodbc_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sys.databases WHERE database_id > 4 ORDER BY name")
            databases = cursor.fetchall()
            cursor.close()
            conn.close()
            
            print("Bancos de dados disponíveis:")
            print("-" * 30)
            for db_name in databases:
                print(f"  - {db_name[0]}")
            
            return databases
        else:
            print("❌ Não foi possível conectar para listar bancos")
            return []
            
    except Exception as e:
        print(f"❌ Erro ao listar bancos: {e}")
        return []

def main():
    # Primeiro testa conexão com master
    if not test_master_connection():
        print("\n❌ Não foi possível conectar ao SQL Server!")
        print("Verifique se o serviço está rodando e se a autenticação Windows está habilitada.")
        return 1
    
    # Se conectou ao master, lista os bancos disponíveis
    databases = list_databases()
    
    if databases:
        print(f"\n✅ Encontrados {len(databases)} bancos de dados!")
        print("\nPara conectar ao banco LOPES, verifique se o nome está correto na lista acima.")
        print("Se não estiver listado, o banco pode não existir ou ter nome diferente.")
    else:
        print("\n❌ Não foi possível listar os bancos de dados.")
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 