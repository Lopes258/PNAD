#!/usr/bin/env python3
"""
Script de teste para a API PNAD
Testa a conexão com o banco e a extração de dados
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from api_PNAD import get_sidra_table, create_pnad_tables, query_pnad_data
from database import DatabaseConnection

def test_database_connection():
    """Testa a conexão com o banco de dados"""
    print("🔌 Testando conexão com o banco de dados...")
    
    db = DatabaseConnection()
    if db.test_connection():
        print("✅ Conexão com o banco estabelecida com sucesso!")
        return True
    else:
        print("❌ Falha na conexão com o banco")
        return False

def test_api_connection():
    """Testa a conexão com a API SIDRA"""
    print("\n🌐 Testando conexão com a API SIDRA...")
    
    try:
        # Testa com uma tabela pequena
        df = get_sidra_table(table_id="5440", variables="all", period="2023", geo="N1")
        
        if df is not None and not df.empty:
            print(f"✅ API funcionando! Extraídos {len(df)} registros")
            print(f"   Colunas disponíveis: {list(df.columns)}")
            print(f"   Primeiros dados:")
            print(df.head(3).to_string())
            return True
        else:
            print("❌ API retornou dados vazios")
            return False
            
    except Exception as e:
        print(f"❌ Erro na API: {e}")
        return False

def test_table_creation():
    """Testa a criação das tabelas"""
    print("\n🗄️ Testando criação das tabelas...")
    
    try:
        if create_pnad_tables():
            print("✅ Tabelas criadas/verificadas com sucesso!")
            return True
        else:
            print("❌ Falha ao criar tabelas")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao criar tabelas: {e}")
        return False

def test_single_table_extraction():
    """Testa a extração de uma única tabela"""
    print("\n📊 Testando extração de tabela única...")
    
    try:
        # Testa com tabela de ocupação
        df = get_sidra_table(table_id="5440", variables="all", period="2023", geo="N3")
        
        if df is not None and not df.empty:
            print(f"✅ Extração bem-sucedida! {len(df)} registros")
            print(f"   Estrutura dos dados:")
            print(f"   - Localidades: {df['Localidade'].nunique()}")
            print(f"   - Variáveis: {df['Variável'].nunique()}")
            print(f"   - Períodos: {df['Período'].nunique()}")
            return True
        else:
            print("❌ Falha na extração")
            return False
            
    except Exception as e:
        print(f"❌ Erro na extração: {e}")
        return False

def main():
    """Função principal de teste"""
    print("🧪 INICIANDO TESTES DA API PNAD")
    print("=" * 50)
    
    tests = [
        ("Conexão com banco", test_database_connection),
        ("Conexão com API", test_api_connection),
        ("Criação de tabelas", test_table_creation),
        ("Extração de dados", test_single_table_extraction)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ Erro no teste '{test_name}': {e}")
            results.append((test_name, False))
    
    # Resumo dos resultados
    print("\n" + "=" * 50)
    print("📋 RESUMO DOS TESTES")
    print("=" * 50)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASSOU" if result else "❌ FALHOU"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\nResultado: {passed}/{total} testes passaram")
    
    if passed == total:
        print("\n🎉 Todos os testes passaram! A API PNAD está funcionando corretamente.")
        print("Você pode executar o script principal para extrair todos os dados:")
        print("python api_PNAD.py")
    else:
        print("\n⚠️ Alguns testes falharam. Verifique os erros acima antes de prosseguir.")

if __name__ == "__main__":
    main()
