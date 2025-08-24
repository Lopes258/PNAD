#!/usr/bin/env python3
"""
Script de teste para a API IBGE
Testa a conexão com o banco e a extração de dados geográficos
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from api_IBGE import get_geo, get_location_info, create_ibge_tables, query_ibge_data
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
    """Testa a conexão com a API IBGE"""
    print("\n🌐 Testando conexão com a API IBGE...")
    
    try:
        # Testa com informações de estados (tabela pequena)
        df = get_location_info(geo_level="N2")
        
        if df is not None and not df.empty:
            print(f"✅ API funcionando! Extraídas {len(df)} localidades")
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
        if create_ibge_tables():
            print("✅ Tabelas criadas/verificadas com sucesso!")
            return True
        else:
            print("❌ Falha ao criar tabelas")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao criar tabelas: {e}")
        return False

def test_single_location_extraction():
    """Testa a extração de informações de localidades"""
    print("\n📊 Testando extração de localidades...")
    
    try:
        # Testa com estados
        df = get_location_info(geo_level="N2")
        
        if df is not None and not df.empty:
            print(f"✅ Extração bem-sucedida! {len(df)} localidades")
            print(f"   Estrutura dos dados:")
            print(f"   - Estados: {df['nome'].nunique()}")
            print(f"   - Siglas: {df['sigla'].nunique()}")
            return True
        else:
            print("❌ Falha na extração")
            return False
            
    except Exception as e:
        print(f"❌ Erro na extração: {e}")
        return False

def test_geographic_malha():
    """Testa a extração de malhas geográficas"""
    print("\n🗺️ Testando extração de malhas geográficas...")
    
    try:
        # Testa com malha do Brasil (mais leve)
        gdf = get_geo(geo_level="N1")
        
        if gdf is not None and not gdf.empty:
            print(f"✅ Malha extraída com sucesso! {len(gdf)} feições")
            print(f"   Estrutura da malha:")
            print(f"   - Colunas: {list(gdf.columns)}")
            print(f"   - Geometria: {gdf.geometry.geom_type.iloc[0] if hasattr(gdf, 'geometry') else 'N/A'}")
            return True
        else:
            print("❌ Falha na extração da malha")
            return False
            
    except Exception as e:
        print(f"❌ Erro na extração da malha: {e}")
        return False

def main():
    """Função principal de teste"""
    print("🧪 INICIANDO TESTES DA API IBGE")
    print("=" * 50)
    
    tests = [
        ("Conexão com banco", test_database_connection),
        ("Conexão com API", test_api_connection),
        ("Criação de tabelas", test_table_creation),
        ("Extração de localidades", test_single_location_extraction),
        ("Extração de malhas", test_geographic_malha)
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
        print("\n🎉 Todos os testes passaram! A API IBGE está funcionando corretamente.")
        print("Você pode executar o script principal para extrair todos os dados:")
        print("python api_IBGE.py")
    else:
        print("\n⚠️ Alguns testes falharam. Verifique os erros acima antes de prosseguir.")

if __name__ == "__main__":
    main()
