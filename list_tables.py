import pyodbc
from database import DatabaseConnection

def list_all_tables():
    """Lista todas as tabelas do banco de dados"""
    db = DatabaseConnection()
    conn = db.get_connection()
    
    if not conn:
        print("❌ Não foi possível conectar ao banco de dados")
        return
    
    try:
        cursor = conn.cursor()
        
        # Lista todas as tabelas do usuário
        cursor.execute("""
        SELECT 
            t.name AS table_name,
            s.name AS schema_name,
            p.rows AS row_count,
            CAST(ROUND((SUM(a.total_pages) * 8) / 1024.00, 2) AS NUMERIC(36, 2)) AS total_space_mb
        FROM sys.tables t
        INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
        INNER JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
        INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.is_ms_shipped = 0
        GROUP BY t.name, s.name, p.rows
        ORDER BY t.name
        """)
        
        tables = cursor.fetchall()
        
        if tables:
            print(f"📋 Tabelas encontradas no banco 'LOPES': {len(tables)}")
            print("-" * 80)
            print(f"{'Nome da Tabela':<30} {'Schema':<15} {'Registros':<10} {'Espaço (MB)':<12}")
            print("-" * 80)
            
            for table in tables:
                table_name, schema_name, row_count, space_mb = table
                print(f"{table_name:<30} {schema_name:<15} {row_count:<10} {space_mb:<12}")
        else:
            print("❌ Nenhuma tabela encontrada no banco")
            
        # Verifica especificamente as tabelas PNAD
        print("\n🔍 Verificando tabelas PNAD específicas:")
        pnad_tables = ['pnad_ocupacao', 'pnad_rendimento', 'pnad_domicilios', 'pnad_log_extracao']
        
        for table_name in pnad_tables:
            cursor.execute(f"""
            SELECT COUNT(*) as count FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = '{table_name}'
            """)
            exists = cursor.fetchone()[0] > 0
            
            if exists:
                # Conta registros
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                print(f"  ✅ {table_name}: {count} registros")
            else:
                print(f"  ❌ {table_name}: NÃO EXISTE")
                
    except Exception as e:
        print(f"❌ Erro ao listar tabelas: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    print("🔍 Verificando tabelas no banco de dados LOPES...")
    list_all_tables()

