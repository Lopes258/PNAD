import requests
import pandas as pd
import logging
import json
from database import DatabaseConnection
import time
import re
import unicodedata
from datetime import datetime

# ------------------------------
# Configuração de logging
# ------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ------------------------------
# Constantes
# ------------------------------
MAX_COLUMN_LENGTH = 128  # Limite do SQL Server
MAX_VARCHAR_LENGTH = 255  # Tamanho padrão para colunas textuais

# ------------------------------
# Função para normalizar nomes de colunas
# ------------------------------
def normalize_column_names(columns):
    new_columns = []
    counts = {}
    
    for col in columns:
        col_name = str(col).strip().lower()
        col_name = ''.join(c for c in unicodedata.normalize('NFKD', col_name) if not unicodedata.combining(c))
        col_name = re.sub(r'[^a-zA-Z0-9_]', '_', col_name)
        col_name = re.sub(r'_+', '_', col_name).strip('_')
        if not col_name:
            col_name = f"col_{len(new_columns)}"
        if col_name[0].isdigit():
            col_name = f"col_{col_name}"
        col_name = col_name[:MAX_COLUMN_LENGTH].rstrip('_')
        original_name = col_name
        count = counts.get(original_name, 0)
        if count > 0:
            suffix = f"_{count}"
            col_name = original_name[:MAX_COLUMN_LENGTH - len(suffix)] + suffix
        counts[original_name] = count + 1
        new_columns.append(col_name)
    
    return new_columns

# ------------------------------
# Função para pivotar dados SIDRA
# ------------------------------
def pivot_sidra_data(df):
    if df is None or df.empty:
        logger.warning("DataFrame vazio para pivotar.")
        return None

    required_cols = ["V", "D1N", "D2N", "D3N"]
    if not all(col in df.columns for col in required_cols):
        logger.error(f"Colunas necessárias ausentes: {set(required_cols) - set(df.columns)}")
        return None

    try:
        df["V"] = pd.to_numeric(df["V"], errors='coerce')
        df_cleaned = df.dropna(subset=["V"]).copy()
        if df_cleaned.empty:
            logger.warning("Nenhum dado válido após limpeza.")
            return None

        df_pivot = df_cleaned.pivot_table(
            index=["D2N", "D3N", "Tabela_ID", "Data_Extracao", "Nivel_Geografico"],
            columns="D1N",
            values="V",
            aggfunc='first',
            fill_value=0
        ).reset_index()

        df_pivot.columns = normalize_column_names(df_pivot.columns)
        logger.info(f"Dados pivotados com sucesso: {len(df_pivot)} registros, {len(df_pivot.columns)} colunas")
        return df_pivot

    except Exception as e:
        logger.error(f"Erro ao pivotar dados: {e}")
        return None

# ------------------------------
# Função para extrair tabelas SIDRA
# ------------------------------
def get_sidra_table(table_id, variables="all", period="last 3", geo="n3", retry_count=3):
    base_url = "https://apisidra.ibge.gov.br"
    encoded_period = "last%203" if period == "last 3" else period
    geo_options = [geo, "n1", "n2", "n6"]

    for geo_option in geo_options:
        for attempt in range(retry_count):
            try:
                url = f"{base_url}/values/t/{table_id}/v/{variables}/p/{encoded_period}/{geo_option}/all"
                logger.info(f"Tentativa {attempt + 1} para tabela {table_id} com geo: {geo_option}")

                response = requests.get(url, timeout=60)
                if response.status_code == 200:
                    data = response.json()
                    if not data or len(data) <= 1:
                        logger.warning(f"Tabela {table_id} retornou dados vazios ou insuficientes")
                        continue

                    df = pd.DataFrame(data[1:], columns=data[0])
                    df["Tabela_ID"] = table_id
                    df["Data_Extracao"] = pd.Timestamp.now()
                    df["Nivel_Geografico"] = geo_option

                    logger.info(f"Tabela {table_id} extraída: {len(df)} registros")
                    return df
                else:
                    logger.error(f"Erro HTTP {response.status_code}: {response.text}")
                    time.sleep(2 ** attempt)
            except requests.exceptions.Timeout:
                logger.error(f"Timeout na tentativa {attempt + 1} para tabela {table_id}")
                time.sleep(5)
            except Exception as e:
                logger.error(f"Erro de requisição: {e}")
                time.sleep(2 ** attempt)
                
    logger.error(f"Todas as tentativas falharam para a tabela {table_id}")
    return None

# ------------------------------
# Funções para banco de dados
# ------------------------------
def create_dynamic_table(table_name, df):
    db = DatabaseConnection()
    conn = db.get_connection()
    if not conn:
        logger.error("Não foi possível conectar ao banco de dados")
        return False

    try:
        cursor = conn.cursor()
        dtype_mapping = {
            "object": f"NVARCHAR({MAX_VARCHAR_LENGTH})",
            "int64": "INT",
            "float64": "FLOAT",
            "datetime64[ns]": "DATETIME",
            "bool": "BIT"
        }
        
        columns_sql = []
        for col, dtype in df.dtypes.items():
            sql_type = dtype_mapping.get(str(dtype), f"NVARCHAR({MAX_VARCHAR_LENGTH})")
            normalized_col = normalize_column_names([col])[0]
            columns_sql.append(f"{normalized_col} {sql_type}")
        
        create_table_query = f"""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
        CREATE TABLE {table_name} (
            id INT IDENTITY(1,1) PRIMARY KEY,
            {', '.join(columns_sql)},
            data_criacao DATETIME DEFAULT GETDATE()
        )
        """
        cursor.execute(create_table_query)
        conn.commit()
        logger.info(f"Tabela {table_name} criada ou já existente")
        return True
    except Exception as e:
        logger.error(f"Erro ao criar tabela {table_name}: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def insert_data_to_sql(df, table_name):
    if df is None or df.empty:
        logger.warning(f"Nenhum dado para inserir na tabela {table_name}")
        return False

    db = DatabaseConnection()
    conn = db.get_connection()
    if not conn:
        logger.error("Não foi possível conectar ao banco de dados")
        return False

    try:
        cursor = conn.cursor()
        normalized_columns = normalize_column_names(df.columns)
        df.columns = normalized_columns
        df = df.drop(columns=['id', 'data_criacao'], errors='ignore')

        # Trunca a tabela para substituir dados antigos
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        conn.commit()
        logger.info(f"Tabela {table_name} truncada com sucesso")

        columns = ', '.join(df.columns)
        placeholders = ', '.join(['?' for _ in df.columns])
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        batch_size = 100
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]
            records = [tuple(row) for row in batch.to_numpy()]
            cursor.executemany(insert_query, records)

        conn.commit()
        logger.info(f"{len(df)} registros inseridos na tabela {table_name}")
        return True
    except Exception as e:
        logger.error(f"Erro ao inserir dados na tabela {table_name}: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def log_extraction(table_id, registros_extraidos, status, mensagem=""):
    db = DatabaseConnection()
    conn = db.get_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='pnad_log_extracao' AND xtype='U')
        CREATE TABLE pnad_log_extracao (
            id INT IDENTITY(1,1) PRIMARY KEY,
            tabela_id NVARCHAR(50),
            data_extracao DATETIME,
            registros_extraidos INT,
            status NVARCHAR(50),
            mensagem NVARCHAR(500),
            data_criacao DATETIME DEFAULT GETDATE()
        )
        """)
        cursor.execute("""
        INSERT INTO pnad_log_extracao 
        (tabela_id, data_extracao, registros_extraidos, status, mensagem)
        VALUES (?, ?, ?, ?, ?)
        """, (table_id, pd.Timestamp.now(), registros_extraidos, status, mensagem))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Erro ao registrar log: {e}")
        return False
    finally:
        conn.close()

# ------------------------------
# Função principal
# ------------------------------
def extract_and_insert_data(table_ids):
    for table_id in table_ids:
        try:
            logger.info(f"Iniciando extração para tabela: {table_id}")
            
            df_sidra = get_sidra_table(table_id)
            if df_sidra is None or df_sidra.empty:
                log_extraction(table_id, 0, "FALHA", "Falha ao extrair dados do SIDRA")
                continue

            df_pivoted = pivot_sidra_data(df_sidra)
            if df_pivoted is None or df_pivoted.empty:
                log_extraction(table_id, 0, "FALHA", "Falha ao pivotar os dados")
                continue

            table_name = f"pnad_pivoted_{table_id}"
            if create_dynamic_table(table_name, df_pivoted):
                if insert_data_to_sql(df_pivoted, table_name):
                    log_extraction(table_id, len(df_pivoted), "SUCESSO", "Dados inseridos com sucesso")
                else:
                    log_extraction(table_id, 0, "FALHA", "Erro ao inserir dados no SQL")
            else:
                log_extraction(table_id, 0, "FALHA", "Falha ao criar tabela dinâmica")
                
        except Exception as e:
            logger.error(f"Erro inesperado processando tabela {table_id}: {e}")
            log_extraction(table_id, 0, "ERRO", f"Erro inesperado: {str(e)}")

# ------------------------------
# Exemplo de uso
# ------------------------------
if __name__ == "__main__":
    TABLE_IDS_TO_FETCH = [
        "4093",  # Força de trabalho
        "4094",  # Pessoas ocupadas
        "4095",  # Rendimento
        "5440",  # Empresas e pessoal
        "5918",  # Serviços para empresas
        "5919",  # Manutenção e reparação
        "1616",  # Domicílios por cômodos
        "1617",  # Tipos de domicílios
        "3416",  # Bens duráveis e internet
        "3516"   # Condição de ocupação
    ]
    extract_and_insert_data(TABLE_IDS_TO_FETCH)
