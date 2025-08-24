import requests
import geopandas as gpd
import pandas as pd
import logging
import time
from io import BytesIO
from database import DatabaseConnection
import json

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ------------------------------
# Função para baixar malhas geográficas (GeoJSON)
# ------------------------------
def get_geo(geo_level="N3", code=None, retry_count=3):
    """
    Baixa malha geográfica do IBGE em GeoJSON.
    
    Params:
        geo_level (str): nível geográfico (N1=Brasil, N2=UF, N3=RM, N6=Município)
        code (str): código da localidade (opcional). Ex: "35" = São Paulo
        retry_count (int): número de tentativas em caso de falha
        
    Retorna:
        GeoDataFrame ou None se falhar
    """
    for attempt in range(retry_count):
        try:
            url = f"https://servicodados.ibge.gov.br/api/v2/malhas/{code if code else ''}?formato=application/vnd.geo+json"
            logger.info(f"Baixando malha geográfica: {url}")
            
            response = requests.get(url, timeout=60)
            
            if response.status_code == 200:
                gdf = gpd.read_file(url)
                
                if gdf.empty:
                    logger.warning(f"Malha geográfica vazia para código {code}")
                    return None
                
                # Adiciona metadados
                gdf['geo_level'] = geo_level
                gdf['ibge_code'] = code if code else 'BR'
                gdf['data_extracao'] = pd.Timestamp.now()
                
                logger.info(f"Malha geográfica baixada com sucesso: {len(gdf)} feições")
                return gdf
                
            else:
                logger.error(f"Erro na requisição: {response.status_code} - {response.text}")
                if attempt < retry_count - 1:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    return None
                    
        except Exception as e:
            logger.error(f"Erro ao baixar malha geográfica na tentativa {attempt + 1}: {e}")
            if attempt < retry_count - 1:
                time.sleep(2 ** attempt)
                continue
            else:
                return None
    
    return None

# ------------------------------
# Função para obter informações das localidades
# ------------------------------
def get_location_info(geo_level="N3", retry_count=3):
    """
    Obtém informações das localidades disponíveis para um nível geográfico.
    
    Params:
        geo_level (str): nível geográfico
        retry_count (int): número de tentativas
        
    Retorna:
        DataFrame com informações das localidades
    """
    for attempt in range(retry_count):
        try:
            if geo_level == "N1":
                url = "https://servicodados.ibge.gov.br/api/v1/localidades/paises/76"
            elif geo_level == "N2":
                url = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"
            elif geo_level == "N3":
                url = "https://servicodados.ibge.gov.br/api/v1/localidades/regioes-metropolitanas"
            elif geo_level == "N6":
                url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
            else:
                logger.error(f"Nível geográfico não suportado: {geo_level}")
                return None
            
            logger.info(f"Obtendo informações de localidades: {url}")
            
            response = requests.get(url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                df = pd.DataFrame(data)
                
                if df.empty:
                    logger.warning(f"Nenhuma localidade encontrada para nível {geo_level}")
                    return None
                
                # Adiciona metadados
                df['geo_level'] = geo_level
                df['data_extracao'] = pd.Timestamp.now()
                
                logger.info(f"Informações obtidas: {len(df)} localidades")
                return df
                
            else:
                logger.error(f"Erro na requisição: {response.status_code}")
                if attempt < retry_count - 1:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    return None
                    
        except Exception as e:
            logger.error(f"Erro ao obter informações na tentativa {attempt + 1}: {e}")
            if attempt < retry_count - 1:
                time.sleep(2 ** attempt)
                continue
            else:
                return None
    
    return None

# ------------------------------
# Funções para banco de dados
# ------------------------------
def create_ibge_tables():
    """Cria as tabelas necessárias para os dados IBGE"""
    db = DatabaseConnection()
    conn = db.get_connection()
    
    if not conn:
        logger.error("Não foi possível conectar ao banco de dados")
        return False
    
    try:
        cursor = conn.cursor()
        
        # Tabela para malhas geográficas
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ibge_malhas' AND xtype='U')
        CREATE TABLE ibge_malhas (
            id INT IDENTITY(1,1) PRIMARY KEY,
            nome NVARCHAR(255),
            codigo_ibge NVARCHAR(50),
            nivel_geografico NVARCHAR(10),
            geometria NVARCHAR(MAX),
            propriedades NVARCHAR(MAX),
            data_extracao DATETIME,
            data_criacao DATETIME DEFAULT GETDATE()
        )
        """)
        
        # Tabela para informações de localidades
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ibge_localidades' AND xtype='U')
        CREATE TABLE ibge_localidades (
            id INT IDENTITY(1,1) PRIMARY KEY,
            nome NVARCHAR(255),
            codigo_ibge NVARCHAR(50),
            nivel_geografico NVARCHAR(10),
            sigla NVARCHAR(10),
            regiao NVARCHAR(255),
            uf NVARCHAR(255),
            municipio NVARCHAR(255),
            propriedades NVARCHAR(MAX),
            data_extracao DATETIME,
            data_criacao DATETIME DEFAULT GETDATE()
        )
        """)
        
        # Tabela para log de extrações
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ibge_log_extracao' AND xtype='U')
        CREATE TABLE ibge_log_extracao (
            id INT IDENTITY(1,1) PRIMARY KEY,
            tipo_extracao NVARCHAR(50),
            nivel_geografico NVARCHAR(10),
            codigo_ibge NVARCHAR(50),
            registros_extraidos INT,
            status NVARCHAR(50),
            mensagem NVARCHAR(500),
            data_extracao DATETIME,
            data_criacao DATETIME DEFAULT GETDATE()
        )
        """)
        
        conn.commit()
        logger.info("Tabelas IBGE criadas com sucesso")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao criar tabelas: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def insert_malha_to_sql(gdf, table_name="ibge_malhas"):
    """Insere dados de malha geográfica no banco SQL"""
    if gdf is None or gdf.empty:
        logger.warning("Nenhuma malha geográfica para inserir")
        return False
    
    db = DatabaseConnection()
    conn = db.get_connection()
    
    if not conn:
        logger.error("Não foi possível conectar ao banco de dados")
        return False
    
    try:
        cursor = conn.cursor()
        
        for idx, row in gdf.iterrows():
            nome = row.get('nome', '')
            codigo_ibge = row.get('codigo_ibge', '')
            nivel_geografico = row.get('geo_level', '')
            
            # Converte geometria para WKT (Well-Known Text)
            geometria = row.geometry.wkt if hasattr(row, 'geometry') else ''
            
            # Converte propriedades para JSON
            propriedades = json.dumps(row.to_dict(), ensure_ascii=False, default=str)
            
            data_extracao = row.get('data_extracao', pd.Timestamp.now())
            
            cursor.execute(f"""
            INSERT INTO {table_name} 
            (nome, codigo_ibge, nivel_geografico, geometria, propriedades, data_extracao)
            VALUES (?, ?, ?, ?, ?, ?)
            """, (nome, codigo_ibge, nivel_geografico, geometria, propriedades, data_extracao))
        
        conn.commit()
        logger.info(f"{len(gdf)} registros de malha inseridos na tabela {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao inserir malha na tabela {table_name}: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def insert_localidades_to_sql(df, table_name="ibge_localidades"):
    """Insere informações de localidades no banco SQL"""
    if df is None or df.empty:
        logger.warning("Nenhuma localidade para inserir")
        return False
    
    db = DatabaseConnection()
    conn = db.get_connection()
    
    if not conn:
        logger.error("Não foi possível conectar ao banco de dados")
        return False
    
    try:
        cursor = conn.cursor()
        
        for idx, row in df.iterrows():
            nome = row.get('nome', '')
            codigo_ibge = str(row.get('id', ''))
            nivel_geografico = row.get('geo_level', '')
            sigla = row.get('sigla', '')
            regiao = row.get('regiao', {}).get('nome', '') if isinstance(row.get('regiao'), dict) else ''
            uf = row.get('uf', {}).get('nome', '') if isinstance(row.get('uf'), dict) else ''
            municipio = row.get('municipio', {}).get('nome', '') if isinstance(row.get('municipio'), dict) else ''
            
            # Converte propriedades para JSON
            propriedades = json.dumps(row.to_dict(), ensure_ascii=False, default=str)
            
            data_extracao = row.get('data_extracao', pd.Timestamp.now())
            
            cursor.execute(f"""
            INSERT INTO {table_name} 
            (nome, codigo_ibge, nivel_geografico, sigla, regiao, uf, municipio, propriedades, data_extracao)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (nome, codigo_ibge, nivel_geografico, sigla, regiao, uf, municipio, propriedades, data_extracao))
        
        conn.commit()
        logger.info(f"{len(df)} registros de localidades inseridos na tabela {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao inserir localidades na tabela {table_name}: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def log_extraction(tipo_extracao, nivel_geografico, codigo_ibge, registros_extraidos, status, mensagem=""):
    """Registra log da extração"""
    db = DatabaseConnection()
    conn = db.get_connection()
    
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
        INSERT INTO ibge_log_extracao 
        (tipo_extracao, nivel_geografico, codigo_ibge, registros_extraidos, status, mensagem, data_extracao)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (tipo_extracao, nivel_geografico, codigo_ibge, registros_extraidos, status, mensagem, pd.Timestamp.now()))
        
        conn.commit()
        return True
        
    except Exception as e:
        logger.error(f"Erro ao registrar log: {e}")
        return False
    finally:
        conn.close()

# ------------------------------
# Função principal de extração
# ------------------------------
def extract_all_ibge_data():
    """Extrai todos os dados IBGE e salva no banco"""
    logger.info("Iniciando extração de dados IBGE...")
    
    # Cria as tabelas se não existirem
    if not create_ibge_tables():
        logger.error("Falha ao criar tabelas")
        return False
    
    success_count = 0
    total_operations = 0
    
    # Lista de operações para extrair
    operations = [
        {"type": "localidades", "geo_level": "N1", "description": "País Brasil"},
        {"type": "localidades", "geo_level": "N2", "description": "Estados"},
        {"type": "localidades", "geo_level": "N3", "description": "Regiões Metropolitanas"},
        {"type": "localidades", "geo_level": "N6", "description": "Municípios"},
        {"type": "malhas", "geo_level": "N1", "description": "Malha do Brasil"},
        {"type": "malhas", "geo_level": "N2", "description": "Malhas dos Estados"},
    ]
    
    for operation in operations:
        total_operations += 1
        try:
            logger.info(f"Processando: {operation['description']}")
            
            if operation['type'] == "localidades":
                # Extrai informações de localidades
                df = get_location_info(geo_level=operation['geo_level'])
                
                if df is not None and not df.empty:
                    if insert_localidades_to_sql(df):
                        log_extraction(
                            "LOCALIDADES",
                            operation['geo_level'],
                            "ALL",
                            len(df),
                            "SUCESSO",
                            f"Extraídas {len(df)} localidades"
                        )
                        success_count += 1
                    else:
                        log_extraction(
                            "LOCALIDADES",
                            operation['geo_level'],
                            "ALL",
                            0,
                            "ERRO_INSERCAO",
                            "Falha ao inserir localidades no banco"
                        )
                else:
                    log_extraction(
                        "LOCALIDADES",
                        operation['geo_level'],
                        "ALL",
                        0,
                        "DADOS_VAZIOS",
                        "API retornou dados vazios"
                    )
                    
            elif operation['type'] == "malhas":
                # Extrai malhas geográficas
                if operation['geo_level'] == "N1":
                    gdf = get_geo(geo_level=operation['geo_level'])
                else:
                    # Para outros níveis, pega algumas localidades como exemplo
                    df_loc = get_location_info(geo_level=operation['geo_level'])
                    if df_loc is not None and not df_loc.empty:
                        # Pega apenas as primeiras 3 localidades para não sobrecarregar
                        sample_codes = df_loc.head(3)['id'].astype(str).tolist()
                        for code in sample_codes:
                            gdf = get_geo(geo_level=operation['geo_level'], code=code)
                            if gdf is not None and not gdf.empty:
                                if insert_malha_to_sql(gdf):
                                    log_extraction(
                                        "MALHAS",
                                        operation['geo_level'],
                                        code,
                                        len(gdf),
                                        "SUCESSO",
                                        f"Malha extraída com sucesso"
                                    )
                                    success_count += 1
                                else:
                                    log_extraction(
                                        "MALHAS",
                                        operation['geo_level'],
                                        code,
                                        0,
                                        "ERRO_INSERCAO",
                                        "Falha ao inserir malha no banco"
                                    )
                            else:
                                log_extraction(
                                    "MALHAS",
                                    operation['geo_level'],
                                    code,
                                    0,
                                    "DADOS_VAZIOS",
                                    "API retornou malha vazia"
                                )
                        continue
                    else:
                        continue
                
                if gdf is not None and not gdf.empty:
                    if insert_malha_to_sql(gdf):
                        log_extraction(
                            "MALHAS",
                            operation['geo_level'],
                            "ALL",
                            len(gdf),
                            "SUCESSO",
                            f"Malha extraída com sucesso"
                        )
                        success_count += 1
                    else:
                        log_extraction(
                            "MALHAS",
                            operation['geo_level'],
                            "ALL",
                            0,
                            "ERRO_INSERCAO",
                            "Falha ao inserir malha no banco"
                        )
                else:
                    log_extraction(
                        "MALHAS",
                        operation['geo_level'],
                        "ALL",
                        0,
                        "DADOS_VAZIOS",
                        "API retornou malha vazia"
                    )
                    
        except Exception as e:
            logger.error(f"Erro ao processar {operation['description']}: {e}")
            log_extraction(
                operation['type'].upper(),
                operation['geo_level'],
                "ALL",
                0,
                "ERRO_EXTRACAO",
                str(e)
            )
    
    logger.info(f"Extração concluída. {success_count}/{total_operations} operações processadas com sucesso")
    return success_count > 0

# ------------------------------
# Função para consultar dados
# ------------------------------
def query_ibge_data(table_name, limit=100):
    """Consulta dados das tabelas IBGE"""
    db = DatabaseConnection()
    conn = db.get_connection()
    
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT TOP {limit} * FROM {table_name} ORDER BY data_criacao DESC")
        
        columns = [column[0] for column in cursor.description]
        results = []
        
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        
        return results
        
    except Exception as e:
        logger.error(f"Erro ao consultar dados: {e}")
        return None
    finally:
        conn.close()

# ------------------------------
# Funções de exemplo e utilitárias
# ------------------------------
def get_estados_info():
    """Obtém informações de todos os estados"""
    return get_location_info(geo_level="N2")

def get_municipios_info():
    """Obtém informações de todos os municípios"""
    return get_location_info(geo_level="N6")

def get_brasil_malha():
    """Obtém malha geográfica do Brasil"""
    return get_geo(geo_level="N1")

def get_estado_malha(codigo_estado):
    """Obtém malha geográfica de um estado específico"""
    return get_geo(geo_level="N2", code=codigo_estado)

def get_sidra_table(table_id, variables="all", period="last 3", geo="n3", retry_count=3):
    """
    Extrai dados da API SIDRA (IBGE) com estrutura correta de URL.
    
    Params:
        table_id (str): ID da tabela no SIDRA (ex: "7060")
        variables (str): variáveis (ex: "all" ou "2265")
        period (str): período ("last 3" para últimos 3 meses, "all" para tudo, ou ano específico)
        geo (str): nível geográfico ("n1"=Brasil, "n2"=UF, "n3"=RM, "n6"=Município)
        retry_count (int): número de tentativas em caso de falha
    
    Retorna:
        pandas.DataFrame ou None se falhar
    """
    # URL base da API SIDRA (única que funciona)
    base_url = "https://apisidra.ibge.gov.br"
    
    # URL encoding correto para o período
    if period == "last 3":
        encoded_period = "last%203"
    else:
        encoded_period = period
    
    # Tenta diferentes níveis geográficos se o principal falhar
    geo_options = [geo, "n1", "n2"]  # Tenta n3, depois n1 (Brasil), depois n2 (UF)
    
    for geo_option in geo_options:
        for attempt in range(retry_count):
            try:
                # Estrutura correta da URL SIDRA: /values/t/{tabela}/v/{variaveis}/p/{periodo}/n{territorio}/all
                url = f"{base_url}/values/t/{table_id}/v/{variables}/p/{encoded_period}/{geo_option}/all"
                logger.info(f"Tentativa {attempt + 1} com geo: {geo_option}")
                logger.info(f"Fazendo requisição para: {url}")
                
                response = requests.get(url, timeout=45)
                
                if response.status_code == 200:
                    data = response.json()
                    if not data:
                        logger.warning(f"Tabela {table_id} retornou dados vazios")
                        continue  # Tenta próxima opção geográfica
                    
                    df = pd.DataFrame(data)
                    
                    # Verifica se há dados
                    if df.empty:
                        logger.warning(f"Tabela {table_id} está vazia")
                        continue  # Tenta próxima opção geográfica
                    
                    # Limpa os nomes das colunas (remove espaços em branco)
                    df.columns = [c.strip() for c in df.columns]
                    
                    # NÃO renomeia as colunas - mantém os nomes originais da API
                    # As colunas mantêm seus nomes originais como D1N, D2N, V, D3N, D4N, etc.
                    
                    # Adiciona metadados da tabela
                    df['Tabela_ID'] = table_id
                    df['Data_Extracao'] = pd.Timestamp.now()
                    df['Nivel_Geografico'] = geo_option
                    
                    logger.info(f"✅ Tabela {table_id} extraída com sucesso (geo: {geo_option}): {len(df)} registros")
                    logger.info(f"Colunas extraídas: {list(df.columns)}")
                    return df
                    
                else:
                    logger.error(f"Erro na requisição (geo: {geo_option}): {response.status_code} - {response.text}")
                    if attempt < retry_count - 1:
                        time.sleep(2 ** attempt)  # Backoff exponencial
                        continue
                    else:
                        break  # Tenta próxima opção geográfica
                        
            except requests.exceptions.RequestException as e:
                logger.error(f"Erro de requisição na tentativa {attempt + 1} (geo: {geo_option}): {e}")
                if attempt < retry_count - 1:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    break  # Tenta próxima opção geográfica
            except Exception as e:
                logger.error(f"Erro inesperado na tentativa {attempt + 1} (geo: {geo_option}): {e}")
                if attempt < retry_count - 1:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    break  # Tenta próxima opção geográfica
        
        # Se chegou aqui, tenta próxima opção geográfica
        logger.info(f"Tentando próxima opção geográfica após falhar com {geo_option}")
    
    logger.error(f"❌ Todas as opções geográficas falharam para a tabela {table_id}")
    return None

# ------------------------------
# Execução principal (só roda se chamado diretamente)
# ------------------------------
if __name__ == "__main__":
    print("🗺️ Iniciando extração de dados IBGE...")
    success = extract_all_ibge_data()
    
    if success:
        print("✅ Extração concluída com sucesso!")
        
        # Mostra alguns dados de exemplo
        print("\n📊 Localidades extraídas (últimos 5 registros):")
        localidades_data = query_ibge_data("ibge_localidades", 5)
        if localidades_data:
            for row in localidades_data:
                print(f"  {row['nome']} ({row['nivel_geografico']}) - {row['codigo_ibge']}")
    else:
        print("❌ Houve problemas na extração. Verifique os logs.")
