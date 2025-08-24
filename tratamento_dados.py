import json
import pyodbc
import os
from dotenv import load_dotenv
from datetime import datetime

# Carrega as variáveis de ambiente
load_dotenv('config.env')

class TratamentoDadosIBGE:
    def __init__(self):
        self.server = os.getenv('DB_SERVER', 'localhost')
        self.database = 'LOPES'  # Mudando para master onde estão as tabelas IBGE
        self.trusted_connection = os.getenv('DB_TRUSTED_CONNECTION', 'True').lower() == 'true'
        
    def get_connection_string(self):
        """Retorna a string de conexão para pyodbc"""
        if self.trusted_connection:
            return f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server};DATABASE={self.database};Trusted_Connection=yes;"
        else:
            user = os.getenv('DB_USER', '')
            password = os.getenv('DB_PASSWORD', '')
            return f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server};DATABASE={self.database};UID={user};PWD={password}"
    
    def get_connection(self):
        """Cria conexão usando pyodbc"""
        try:
            connection = pyodbc.connect(self.get_connection_string())
            return connection
        except Exception as e:
            print(f"Erro ao conectar com pyodbc: {e}")
            return None
    
    def criar_tabela_tratada(self):
        """Cria a tabela ibge_localidades_tratado"""
        try:
            conn = self.get_connection()
            if not conn:
                return False
                
            cursor = conn.cursor()
            
            # Script para criar a nova tabela
            create_table_sql = '''
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ibge_localidades_tratado' AND xtype='U')
            BEGIN
                CREATE TABLE [dbo].[ibge_localidades_tratado](
                    [id] [int] IDENTITY(1,1) NOT NULL,
                    [codigo_ibge] [nvarchar](50) NULL,
                    [nome] [nvarchar](255) NULL,
                    [nivel_geografico] [nvarchar](10) NULL,
                    [sigla] [nvarchar](10) NULL,
                    [regiao_id] [int] NULL,
                    [regiao_sigla] [nvarchar](10) NULL,
                    [regiao_nome] [nvarchar](255) NULL,
                    [uf_id] [int] NULL,
                    [uf_sigla] [nvarchar](10) NULL,
                    [uf_nome] [nvarchar](255) NULL,
                    [municipio_id] [int] NULL,
                    [municipio_nome] [nvarchar](255) NULL,
                    [regiao_metropolitana] [nvarchar](500) NULL,
                    [data_extracao] [datetime] NULL,
                    [data_criacao] [datetime] NOT NULL DEFAULT (GETDATE()),
                    CONSTRAINT [PK_ibge_localidades_tratado] PRIMARY KEY CLUSTERED ([id] ASC)
                )
                PRINT 'Tabela ibge_localidades_tratado criada com sucesso!'
            END
            ELSE
            BEGIN
                PRINT 'Tabela ibge_localidades_tratado já existe.'
            END
            '''
            
            cursor.execute(create_table_sql)
            conn.commit()
            cursor.close()
            conn.close()
            
            print("Tabela ibge_localidades_tratado criada/verificada com sucesso!")
            return True
            
        except Exception as e:
            print(f"Erro ao criar tabela: {e}")
            return False
    
    def processar_propriedades(self, propriedades_json):
        """Processa as propriedades JSON e retorna dados normalizados"""
        try:
            if not propriedades_json:
                return None
                
            props = json.loads(propriedades_json)
            
            # Extrair informações básicas
            dados = {
                'codigo_ibge': props.get('id', ''),
                'nome': props.get('nome', ''),
                'nivel_geografico': props.get('geo_level', ''),
                'sigla': props.get('sigla', ''),
                'regiao_id': None,
                'regiao_sigla': '',
                'regiao_nome': '',
                'uf_id': None,
                'uf_sigla': '',
                'uf_nome': '',
                'municipio_id': None,
                'municipio_nome': '',
                'regiao_metropolitana': '-',
                'data_extracao': props.get('data_extracao', '')
            }
            
            # Extrair informações da UF se existir
            if 'UF' in props:
                uf = props['UF']
                dados['uf_id'] = uf.get('id')
                dados['uf_sigla'] = uf.get('sigla', '')
                dados['uf_nome'] = uf.get('nome', '')
                
                # Extrair informações da região se existir
                if 'regiao' in uf:
                    regiao = uf['regiao']
                    dados['regiao_id'] = regiao.get('id')
                    dados['regiao_sigla'] = regiao.get('sigla', '')
                    dados['regiao_nome'] = regiao.get('nome', '')
            
            # Verificar se é região metropolitana
            if 'municipios' in props and props['municipios']:
                dados['regiao_metropolitana'] = dados['nome']
                return dados
            
            # Se não é região metropolitana, pode ser município
            if dados['nivel_geografico'] in ['N6', 'N7']:  # Níveis de município
                dados['municipio_id'] = dados['codigo_ibge']
                dados['municipio_nome'] = dados['nome']
                
                # Para municípios, se não tiver informações de UF/região, tentar buscar
                if not dados['uf_id'] and not dados['regiao_id']:
                    # Buscar informações da UF baseado no código do município
                    # Códigos de município geralmente começam com o código da UF
                    codigo_municipio = str(dados['codigo_ibge'])
                    if len(codigo_municipio) >= 2:
                        codigo_uf = codigo_municipio[:2]
                        # Aqui poderíamos buscar na tabela de UFs se necessário
                        # Por enquanto, vamos deixar para preencher depois
            
            return dados
            
        except json.JSONDecodeError as e:
            print(f"Erro ao decodificar JSON: {e}")
            return None
        except Exception as e:
            print(f"Erro ao processar propriedades: {e}")
            return None
    
    def mapear_regioes_metropolitanas(self):
        """Cria um mapeamento de municípios para suas regiões metropolitanas"""
        try:
            conn = self.get_connection()
            if not conn:
                return {}
                
            cursor = conn.cursor()
            
            # Buscar todas as regiões metropolitanas
            cursor.execute('''
                SELECT propriedades 
                FROM ibge_localidades 
                WHERE propriedades LIKE '%"municipios"%'
            ''')
            
            mapeamento = {}
            for row in cursor.fetchall():
                try:
                    props = json.loads(row[0])
                    if 'municipios' in props:
                        regiao_metropolitana = props.get('nome', '')
                        for municipio in props['municipios']:
                            municipio_id = str(municipio.get('id', ''))
                            if municipio_id:
                                mapeamento[municipio_id] = regiao_metropolitana
                except:
                    continue
            
            cursor.close()
            conn.close()
            
            print(f"Mapeamento criado com {len(mapeamento)} municípios")
            return mapeamento
            
        except Exception as e:
            print(f"Erro ao mapear regiões metropolitanas: {e}")
            return {}
    
    def buscar_informacoes_uf_regiao(self):
        """Busca informações de UF e região para preencher dados faltantes"""
        try:
            conn = self.get_connection()
            if not conn:
                return {}
                
            cursor = conn.cursor()
            
            # Buscar todas as informações de UF e região dos registros que contêm essas informações
            cursor.execute('''
                SELECT propriedades 
                FROM ibge_localidades 
                WHERE propriedades LIKE '%"UF"%' AND propriedades LIKE '%"regiao"%'
            ''')
            
            mapeamento_uf = {}
            for row in cursor.fetchall():
                try:
                    props = json.loads(row[0])
                    if 'UF' in props and 'id' in props['UF']:
                        uf_id = props['UF']['id']
                        uf_sigla = props['UF'].get('sigla', '')
                        uf_nome = props['UF'].get('nome', '')
                        
                        regiao_id = None
                        regiao_sigla = ''
                        regiao_nome = ''
                        
                        # Procurar região dentro da UF
                        if 'regiao' in props['UF']:
                            regiao = props['UF']['regiao']
                            regiao_id = regiao.get('id')
                            regiao_sigla = regiao.get('sigla', '')
                            regiao_nome = regiao.get('nome', '')
                        
                        # Usar o ID da UF como chave (convertendo para string para facilitar a busca)
                        if uf_id not in mapeamento_uf:
                            mapeamento_uf[str(uf_id)] = {
                                'uf_id': uf_id,
                                'uf_sigla': uf_sigla,
                                'uf_nome': uf_nome,
                                'regiao_id': regiao_id,
                                'regiao_sigla': regiao_sigla,
                                'regiao_nome': regiao_nome
                            }
                except:
                    continue
            
            cursor.close()
            conn.close()
            
            print(f"Mapeamento de UFs criado com {len(mapeamento_uf)} UFs")
            return mapeamento_uf
            
        except Exception as e:
            print(f"Erro ao mapear UFs: {e}")
            return {}
    
    def processar_dados(self):
        """Processa todos os dados da tabela ibge_localidades"""
        try:
            conn = self.get_connection()
            if not conn:
                return False
            
            # Primeiro, limpar a tabela de destino
            cursor = conn.cursor()
            cursor.execute("DELETE FROM ibge_localidades_tratado")
            conn.commit()
            
            # Criar mapeamento de regiões metropolitanas
            mapeamento_rm = self.mapear_regioes_metropolitanas()
            
            # Criar mapeamento de UFs e regiões
            mapeamento_uf = self.buscar_informacoes_uf_regiao()
            
            # Buscar todos os dados da tabela original
            cursor.execute('''
                SELECT codigo_ibge, nome, nivel_geografico, sigla, propriedades, data_extracao
                FROM ibge_localidades
                ORDER BY codigo_ibge
            ''')
            
            registros_processados = 0
            for row in cursor.fetchall():
                codigo_ibge, nome, nivel_geografico, sigla, propriedades, data_extracao = row
                
                # Processar propriedades JSON
                dados_processados = self.processar_propriedades(propriedades)
                
                if dados_processados:
                    # Verificar se é município e se pertence a uma região metropolitana
                    if dados_processados['municipio_id']:
                        municipio_id_str = str(dados_processados['municipio_id'])
                        if municipio_id_str in mapeamento_rm:
                            dados_processados['regiao_metropolitana'] = mapeamento_rm[municipio_id_str]
                    
                    # Preencher informações de UF e região se estiverem faltando
                    if not dados_processados['uf_id'] or not dados_processados['regiao_id']:
                        # Tentar extrair código da UF do código do município
                        codigo_municipio = str(dados_processados['codigo_ibge'])
                        if len(codigo_municipio) >= 2:
                            codigo_uf = codigo_municipio[:2]
                            # Procurar UF correspondente no mapeamento
                            for uf_key, info_uf in mapeamento_uf.items():
                                if str(info_uf['uf_id']) == codigo_uf:
                                    if not dados_processados['uf_id']:
                                        dados_processados['uf_id'] = info_uf['uf_id']
                                        dados_processados['uf_sigla'] = info_uf['uf_sigla']
                                        dados_processados['uf_nome'] = info_uf['uf_nome']
                                    if not dados_processados['regiao_id']:
                                        dados_processados['regiao_id'] = info_uf['regiao_id']
                                        dados_processados['regiao_sigla'] = info_uf['regiao_sigla']
                                        dados_processados['regiao_nome'] = info_uf['regiao_nome']
                                    break
                    
                    # Tratar data de extração
                    data_extracao_tratada = None
                    if dados_processados['data_extracao']:
                        try:
                            # Tentar converter a data
                            if isinstance(dados_processados['data_extracao'], str):
                                # Assumir formato ISO: "2025-08-22 10:22:16.203997"
                                data_str = dados_processados['data_extracao']
                                if '.' in data_str:
                                    # Remover microssegundos se existirem
                                    data_str = data_str.split('.')[0]
                                data_extracao_tratada = datetime.strptime(data_str, '%Y-%m-%d %H:%M:%S')
                            else:
                                data_extracao_tratada = dados_processados['data_extracao']
                        except:
                            # Se não conseguir converter, deixar como None
                            data_extracao_tratada = None
                    
                    # Inserir na nova tabela
                    insert_sql = '''
                    INSERT INTO ibge_localidades_tratado 
                    (codigo_ibge, nome, nivel_geografico, sigla, regiao_id, regiao_sigla, regiao_nome,
                     uf_id, uf_sigla, uf_nome, municipio_id, municipio_nome, regiao_metropolitana, data_extracao)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    '''
                    
                    cursor.execute(insert_sql, (
                        dados_processados['codigo_ibge'],
                        dados_processados['nome'],
                        dados_processados['nivel_geografico'],
                        dados_processados['sigla'],
                        dados_processados['regiao_id'],
                        dados_processados['regiao_sigla'],
                        dados_processados['regiao_nome'],
                        dados_processados['uf_id'],
                        dados_processados['uf_sigla'],
                        dados_processados['uf_nome'],
                        dados_processados['municipio_id'],
                        dados_processados['municipio_nome'],
                        dados_processados['regiao_metropolitana'],
                        data_extracao_tratada
                    ))
                    
                    registros_processados += 1
                    
                    if registros_processados % 100 == 0:
                        print(f"Processados {registros_processados} registros...")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"Processamento concluído! {registros_processados} registros processados.")
            return True
            
        except Exception as e:
            print(f"Erro ao processar dados: {e}")
            return False
    
    def executar_tratamento(self):
        """Executa todo o processo de tratamento"""
        print("=== INICIANDO TRATAMENTO DOS DADOS IBGE ===")
        
        # 1. Criar tabela de destino
        print("1. Criando tabela de destino...")
        if not self.criar_tabela_tratada():
            print("Erro ao criar tabela. Abortando...")
            return False
        
        # 2. Processar dados
        print("2. Processando dados...")
        if not self.processar_dados():
            print("Erro ao processar dados. Abortando...")
            return False
        
        print("=== TRATAMENTO CONCLUÍDO COM SUCESSO ===")
        return True
    
    def verificar_resultados(self):
        """Verifica os resultados do tratamento"""
        try:
            conn = self.get_connection()
            if not conn:
                return False
                
            cursor = conn.cursor()
            
            # Contar registros
            cursor.execute("SELECT COUNT(*) FROM ibge_localidades_tratado")
            total = cursor.fetchone()[0]
            
            # Contar por nível geográfico
            cursor.execute('''
                SELECT nivel_geografico, COUNT(*) as total
                FROM ibge_localidades_tratado
                GROUP BY nivel_geografico
                ORDER BY nivel_geografico
            ''')
            
            print(f"\n=== RESUMO DOS RESULTADOS ===")
            print(f"Total de registros processados: {total}")
            print("\nDistribuição por nível geográfico:")
            
            for row in cursor.fetchall():
                nivel, count = row
                print(f"  {nivel}: {count} registros")
            
            # Verificar regiões metropolitanas
            cursor.execute('''
                SELECT COUNT(*) as total
                FROM ibge_localidades_tratado
                WHERE regiao_metropolitana != '-'
            ''')
            
            rm_count = cursor.fetchone()[0]
            print(f"\nMunicípios em regiões metropolitanas: {rm_count}")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            print(f"Erro ao verificar resultados: {e}")

def main():
    """Função principal"""
    tratamento = TratamentoDadosIBGE()
    
    # Executar tratamento
    if tratamento.executar_tratamento():
        # Verificar resultados
        tratamento.verificar_resultados()
    else:
        print("Falha no tratamento dos dados.")

if __name__ == "__main__":
    main()
