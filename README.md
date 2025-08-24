# 📊 Projetos de Extração de Dados - IBGE e PNAD

Este repositório contém dois projetos principais para extração e armazenamento de dados do IBGE:

## 🗂️ Projetos Disponíveis

### 1. 📊 [Projeto PNAD](README_PNAD.md) - Dados Sócio-Econômicos
- **Arquivo**: `api_PNAD.py`
- **Descrição**: Extrai dados da Pesquisa Nacional por Amostra de Domicílios (PNAD)
- **Dados**: Ocupação, rendimentos, domicílios por região metropolitana
- **API**: SIDRA (Sistema IBGE de Recuperação Automática)

### 2. 🗺️ [Projeto IBGE](README_IBGE.md) - Dados Geográficos
- **Arquivo**: `api_IBGE.py`
- **Descrição**: Extrai malhas geográficas e informações de localidades
- **Dados**: Malhas (Brasil, Estados, Municípios), localidades, coordenadas
- **API**: APIs de Malhas e Localidades do IBGE

## 🚀 Início Rápido

### Pré-requisitos
- Python 3.7+
- SQL Server
- Conexão com internet

### Instalação
```bash
# 1. Clone o repositório
git clone <seu-repositorio>

# 2. Instale as dependências
pip install -r requirements.txt

# 3. Configure o banco de dados
# Edite config.env com suas configurações

# 4. Teste a conexão
python test_connection.py
```

### Execução dos Projetos

#### PNAD - Dados Sócio-Econômicos
```bash
# Teste primeiro
python test_pnad.py

# Execute a extração completa
python api_PNAD.py
```

#### IBGE - Dados Geográficos
```bash
# Teste primeiro
python test_ibge.py

# Execute a extração completa
python api_IBGE.py
```

## 🗄️ Estrutura do Banco de Dados

### Tabelas PNAD
- `pnad_ocupacao` - Força de trabalho
- `pnad_rendimento` - Rendimentos
- `pnad_domicilios` - Domicílios
- `pnad_log_extracao` - Log de extrações

### Tabelas IBGE
- `ibge_malhas` - Malhas geográficas
- `ibge_localidades` - Informações de localidades
- `ibge_log_extracao` - Log de extrações

## 📋 Scripts Disponíveis

| Arquivo | Descrição |
|---------|-----------|
| `api_PNAD.py` | API principal para dados PNAD |
| `api_IBGE.py` | API principal para dados geográficos |
| `database.py` | Configuração de conexão com banco |
| `test_pnad.py` | Testes para API PNAD |
| `test_ibge.py` | Testes para API IBGE |
| `test_connection.py` | Teste de conexão com banco |
| `create_pnad_tables.sql` | Script SQL para tabelas PNAD |
| `create_ibge_tables.sql` | Script SQL para tabelas IBGE |

## 🔧 Configuração

### Arquivo config.env
```env
# Configurações do SQL Server
DB_SERVER=localhost
DB_PORT=1433
DB_DATABASE=LOPES
DB_TRUSTED_CONNECTION=True

# Configurações do Servidor
PORT=8000
NODE_ENV=development
```

## 📊 Exemplos de Uso

### Consulta PNAD
```sql
-- Dados de ocupação por região
SELECT localidade, variavel, valor, periodo
FROM pnad_ocupacao 
WHERE periodo = '2023'
ORDER BY localidade, variavel;
```

### Consulta IBGE
```sql
-- Estados e suas siglas
SELECT nome, sigla, regiao
FROM ibge_localidades 
WHERE nivel_geografico = 'N2'
ORDER BY nome;
```

## 🧪 Testes

### Teste de Conexão
```bash
python test_connection.py
```

### Teste PNAD
```bash
python test_pnad.py
```

### Teste IBGE
```bash
python test_ibge.py
```

## 📝 Logs

Ambos os projetos incluem:
- Logging detalhado de todas as operações
- Timestamps para auditoria
- Tratamento de erros com retry automático
- Logs de banco de dados para rastreamento

## 🚨 Tratamento de Erros

- **Retry automático** com backoff exponencial
- **Timeout configurável** para APIs
- **Rollback automático** em caso de falha
- **Logs detalhados** para debugging

## 🔍 Monitoramento

### Logs de Extração
```sql
-- PNAD
SELECT * FROM pnad_log_extracao ORDER BY data_extracao DESC;

-- IBGE
SELECT * FROM ibge_log_extracao ORDER BY data_extracao DESC;
```

### Estatísticas de Dados
```sql
-- Contagem por tabela
SELECT 
    'PNAD Ocupação' as tabela, COUNT(*) as registros 
FROM pnad_ocupacao
UNION ALL
SELECT 
    'IBGE Localidades' as tabela, COUNT(*) as registros 
FROM ibge_localidades;
```

## 📞 Suporte

Para problemas ou dúvidas:
1. Verifique os logs de erro
2. Execute os scripts de teste
3. Confirme as configurações do banco
4. Verifique a conectividade com as APIs

## 📄 Licença

Este projeto é para uso educacional e de pesquisa. Respeite os termos de uso das APIs do IBGE.

## 🔗 Links Úteis

- [Documentação PNAD](README_PNAD.md)
- [Documentação IBGE](README_IBGE.md)
- [API SIDRA](https://servicodados.ibge.gov.br/docs/sidra/)
- [API Malhas IBGE](https://servicodados.ibge.gov.br/docs/malhas/)
- [API Localidades IBGE](https://servicodados.ibge.gov.br/docs/localidades/) 
