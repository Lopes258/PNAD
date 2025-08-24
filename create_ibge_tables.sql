-- Script para criar as tabelas IBGE no SQL Server
-- Execute este script no seu banco de dados LOPES

USE [LOPES]
GO

-- Tabela para malhas geográficas
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ibge_malhas' AND xtype='U')
BEGIN
    CREATE TABLE [dbo].[ibge_malhas](
        [id] [int] IDENTITY(1,1) NOT NULL,
        [nome] [nvarchar](255) NULL,
        [codigo_ibge] [nvarchar](50) NULL,
        [nivel_geografico] [nvarchar](10) NULL,
        [geometria] [nvarchar](MAX) NULL,
        [propriedades] [nvarchar](MAX) NULL,
        [data_extracao] [datetime] NULL,
        [data_criacao] [datetime] NOT NULL DEFAULT (GETDATE()),
        CONSTRAINT [PK_ibge_malhas] PRIMARY KEY CLUSTERED ([id] ASC)
    )
    PRINT 'Tabela ibge_malhas criada com sucesso!'
END
ELSE
BEGIN
    PRINT 'Tabela ibge_malhas já existe.'
END
GO

-- Tabela para informações de localidades
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ibge_localidades' AND xtype='U')
BEGIN
    CREATE TABLE [dbo].[ibge_localidades](
        [id] [int] IDENTITY(1,1) NOT NULL,
        [nome] [nvarchar](255) NULL,
        [codigo_ibge] [nvarchar](50) NULL,
        [nivel_geografico] [nvarchar](10) NULL,
        [sigla] [nvarchar](10) NULL,
        [regiao] [nvarchar](255) NULL,
        [uf] [nvarchar](255) NULL,
        [municipio] [nvarchar](255) NULL,
        [propriedades] [nvarchar](MAX) NULL,
        [data_extracao] [datetime] NULL,
        [data_criacao] [datetime] NOT NULL DEFAULT (GETDATE()),
        CONSTRAINT [PK_ibge_localidades] PRIMARY KEY CLUSTERED ([id] ASC)
    )
    PRINT 'Tabela ibge_localidades criada com sucesso!'
END
ELSE
BEGIN
    PRINT 'Tabela ibge_localidades já existe.'
END
GO

-- Tabela para log de extrações
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ibge_log_extracao' AND xtype='U')
BEGIN
    CREATE TABLE [dbo].[ibge_log_extracao](
        [id] [int] IDENTITY(1,1) NOT NULL,
        [tipo_extracao] [nvarchar](50) NULL,
        [nivel_geografico] [nvarchar](10) NULL,
        [codigo_ibge] [nvarchar](50) NULL,
        [registros_extraidos] [int] NULL,
        [status] [nvarchar](50) NULL,
        [mensagem] [nvarchar](500) NULL,
        [data_extracao] [datetime] NULL,
        [data_criacao] [datetime] NOT NULL DEFAULT (GETDATE()),
        CONSTRAINT [PK_ibge_log_extracao] PRIMARY KEY CLUSTERED ([id] ASC)
    )
    PRINT 'Tabela ibge_log_extracao criada com sucesso!'
END
ELSE
BEGIN
    PRINT 'Tabela ibge_log_extracao já existe.'
END
GO

-- Índices para melhorar performance
-- Índice na tabela de malhas
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_ibge_malhas_codigo_ibge')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_ibge_malhas_codigo_ibge] ON [dbo].[ibge_malhas]
    ([codigo_ibge] ASC)
    PRINT 'Índice IX_ibge_malhas_codigo_ibge criado.'
END

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_ibge_malhas_nivel_geografico')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_ibge_malhas_nivel_geografico] ON [dbo].[ibge_malhas]
    ([nivel_geografico] ASC)
    PRINT 'Índice IX_ibge_malhas_nivel_geografico criado.'
END

-- Índices na tabela de localidades
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_ibge_localidades_codigo_ibge')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_ibge_localidades_codigo_ibge] ON [dbo].[ibge_localidades]
    ([codigo_ibge] ASC)
    PRINT 'Índice IX_ibge_localidades_codigo_ibge criado.'
END

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_ibge_localidades_nivel_geografico')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_ibge_localidades_nivel_geografico] ON [dbo].[ibge_localidades]
    ([nivel_geografico] ASC)
    PRINT 'Índice IX_ibge_localidades_nivel_geografico criado.'
END

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_ibge_localidades_sigla')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_ibge_localidades_sigla] ON [dbo].[ibge_localidades]
    ([sigla] ASC)
    PRINT 'Índice IX_ibge_localidades_sigla criado.'
END

-- Índices na tabela de log
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_ibge_log_tipo_extracao')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_ibge_log_tipo_extracao] ON [dbo].[ibge_log_extracao]
    ([tipo_extracao] ASC)
    PRINT 'Índice IX_ibge_log_tipo_extracao criado.'
END

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_ibge_log_data_extracao')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_ibge_log_data_extracao] ON [dbo].[ibge_log_extracao]
    ([data_extracao] ASC)
    PRINT 'Índice IX_ibge_log_data_extracao criado.'
END

PRINT ''
PRINT '=== CRIAÇÃO DAS TABELAS IBGE CONCLUÍDA ==='
PRINT 'Tabelas criadas:'
PRINT '  - ibge_malhas (Malhas geográficas)'
PRINT '  - ibge_localidades (Informações de localidades)'
PRINT '  - ibge_log_extracao (Log de extrações)'
PRINT ''
PRINT 'Agora você pode executar o script Python para extrair os dados:'
PRINT 'python api_IBGE.py'
GO
