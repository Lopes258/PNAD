-- Script para criar as tabelas PNAD no SQL Server
-- Execute este script no seu banco de dados LOPES

USE [LOPES]
GO

-- Tabela para dados de ocupação (Força de trabalho)
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='pnad_ocupacao' AND xtype='U')
BEGIN
    CREATE TABLE [dbo].[pnad_ocupacao](
        [id] [int] IDENTITY(1,1) NOT NULL,
        [localidade] [nvarchar](255) NULL,
        [variavel] [nvarchar](255) NULL,
        [valor] [nvarchar](255) NULL,
        [periodo] [nvarchar](255) NULL,
        [categoria] [nvarchar](255) NULL,
        [tabela_id] [nvarchar](50) NULL,
        [data_extracao] [datetime] NULL,
        [data_criacao] [datetime] NOT NULL DEFAULT (GETDATE()),
        CONSTRAINT [PK_pnad_ocupacao] PRIMARY KEY CLUSTERED ([id] ASC)
    )
    PRINT 'Tabela pnad_ocupacao criada com sucesso!'
END
ELSE
BEGIN
    PRINT 'Tabela pnad_ocupacao já existe.'
END
GO

-- Tabela para dados de rendimento
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='pnad_rendimento' AND xtype='U')
BEGIN
    CREATE TABLE [dbo].[pnad_rendimento](
        [id] [int] IDENTITY(1,1) NOT NULL,
        [localidade] [nvarchar](255) NULL,
        [variavel] [nvarchar](255) NULL,
        [valor] [nvarchar](255) NULL,
        [periodo] [nvarchar](255) NULL,
        [categoria] [nvarchar](255) NULL,
        [tabela_id] [nvarchar](50) NULL,
        [data_extracao] [datetime] NULL,
        [data_criacao] [datetime] NOT NULL DEFAULT (GETDATE()),
        CONSTRAINT [PK_pnad_rendimento] PRIMARY KEY CLUSTERED ([id] ASC)
    )
    PRINT 'Tabela pnad_rendimento criada com sucesso!'
END
ELSE
BEGIN
    PRINT 'Tabela pnad_rendimento já existe.'
END
GO

-- Tabela para dados de domicílios
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='pnad_domicilios' AND xtype='U')
BEGIN
    CREATE TABLE [dbo].[pnad_domicilios](
        [id] [int] IDENTITY(1,1) NOT NULL,
        [localidade] [nvarchar](255) NULL,
        [variavel] [nvarchar](255) NULL,
        [valor] [nvarchar](255) NULL,
        [periodo] [nvarchar](255) NULL,
        [categoria] [nvarchar](255) NULL,
        [tabela_id] [nvarchar](50) NULL,
        [data_extracao] [datetime] NULL,
        [data_criacao] [datetime] NOT NULL DEFAULT (GETDATE()),
        CONSTRAINT [PK_pnad_domicilios] PRIMARY KEY CLUSTERED ([id] ASC)
    )
    PRINT 'Tabela pnad_domicilios criada com sucesso!'
END
ELSE
BEGIN
    PRINT 'Tabela pnad_domicilios já existe.'
END
GO

-- Tabela para log de extrações
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='pnad_log_extracao' AND xtype='U')
BEGIN
    CREATE TABLE [dbo].[pnad_log_extracao](
        [id] [int] IDENTITY(1,1) NOT NULL,
        [tabela_id] [nvarchar](50) NULL,
        [data_extracao] [datetime] NULL,
        [registros_extraidos] [int] NULL,
        [status] [nvarchar](50) NULL,
        [mensagem] [nvarchar](500) NULL,
        [data_criacao] [datetime] NOT NULL DEFAULT (GETDATE()),
        CONSTRAINT [PK_pnad_log_extracao] PRIMARY KEY CLUSTERED ([id] ASC)
    )
    PRINT 'Tabela pnad_log_extracao criada com sucesso!'
END
ELSE
BEGIN
    PRINT 'Tabela pnad_log_extracao já existe.'
END
GO

-- Índices para melhorar performance
-- Índice na tabela de ocupação
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_pnad_ocupacao_localidade')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_pnad_ocupacao_localidade] ON [dbo].[pnad_ocupacao]
    ([localidade] ASC)
    PRINT 'Índice IX_pnad_ocupacao_localidade criado.'
END

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_pnad_ocupacao_periodo')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_pnad_ocupacao_periodo] ON [dbo].[pnad_ocupacao]
    ([periodo] ASC)
    PRINT 'Índice IX_pnad_ocupacao_periodo criado.'
END

-- Índice na tabela de rendimento
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_pnad_rendimento_localidade')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_pnad_rendimento_localidade] ON [dbo].[pnad_rendimento]
    ([localidade] ASC)
    PRINT 'Índice IX_pnad_rendimento_localidade criado.'
END

-- Índice na tabela de domicílios
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_pnad_domicilios_localidade')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_pnad_domicilios_localidade] ON [dbo].[pnad_domicilios]
    ([localidade] ASC)
    PRINT 'Índice IX_pnad_domicilios_localidade criado.'
END

-- Índice na tabela de log
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_pnad_log_tabela_id')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_pnad_log_tabela_id] ON [dbo].[pnad_log_extracao]
    ([tabela_id] ASC)
    PRINT 'Índice IX_pnad_log_tabela_id criado.'
END

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_pnad_log_data_extracao')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_pnad_log_data_extracao] ON [dbo].[pnad_log_extracao]
    ([data_extracao] ASC)
    PRINT 'Índice IX_pnad_log_data_extracao criado.'
END

PRINT ''
PRINT '=== CRIAÇÃO DAS TABELAS PNAD CONCLUÍDA ==='
PRINT 'Tabelas criadas:'
PRINT '  - pnad_ocupacao (Força de trabalho)'
PRINT '  - pnad_rendimento (Rendimentos)'
PRINT '  - pnad_domicilios (Domicílios)'
PRINT '  - pnad_log_extracao (Log de extrações)'
PRINT ''
PRINT 'Agora você pode executar o script Python para extrair os dados:'
PRINT 'python api_PNAD.py'
GO
