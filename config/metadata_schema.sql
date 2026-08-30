-- Stratum metadata catalog
-- Deployed once (and re-run safely on every deploy) into SQL_STRATUM_CATALOG by
-- setup/NB_DEPLOY.ipynb. Every statement is idempotent (guarded on existence) so
-- re-running this file never fails on an already-deployed database.
--
-- Batches are separated by a line containing only "GO" -- not executed as literal
-- SQL (pyodbc doesn't support the SSMS batch separator); NB_DEPLOY splits the file
-- on that marker and sends each batch as its own statement.

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'catalog') EXEC('CREATE SCHEMA catalog')
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold') EXEC('CREATE SCHEMA gold')
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ai') EXEC('CREATE SCHEMA ai')
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'runtime') EXEC('CREATE SCHEMA runtime')
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'audit') EXEC('CREATE SCHEMA audit')
GO

-- ============================================================
-- catalog -- connections, sources, and one entity table per layer
-- ============================================================

IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'catalog' AND t.name = 'Connection')
CREATE TABLE [catalog].[Connection] (
    [ConnectionId]   INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [ConnectionGuid] UNIQUEIDENTIFIER NOT NULL,          -- the Fabric Connection item's GUID
    [Name]           VARCHAR(200) NOT NULL,
    [Type]           VARCHAR(50)  NOT NULL,              -- SQL | ADLS | SFTP | HTTP | ONELAKE
    [IsActive]       BIT NOT NULL DEFAULT 1
)
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'catalog' AND t.name = 'Source')
CREATE TABLE [catalog].[Source] (
    [SourceId]     INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [ConnectionId] INT NOT NULL REFERENCES [catalog].[Connection]([ConnectionId]),
    [Name]         VARCHAR(100) NOT NULL,
    [Namespace]    VARCHAR(100) NOT NULL,                -- prefix used for Bronze table names
    [IsActive]     BIT NOT NULL DEFAULT 1
)
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'catalog' AND t.name = 'LandingEntity')
CREATE TABLE [catalog].[LandingEntity] (
    [LandingEntityId]    BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [SourceId]           INT NOT NULL REFERENCES [catalog].[Source]([SourceId]),
    [SourceSchema]       NVARCHAR(100) NULL,
    [SourceObject]       NVARCHAR(200) NOT NULL,         -- source table/query name
    [SourceQuery]        NVARCHAR(MAX) NULL,             -- optional custom SELECT; NULL = SELECT *
    [FilePath]           NVARCHAR(500) NOT NULL,         -- target folder under Landing/Files/
    [FileType]           VARCHAR(20)   NOT NULL DEFAULT 'parquet',
    [IsIncremental]      BIT NOT NULL DEFAULT 0,
    [IncrementalColumn]  NVARCHAR(100) NULL,
    [IsActive]           BIT NOT NULL DEFAULT 1
)
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'catalog' AND t.name = 'BronzeEntity')
CREATE TABLE [catalog].[BronzeEntity] (
    [BronzeEntityId]   BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [LandingEntityId]  BIGINT NOT NULL REFERENCES [catalog].[LandingEntity]([LandingEntityId]),
    [Schema]           NVARCHAR(100) NOT NULL,
    [Name]             NVARCHAR(200) NOT NULL,
    [PrimaryKeys]      NVARCHAR(200) NOT NULL,           -- comma-separated
    [CleansingRules]   NVARCHAR(MAX) NULL,               -- JSON array: dedupe/null-handling/explode rules
    [IsActive]         BIT NOT NULL DEFAULT 1
)
GO

-- Populated only when an environment is deployed with include_silver = true.
-- A Bronze entity earns a Silver row specifically because its logic is reused by
-- two or more Gold objects -- most Bronze entities never get one.
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'catalog' AND t.name = 'SilverEntity')
CREATE TABLE [catalog].[SilverEntity] (
    [SilverEntityId]    BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [BronzeEntityId]    BIGINT NOT NULL REFERENCES [catalog].[BronzeEntity]([BronzeEntityId]),
    [Schema]            NVARCHAR(100) NOT NULL,
    [Name]              NVARCHAR(200) NOT NULL,
    [TransformNotebook] NVARCHAR(200) NULL,              -- custom notebook/function reference, if any
    [IsActive]          BIT NOT NULL DEFAULT 1
)
GO

-- ============================================================
-- gold -- one entity table, sourcing from Bronze by default, Silver only when configured
-- ============================================================

IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'gold' AND t.name = 'GoldEntity')
CREATE TABLE [gold].[GoldEntity] (
    [GoldEntityId]        BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [SourceLayer]         VARCHAR(10)   NOT NULL,        -- bronze | silver
    [SourceEntityId]      BIGINT        NOT NULL,        -- BronzeEntityId or SilverEntityId, per SourceLayer
    [EntityType]          VARCHAR(20)   NOT NULL,        -- dimension | fact | bridge
    [Schema]              NVARCHAR(100) NULL,
    [Name]                NVARCHAR(200) NOT NULL,
    [SurrogateKeyColumn]  NVARCHAR(100) NULL,
    [BusinessKeyColumns]  NVARCHAR(500) NULL,
    [LoadPattern]         VARCHAR(20)   NOT NULL,        -- scd1 | scd2 | overwrite | append
    [TransformTemplate]   NVARCHAR(100) NULL,            -- which templated notebook applies
    [CustomTransformHook] NVARCHAR(200) NULL,            -- escape hatch for genuine one-offs
    [IsActive]            BIT NOT NULL DEFAULT 1
)
GO

-- ============================================================
-- ai -- feature/vector registration for AI-team consumption
-- ============================================================

IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'ai' AND t.name = 'FeatureSet')
CREATE TABLE [ai].[FeatureSet] (
    [FeatureSetId]     INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [SourceEntityId]   BIGINT NOT NULL,                  -- Silver or Gold entity
    [SourceLayer]      VARCHAR(20) NOT NULL,              -- silver | gold
    [Name]             NVARCHAR(150) NOT NULL,
    [RefreshCadence]   VARCHAR(20) NULL,
    [VectorIndexRef]   NVARCHAR(200) NULL,                -- embedding index, if any
    [IsActive]         BIT NOT NULL DEFAULT 1
)
GO

-- ============================================================
-- runtime -- incremental-load state
-- ============================================================

IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'runtime' AND t.name = 'LoadWatermark')
CREATE TABLE [runtime].[LoadWatermark] (
    [WatermarkId]  BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [EntityType]   VARCHAR(20) NOT NULL,                  -- landing | bronze | silver | gold
    [EntityId]     BIGINT NOT NULL,
    [LastValue]    NVARCHAR(100) NULL,
    [LastRunUtc]   DATETIME2(7) NULL
)
GO

-- ============================================================
-- audit -- execution history
-- ============================================================

IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'audit' AND t.name = 'PipelineRun')
CREATE TABLE [audit].[PipelineRun] (
    [RunId]         BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [PipelineName]  NVARCHAR(200) NOT NULL,
    [RunGuid]       UNIQUEIDENTIFIER NOT NULL,
    [Status]        NVARCHAR(50) NOT NULL,
    [StartTimeUtc]  DATETIME2(7) NOT NULL,
    [EndTimeUtc]    DATETIME2(7) NULL,
    [ErrorMessage]  NVARCHAR(MAX) NULL,
    [RowsRead]      BIGINT NULL,
    [RowsWritten]   BIGINT NULL
)
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'audit' AND t.name = 'NotebookRun')
CREATE TABLE [audit].[NotebookRun] (
    [RunId]         BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [NotebookName]  NVARCHAR(200) NOT NULL,
    [RunGuid]       UNIQUEIDENTIFIER NOT NULL,
    [Status]        NVARCHAR(50) NOT NULL,
    [StartTimeUtc]  DATETIME2(7) NOT NULL,
    [EndTimeUtc]    DATETIME2(7) NULL,
    [ErrorMessage]  NVARCHAR(MAX) NULL
)
GO
