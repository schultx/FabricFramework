-- Keystone metadata catalog
-- Deployed once (and re-run safely on every deploy) into SQL_METADATA_DATABASE by
-- setup/NB_DEPLOY.ipynb. Every statement is idempotent (guarded on existence) so
-- re-running this file never fails on an already-deployed database.
--
-- Batches are separated by a line containing only "GO" -- not executed as literal
-- SQL (pyodbc doesn't support the SSMS batch separator); NB_DEPLOY splits the file
-- on that marker and sends each batch as its own statement.

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ingestion') EXEC('CREATE SCHEMA ingestion')
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ai') EXEC('CREATE SCHEMA ai')
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'runtime') EXEC('CREATE SCHEMA runtime')
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'audit') EXEC('CREATE SCHEMA audit')
GO

-- ============================================================
-- ingestion -- a lean 3-level hierarchy: Connection -> Database -> Table.
-- Deliberately NOT one table per pipeline stage (no separate Landing/Bronze/
-- Silver entity tables) -- a single active ingestion.Table row drives a
-- table's entire Source -> Landing -> Bronze flow. Silver and Gold are
-- hand-written, per-table notebooks (%run-chained by NB_LOAD_SILVER /
-- NB_LOAD_GOLD), not metadata-loop-driven, so neither layer has a table of
-- its own in this schema.
-- ============================================================

IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'ingestion' AND t.name = 'Connection')
CREATE TABLE [ingestion].[Connection] (
    [ConnectionId]   INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [Name]           VARCHAR(200) NOT NULL,
    [ConnectionType] VARCHAR(10)  NOT NULL,              -- Sql | File
    [ConnectionGuid] UNIQUEIDENTIFIER NOT NULL,           -- the Fabric Connection item's GUID
    [IsActive]       BIT NOT NULL DEFAULT 1
)
GO

-- One row per database (Sql) or container/filesystem (File) within a Connection.
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'ingestion' AND t.name = 'Database')
CREATE TABLE [ingestion].[Database] (
    [DatabaseId]   INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [ConnectionId] INT NOT NULL REFERENCES [ingestion].[Connection]([ConnectionId]),
    [Name]         VARCHAR(200) NOT NULL,                 -- source database name (Sql) or container/filesystem name (File)
    [IsActive]     BIT NOT NULL DEFAULT 1
)
GO

-- One row per table drives its ENTIRE Source -> Landing -> Bronze flow when active.
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'ingestion' AND t.name = 'Table')
CREATE TABLE [ingestion].[Table] (
    [TableId]           BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [DatabaseId]        INT NOT NULL REFERENCES [ingestion].[Database]([DatabaseId]),
    [SourceSchema]      NVARCHAR(100) NULL,
    [SourceObject]      NVARCHAR(200) NOT NULL,          -- source table/query name
    [SourceQuery]       NVARCHAR(MAX) NULL,              -- optional custom SELECT override; NULL = SELECT *
    [FilePath]          NVARCHAR(500) NOT NULL,          -- target folder under Landing/Files/
    [FileType]          VARCHAR(20)   NOT NULL DEFAULT 'parquet',
    [BronzeSchema]      NVARCHAR(100) NOT NULL,
    [BronzeName]        NVARCHAR(200) NOT NULL,
    [PrimaryKeys]       NVARCHAR(200) NOT NULL,          -- comma-separated: dedupe key AND (LoadType='Delta') the MERGE key
    [LoadType]          VARCHAR(10)   NOT NULL DEFAULT 'Full',  -- Full | Delta
    [IncrementalColumn] NVARCHAR(100) NULL,              -- required when LoadType = 'Delta'; drives the watermark filter
    [DeleteHandling]    VARCHAR(20)   NOT NULL DEFAULT 'None',  -- None | SoftDelete | Reconcile
    [IsDeletedColumn]   NVARCHAR(100) NULL,              -- required when DeleteHandling = 'SoftDelete'
    [CleansingRules]    NVARCHAR(MAX) NULL,              -- JSON array: dedupe/null-handling/explode rules
    [IsActive]          BIT NOT NULL DEFAULT 1
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

-- Repointed at ingestion.Table (EntityType is always 'Table' now -- kept as a
-- column, not dropped, so a future watermark consumer outside ingestion.Table
-- doesn't force a schema change here). EntityId = ingestion.Table.TableId.
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'runtime' AND t.name = 'LoadWatermark')
CREATE TABLE [runtime].[LoadWatermark] (
    [WatermarkId]  BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [EntityType]   VARCHAR(20) NOT NULL,
    [EntityId]     BIGINT NOT NULL,
    [LastValue]    NVARCHAR(100) NULL,                    -- MAX(IncrementalColumn) seen as of LastRunUtc
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
