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

-- ConnectionType values and what ConnectionGuid/ingestion.Database.Name mean for each:
--   Sql            -- ConnectionGuid = Fabric Connection GUID (Azure SQL). Database.Name = source database name.
--   File           -- ConnectionGuid = Fabric Connection GUID (ADLS Gen2). Database.Name = container/filesystem name.
--   SqlMI          -- ConnectionGuid = Fabric Connection GUID (Azure SQL Managed Instance). Database.Name = source database name.
--   Oracle         -- ConnectionGuid = Fabric Connection GUID (Oracle, routed through an on-premises Data
--                     Gateway). Database.Name = source database/service name. The gateway itself is configured
--                     on the Fabric Connection object, not tracked here -- no extra column needed for it.
--   Sftp           -- ConnectionGuid = Fabric Connection GUID (SFTP). Database.Name = unused (NULL-ish; use '').
--   Ftp            -- ConnectionGuid = Fabric Connection GUID (FTP). Database.Name = unused (use '').
--   OneLakeTable   -- No Fabric Connection object exists for same-tenant cross-workspace OneLake access (Fabric
--                     addresses it directly by workspace + item GUID, the same mechanism this framework's own
--                     Landing/Bronze/Gold lakehouse references already use) -- so these two types repurpose the
--                     columns instead of adding new ones: ConnectionGuid = the SOURCE WORKSPACE GUID, and
--                     Database.Name = the SOURCE LAKEHOUSE's item GUID (as text, not a display name).
--   OneLakeFile    -- Same repurposing as OneLakeTable (source workspace GUID / source lakehouse GUID).
--   Custom         -- Escape hatch for sources with no dedicated connector (REST APIs, SharePoint, Dataverse,
--                     Salesforce, etc.) -- delegates to a hand-written per-table notebook instead of a generic
--                     Copy activity (see ingestion.Table.CustomNotebookName below). ConnectionGuid/Database.Name
--                     are not used by any pipeline for this type; set them to placeholder values.
-- FMD Framework's "ADF" type (pass-through metadata tracking for an externally-orchestrated ADF pipeline) was
-- deliberately NOT ported -- it isn't a real data connector, and doesn't fit this framework's self-contained
-- model where every ingestion runs from inside Keystone's own pipelines.
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'ingestion' AND t.name = 'Connection')
CREATE TABLE [ingestion].[Connection] (
    [ConnectionId]   INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [Name]           VARCHAR(200) NOT NULL,
    [ConnectionType] VARCHAR(20)  NOT NULL,              -- Sql | File | SqlMI | Oracle | Sftp | Ftp | OneLakeTable | OneLakeFile | Custom
    [ConnectionGuid] UNIQUEIDENTIFIER NOT NULL,           -- the Fabric Connection item's GUID (see ConnectionType notes above for exceptions)
    [IsActive]       BIT NOT NULL DEFAULT 1
)
GO

-- One row per database/container/lakehouse within a Connection -- exact meaning of Name depends on
-- ConnectionType; see the comment above ingestion.Connection.
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'ingestion' AND t.name = 'Database')
CREATE TABLE [ingestion].[Database] (
    [DatabaseId]   INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [ConnectionId] INT NOT NULL REFERENCES [ingestion].[Connection]([ConnectionId]),
    [Name]         VARCHAR(200) NOT NULL,                 -- source database name (Sql/SqlMI/Oracle), container/filesystem name (File), or source lakehouse GUID (OneLakeTable/OneLakeFile)
    [IsActive]     BIT NOT NULL DEFAULT 1
)
GO

-- One row per table drives its ENTIRE Source -> Landing -> Bronze flow when active.
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'ingestion' AND t.name = 'Table')
CREATE TABLE [ingestion].[Table] (
    [TableId]            BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [DatabaseId]         INT NOT NULL REFERENCES [ingestion].[Database]([DatabaseId]),
    [SourceSchema]       NVARCHAR(100) NULL,
    [SourceObject]       NVARCHAR(200) NOT NULL,          -- source table/query name
    [SourceQuery]        NVARCHAR(MAX) NULL,              -- optional custom SELECT override; NULL = SELECT *
    [FilePath]           NVARCHAR(500) NOT NULL,          -- target folder under Landing/Files/
    [FileType]           VARCHAR(20)   NOT NULL DEFAULT 'parquet',
    [BronzeSchema]       NVARCHAR(100) NOT NULL,
    [BronzeName]         NVARCHAR(200) NOT NULL,
    [PrimaryKeys]        NVARCHAR(200) NOT NULL,          -- comma-separated: dedupe key AND (LoadType='Delta') the MERGE key
    [LoadType]           VARCHAR(10)   NOT NULL DEFAULT 'Full',  -- Full | Delta
    [IncrementalColumn]  NVARCHAR(100) NULL,              -- required when LoadType = 'Delta'; drives the watermark filter
    [DeleteHandling]     VARCHAR(20)   NOT NULL DEFAULT 'None',  -- None | SoftDelete | Reconcile
    [IsDeletedColumn]    NVARCHAR(100) NULL,              -- required when DeleteHandling = 'SoftDelete'
    [CleansingRules]     NVARCHAR(MAX) NULL,              -- JSON array: dedupe/null-handling/explode rules
    [CustomNotebookName] NVARCHAR(200) NULL,              -- populated only when the row's Connection.ConnectionType = 'Custom' -- the hand-written notebook (config/items.yaml) that lands this table; see DEPLOYMENT.md's "Custom sources" section
    [IsActive]           BIT NOT NULL DEFAULT 1
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

-- ============================================================
-- ingestion lookup view -- resolves each active Table row's ready-to-run
-- SourceQuery in T-SQL (Full = SELECT * or the row's own override; Delta =
-- the same, watermark-bounded) so a native pipeline Lookup activity can
-- drive each PL_INGEST_* pipeline directly -- no notebook in the loop, no
-- pipeline needing its own connection to the metadata catalog beyond the
-- one bootstrap Connection (see config/environments.yaml's
-- metadata_connection_guid). Every PL_INGEST_* pipeline's Lookup queries
-- this same view, filtered to its own ConnectionType.
--
-- Excludes ConnectionType = 'Custom' rows -- those are the hand-written
-- notebook escape hatch (see ingestion.Table.CustomNotebookName above and
-- DEPLOYMENT.md's "Custom sources" section), not meant to be picked up by
-- any generic Lookup-driven PL_INGEST_* pipeline.
--
-- CREATE VIEW must be the only statement in its batch, so this uses
-- drop-then-recreate instead of the IF NOT EXISTS-guarded CREATE used for
-- tables above -- a view has no data to preserve, so re-running this is
-- exactly as idempotent as everything else in this file.
-- ============================================================

IF OBJECT_ID('[ingestion].[vw_ActiveIngestTables]', 'V') IS NOT NULL DROP VIEW [ingestion].[vw_ActiveIngestTables]
GO

CREATE VIEW [ingestion].[vw_ActiveIngestTables] AS
SELECT
    t.[TableId], t.[SourceSchema], t.[SourceObject], t.[FilePath], t.[FileType],
    t.[LoadType], t.[IncrementalColumn],
    d.[Name] AS DatabaseName, c.[ConnectionGuid], c.[ConnectionType],
    CASE
        WHEN t.[LoadType] = 'Delta' THEN
            'SELECT * FROM (' + base.[BaseQuery] + ') AS w WHERE ' + t.[IncrementalColumn]
                + ' > ''' + ISNULL(CONVERT(NVARCHAR(100), w.[LastValue], 120), '1900-01-01') + ''''
        ELSE base.[BaseQuery]
    END AS ResolvedSourceQuery
FROM [ingestion].[Table] t
JOIN [ingestion].[Database] d ON t.[DatabaseId] = d.[DatabaseId]
JOIN [ingestion].[Connection] c ON d.[ConnectionId] = c.[ConnectionId]
CROSS APPLY (
    SELECT CASE
        WHEN t.[SourceQuery] IS NOT NULL THEN 'SELECT * FROM (' + t.[SourceQuery] + ') AS src_query'
        ELSE 'SELECT * FROM ' + ISNULL(t.[SourceSchema] + '.', '') + t.[SourceObject]
    END AS BaseQuery
) base
LEFT JOIN [runtime].[LoadWatermark] w ON w.[EntityType] = 'Table' AND w.[EntityId] = t.[TableId]
WHERE t.[IsActive] = 1 AND d.[IsActive] = 1 AND c.[IsActive] = 1 AND c.[ConnectionType] <> 'Custom'
GO
