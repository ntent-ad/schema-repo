/*

You are recommended to back up your database before running this script

Script created by SQL Compare version 10.7.0 from Red Gate Software Ltd at 10/21/2014 3:57:22 PM

*/
SET NUMERIC_ROUNDABORT OFF
GO
SET ANSI_PADDING, ANSI_WARNINGS, CONCAT_NULL_YIELDS_NULL, ARITHABORT, QUOTED_IDENTIFIER, ANSI_NULLS ON
GO
IF EXISTS (SELECT * FROM tempdb..sysobjects WHERE id=OBJECT_ID('tempdb..#tmpErrors')) DROP TABLE #tmpErrors
GO
CREATE TABLE #tmpErrors (Error int)
GO
SET XACT_ABORT ON
GO
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE
GO
IF NOT EXISTS (SELECT * FROM master.dbo.syslogins WHERE loginname = N'avro')
CREATE LOGIN [avro] WITH PASSWORD = 'p@ssw0rd'
GO
CREATE USER [avro] FOR LOGIN [avro]
GO
PRINT N'Altering members of role db_datareader'
GO
EXEC sp_addrolemember N'db_datareader', N'avro'
GO
PRINT N'Altering members of role db_datawriter'
GO
EXEC sp_addrolemember N'db_datawriter', N'avro'
GO
BEGIN TRANSACTION
GO
PRINT N'Altering schemata'
GO
ALTER AUTHORIZATION ON SCHEMA::[db_datawriter]
TO [avro]
GO
ALTER AUTHORIZATION ON SCHEMA::[db_datareader]
TO [avro]
GO
PRINT N'Creating [dbo].[Schema]'
GO
CREATE TABLE [dbo].[Schema]
(
[Id] [int] NOT NULL IDENTITY(1, 1),
[Schema] [ntext] COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[Hash] [uniqueidentifier] NOT NULL
)
GO
IF @@ERROR<>0 AND @@TRANCOUNT>0 ROLLBACK TRANSACTION
GO
IF @@TRANCOUNT=0 BEGIN INSERT INTO #tmpErrors (Error) SELECT 1 BEGIN TRANSACTION END
GO
PRINT N'Creating primary key [PK_Schema] on [dbo].[Schema]'
GO
ALTER TABLE [dbo].[Schema] ADD CONSTRAINT [PK_Schema] PRIMARY KEY CLUSTERED  ([Id])
GO
IF @@ERROR<>0 AND @@TRANCOUNT>0 ROLLBACK TRANSACTION
GO
IF @@TRANCOUNT=0 BEGIN INSERT INTO #tmpErrors (Error) SELECT 1 BEGIN TRANSACTION END
GO
PRINT N'Creating index [IX_Schema] on [dbo].[Schema]'
GO
CREATE NONCLUSTERED INDEX [IX_Schema] ON [dbo].[Schema] ([Hash])
GO
IF @@ERROR<>0 AND @@TRANCOUNT>0 ROLLBACK TRANSACTION
GO
IF @@TRANCOUNT=0 BEGIN INSERT INTO #tmpErrors (Error) SELECT 1 BEGIN TRANSACTION END
GO
PRINT N'Creating [dbo].[TopicSchemaMap]'
GO
CREATE TABLE [dbo].[TopicSchemaMap]
(
[TopicId] [int] NOT NULL,
[SchemaId] [int] NOT NULL
)
GO
IF @@ERROR<>0 AND @@TRANCOUNT>0 ROLLBACK TRANSACTION
GO
IF @@TRANCOUNT=0 BEGIN INSERT INTO #tmpErrors (Error) SELECT 1 BEGIN TRANSACTION END
GO
PRINT N'Creating primary key [PK_TopicSchemaMap] on [dbo].[TopicSchemaMap]'
GO
ALTER TABLE [dbo].[TopicSchemaMap] ADD CONSTRAINT [PK_TopicSchemaMap] PRIMARY KEY CLUSTERED  ([TopicId], [SchemaId])
GO
IF @@ERROR<>0 AND @@TRANCOUNT>0 ROLLBACK TRANSACTION
GO
IF @@TRANCOUNT=0 BEGIN INSERT INTO #tmpErrors (Error) SELECT 1 BEGIN TRANSACTION END
GO
PRINT N'Creating [dbo].[Topic]'
GO
CREATE TABLE [dbo].[Topic]
(
[Id] [int] NOT NULL IDENTITY(1, 1),
[Topic] [varchar] (500) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[Configuration] [varchar] (MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
)
GO
IF @@ERROR<>0 AND @@TRANCOUNT>0 ROLLBACK TRANSACTION
GO
IF @@TRANCOUNT=0 BEGIN INSERT INTO #tmpErrors (Error) SELECT 1 BEGIN TRANSACTION END
GO
PRINT N'Creating primary key [PK_Topic] on [dbo].[Topic]'
GO
ALTER TABLE [dbo].[Topic] ADD CONSTRAINT [PK_Topic] PRIMARY KEY CLUSTERED  ([Id])
GO
IF @@ERROR<>0 AND @@TRANCOUNT>0 ROLLBACK TRANSACTION
GO
IF @@TRANCOUNT=0 BEGIN INSERT INTO #tmpErrors (Error) SELECT 1 BEGIN TRANSACTION END
GO
PRINT N'Creating index [IX_Topic] on [dbo].[Topic]'
GO
CREATE NONCLUSTERED INDEX [IX_Topic] ON [dbo].[Topic] ([Topic])
GO
IF @@ERROR<>0 AND @@TRANCOUNT>0 ROLLBACK TRANSACTION
GO
IF @@TRANCOUNT=0 BEGIN INSERT INTO #tmpErrors (Error) SELECT 1 BEGIN TRANSACTION END
GO
PRINT N'Adding foreign keys to [dbo].[TopicSchemaMap]'
GO
ALTER TABLE [dbo].[TopicSchemaMap] ADD CONSTRAINT [FK_TopicSchemaMap_Schema] FOREIGN KEY ([SchemaId]) REFERENCES [dbo].[Schema] ([Id])
ALTER TABLE [dbo].[TopicSchemaMap] ADD CONSTRAINT [FK_TopicSchemaMap_Topic] FOREIGN KEY ([TopicId]) REFERENCES [dbo].[Topic] ([Id])
GO
IF @@ERROR<>0 AND @@TRANCOUNT>0 ROLLBACK TRANSACTION
GO
IF @@TRANCOUNT=0 BEGIN INSERT INTO #tmpErrors (Error) SELECT 1 BEGIN TRANSACTION END
GO
IF EXISTS (SELECT * FROM #tmpErrors) ROLLBACK TRANSACTION
GO
IF @@TRANCOUNT>0 BEGIN
PRINT 'The database update succeeded'
COMMIT TRANSACTION
END
ELSE PRINT 'The database update failed'
GO
DROP TABLE #tmpErrors
GO
