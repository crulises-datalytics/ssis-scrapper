CREATE PROCEDURE [dbo].[spADQC_CreateCheckContext]
	@DQInstanceId INT
AS
	BEGIN
		/*	=====================================================================================================
		**	Procedure Name: [dbo].[spADQC_CreateCheckContext]
		**	Description : This procedure is to create entries in [dbo].[ADQCCheckReports], [dbo].[ADQCCheckColumnsReports]
		**	Created By: Suhas De
		**	Created Date: 19 Oct 2022
		**	Comments : 
		**		a. SET NOCOUNT ON added to prevent extra result sets from interfering with SELECT statements.
		**		b. This Stored Procedure is only called by ETL to load DQ Checks.
		**	=====================================================================================================
		**	Parameter Description:
		**	@DQInstanceId: To get checks for required instance
		**	=====================================================================================================
		**	Version		ModifiedDate		ModifiedBy			Description
		**	=====================================================================================================
		**	1.0			19 Oct 2022			Suhas De			Initial Version
		**	=====================================================================================================
		*/
		SET NOCOUNT ON;
		DECLARE @ContextId BIGINT = NULL,
				@IsAnOldContext BIT = 0;

		IF EXISTS (
			SELECT 1 FROM [dbo].[ADQCCheckReports]
			WHERE [InstanceId] = @DQInstanceId
			AND [CreatedDate] >= DATEADD(HOUR, -2, GETDATE())
			AND [StatusCodeId] IN (
				SELECT [StatusCodeId] FROM [dbo].[ADQCStatusCodes]
				WHERE [StatusCodeName] IN ('Failed', 'Validation Failed', 'In Progress', 'Pending')
			)
		)
			BEGIN
				SELECT
					@ContextId = MAX([CheckContextId]),
					@IsAnOldContext = 1
				FROM [dbo].[ADQCCheckReports]
				WHERE [InstanceId] = @DQInstanceId
				AND [CreatedDate] >= DATEADD(HOUR, -2, GETDATE())
				AND [StatusCodeId] IN (
					SELECT [StatusCodeId] FROM [dbo].[ADQCStatusCodes]
					WHERE [StatusCodeName] IN ('Failed', 'Validation Failed', 'In Progress', 'Pending')
				);
			END;
		ELSE
			SELECT @ContextId = CONVERT(BIGINT, REPLACE(REPLACE(REPLACE(REPLACE(CONVERT(VARCHAR(30), GETDATE(), 121), '-',''), ':', ''), '.', ''), ' ', '')) * 100 + CONVERT(BIGINT, RAND() * 100);

		BEGIN TRY
			IF (@IsAnOldContext = 1)
				BEGIN
					UPDATE [dbo].[ADQCCheckReports]
						SET [StatusCodeId] = (SELECT [StatusCodeId] FROM [dbo].[ADQCStatusCodes] WHERE [StatusCodeName] = 'Pending'),
							[CheckStartDateTime] = NULL,
							[CheckEndDateTime] = NULL,
							[StatusMessages] = NULL
					WHERE [InstanceId] = @DQInstanceId
					AND [CheckContextId] = @ContextId
					AND [StatusCodeId] IN (
						SELECT [StatusCodeId] FROM [dbo].[ADQCStatusCodes]
						WHERE [StatusCodeName] IN ('Failed', 'Validation Failed', 'Skipped', 'In Progress')
					);

					DELETE [C]
					FROM [dbo].[ADQCCheckColumnsReports] [C]
					JOIN [dbo].[ADQCCheckReports] [R]
						ON [C].[CheckId] = [R].[CheckId]
						AND [C].[CheckContextId] = [R].[CheckContextId]
					WHERE [R].[InstanceId] = @DQInstanceId
					AND [R].[CheckContextId] = @ContextId
					AND [R].[StatusCodeId] = ( SELECT [StatusCodeId] FROM [dbo].[ADQCStatusCodes] WHERE [StatusCodeName] IN ('Pending') );

					INSERT INTO [dbo].[ADQCCheckColumnsReports] (
						[CheckContextId], [CheckColumnId], [CheckId], [SourceColumnName], [TargetColumnName], [IsPrimaryKey], [ToleranceLimit], [IsTolerancePercent], [CreatedBy], [CreatedDate]
					)
					SELECT
						@ContextId, [C].[CheckColumnId], [C].[CheckId], [C].[SourceColumnName], [C].[TargetColumnName], [C].[IsPrimaryKey], [C].[ToleranceLimit], [C].[IsTolerancePercent],
						SUSER_SNAME(), GETDATE()
					FROM [dbo].[ADQCCheckColumns] [C]
					JOIN [dbo].[ADQCCheckReports] [R]
						ON [C].[CheckId] = [R].[CheckId]
						AND [R].[InstanceId] = @DQInstanceId
						AND [R].[CheckContextId] = @ContextId
						AND [C].[IsActive] = 1
						AND [R].[StatusCodeId] = ( SELECT [StatusCodeId] FROM [dbo].[ADQCStatusCodes] WHERE [StatusCodeName] IN ('Pending') );
				END
			ELSE
				BEGIN
					DROP TABLE IF EXISTS #AllChecks;

					SELECT
						[I].[InstanceId],
						[I].[InstanceName],
						[G].[CheckGroupId],
						[G].[CheckGroupName],
						[C].[CheckId],
						[C].[CheckName],
						[ST].[DataSourceTypeName] AS [SourceTypeName],
						[SC].[ConnectionString] AS [SourceConnectionString],
						[C].[SourceCommand],
						[C].[ShouldDeriveSourceCommand],
						[C].[SourceIdentifier],
						[TT].[DataSourceTypeName] AS [TargetTypeName],
						[TC].[ConnectionString] AS [TargetConnectionString],
						[C].[TargetCommand],
						[C].[ShouldDeriveTargetCommand],
						[C].[TargetIdentifier],
						[C].[IsFailFast]
					INTO #AllChecks
					FROM [dbo].[ADQCInstances] [I]
					JOIN [dbo].[ADQCCheckGroups] [G]
						ON [I].[InstanceId] = [G].[InstanceId]
						AND [G].[IsActive] = 1
					JOIN [dbo].[ADQCChecks] [C]
						ON [G].[CheckGroupId] = [C].[CheckGroupId]
						AND [C].[IsActive] = 1
					JOIN [dbo].[ADQCConnectionStrings] [SC]
						ON [C].[SourceConnectionStringId] = [SC].[ConnectionStringId]
					JOIN [dbo].[ADQCDataSourceTypes] [ST]
						ON [SC].[DataSourceTypeId] = [ST].[DataSourceTypeId]
					JOIN [dbo].[ADQCConnectionStrings] [TC]
						ON [C].[TargetConnectionStringId] = [TC].[ConnectionStringId]
					JOIN [dbo].[ADQCDataSourceTypes] [TT]
						ON [TC].[DataSourceTypeId] = [TT].[DataSourceTypeId]
					WHERE [I].[InstanceId] = @DQInstanceId

					INSERT INTO [dbo].[ADQCCheckReports] (
						[CheckContextId], [InstanceId], [InstanceName], [CheckGroupId], [CheckGroupName], [CheckId], [CheckName], [SourceTypeName], [SourceConnectionString], [SourceCommand], [ShouldDeriveSourceCommand], [SourceIdentifier],
						[TargetTypeName], [TargetConnectionString], [TargetCommand], [ShouldDeriveTargetCommand], [TargetIdentifier], [IsFailFast], [CreatedBy], [CreatedDate], [StatusCodeId]
					)
					SELECT
						@ContextId, [InstanceId], [InstanceName], [CheckGroupId], [CheckGroupName], [CheckId], [CheckName], [SourceTypeName], [SourceConnectionString], [SourceCommand], [ShouldDeriveSourceCommand], [SourceIdentifier],
						[TargetTypeName], [TargetConnectionString], [TargetCommand], [ShouldDeriveTargetCommand], [TargetIdentifier], [IsFailFast], SUSER_SNAME(), GETDATE(),
						(SELECT [StatusCodeId] FROM [dbo].[ADQCStatusCodes] WHERE [StatusCodeName] = 'Pending')
					FROM #AllChecks
					WHERE [InstanceId] = @DQInstanceId;

					INSERT INTO [dbo].[ADQCCheckColumnsReports] (
						[CheckContextId], [CheckColumnId], [CheckId], [SourceColumnName], [TargetColumnName], [IsPrimaryKey], [ToleranceLimit], [IsTolerancePercent], [CreatedBy], [CreatedDate]
					)
					SELECT
						@ContextId, [C].[CheckColumnId], [C].[CheckId], [C].[SourceColumnName], [C].[TargetColumnName], [C].[IsPrimaryKey], [C].[ToleranceLimit], [C].[IsTolerancePercent],
						SUSER_SNAME(), GETDATE()
					FROM [dbo].[ADQCCheckColumns] [C]
					JOIN #AllChecks [K]
						ON [C].[CheckId] = [K].[CheckId]
						AND [K].[InstanceId] = @DQInstanceId
						AND [C].[IsActive] = 1;
				END

			SELECT @ContextId AS [ExecutionContextId];
		END TRY
		BEGIN CATCH
			DECLARE @errorMessage NVARCHAR(MAX),
					@errorSeverity INT,
					@errorState INT,
					@errorNumber INT,
					@errorLine INT;

			SELECT 
				@errorMessage = ERROR_MESSAGE(),
				@errorSeverity = ERROR_SEVERITY(),
				@errorState = ERROR_STATE(),
				@errorNumber = ERROR_NUMBER(),
				@errorLine = ERROR_LINE();

			RAISERROR (@errorMessage, @errorSeverity, @errorState);

		END CATCH;

		DROP TABLE IF EXISTS #AllChecks;

	END