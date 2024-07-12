CREATE PROCEDURE [dbo].[spADQC_GetNextCheck]
	@ContextId BIGINT
AS
	BEGIN
		/*	=====================================================================================================
		**	Procedure Name: [dbo].[spADQC_GetNextCheck]
		**	Description : This procedure is to get details of next scheduled check
		**	Created By: Suhas De
		**	Created Date: 19 Oct 2022
		**	Comments : 
		**		a. SET NOCOUNT ON added to prevent extra result sets from interfering with SELECT statements.
		**		b. This Stored Procedure is only called by ETL to read DQ Checks.
		**	=====================================================================================================
		**	Parameter Description:
		**	@ContextId: To get checks for required instance
		**	=====================================================================================================
		**	Version		ModifiedDate		ModifiedBy			Description
		**	=====================================================================================================
		**	1.0			19 Oct 2022			Suhas De			Initial Version
		**	=====================================================================================================
		*/
		SET NOCOUNT ON;
		DECLARE @DQCheckId INT = NULL;
		
		BEGIN TRY
			
			SELECT TOP 1
				@DQCheckId = [CheckId]
			FROM [dbo].[ADQCCheckReports]
			WHERE [CheckContextId] = @ContextId
			AND [StatusCodeId] = (SELECT [StatusCodeId] FROM [dbo].[ADQCStatusCodes] WHERE [StatusCodeName] = 'Pending')
			ORDER BY [CheckGroupId], [CheckId];

			IF (@DQCheckId IS NOT NULL)
				BEGIN
					UPDATE [dbo].[ADQCCheckReports]
						SET [StatusCodeId] = (SELECT [StatusCodeId] FROM [dbo].[ADQCStatusCodes] WHERE [StatusCodeName] = 'In Progress'),
							[CheckStartDateTime] = GETDATE()
					WHERE [CheckContextId] = @ContextId
					AND [CheckId] = @DQCheckId;
					
					SELECT
						[CheckId], [SourceTypeName], [SourceConnectionString], [SourceCommand],
						CONVERT(INT, [ShouldDeriveSourceCommand]) AS [ShouldDeriveSourceCommand],
						[SourceIdentifier] AS [SourceFlatFileDelimiter],
						[TargetTypeName], [TargetConnectionString], [TargetCommand],
						CONVERT(INT, [ShouldDeriveTargetCommand]) AS [ShouldDeriveTargetCommand],
						[TargetIdentifier] AS [TargetFlatFileDelimiter]
					FROM [dbo].[ADQCCheckReports]
					WHERE [CheckContextId] = @ContextId
					AND [CheckId] = @DQCheckId;
				END;
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
	END