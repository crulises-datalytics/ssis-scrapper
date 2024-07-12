CREATE PROCEDURE [dbo].[spADQC_GetPendingTasksCount]
	@ContextId BIGINT
AS
	BEGIN
		/*	=====================================================================================================
		**	Procedure Name: [dbo].[spADQC_GetPendingTasksCount]
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
		DECLARE @TasksCount INT = NULL;
		
		BEGIN TRY
			
			SELECT
				@TasksCount = COUNT(*)
			FROM [dbo].[ADQCCheckReports]
			WHERE [CheckContextId] = @ContextId
			AND [StatusCodeId] = (SELECT [StatusCodeId] FROM [dbo].[ADQCStatusCodes] WHERE [StatusCodeName] = 'Pending');

			SELECT @TasksCount AS [TasksCount];

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