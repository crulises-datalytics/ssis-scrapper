CREATE PROCEDURE [dbo].[spADQC_GetStatus]
	@ContextId BIGINT
AS
	BEGIN
		/*	=====================================================================================================
		**	Procedure Name: [dbo].[spADQC_GetStatus]
		**	Description : This procedure is to get final status of all checks
		**	Created By: Suhas De
		**	Created Date: 19 Oct 2022
		**	Comments : 
		**		a. SET NOCOUNT ON added to prevent extra result sets from interfering with SELECT statements.
		**		b. This Stored Procedure is only called by ETL to update DQ Check status.
		**	=====================================================================================================
		**	Parameter Description:
		**	@ContextId: To get check results for required instance/context
		**	=====================================================================================================
		**	Version		ModifiedDate		ModifiedBy			Description
		**	=====================================================================================================
		**	1.0			19 Oct 2022			Suhas De			Initial Version
		**	=====================================================================================================
		*/
		SET NOCOUNT ON;
		
		BEGIN TRY
			
			SELECT
				[R].[StatusCodeId],
				[S].[StatusCodeName],
				COUNT([R].[CheckId]) AS [RecordCount]
			FROM [dbo].[ADQCCheckReports] [R]
			JOIN [dbo].[ADQCStatusCodes] [S]
				ON [R].[StatusCodeId] = [S].[StatusCodeId]
			WHERE [R].[CheckContextId] = @ContextId
			GROUP BY [R].[StatusCodeId], [S].[StatusCodeName];

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