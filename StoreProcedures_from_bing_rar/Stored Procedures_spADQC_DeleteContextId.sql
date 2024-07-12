CREATE   PROCEDURE [dbo].[spADQC_DeleteContextId]
	@ContextId BIGINT
AS
	BEGIN
		/*	=====================================================================================================
		**	Procedure Name: [dbo].[spADQC_DeleteContextId]
		**	Description : This procedure is to delete a Check Context ID
		**	Created By: Suhas De
		**	Created Date: 19 Oct 2022
		**	Comments : 
		**		a. SET NOCOUNT ON added to prevent extra result sets from interfering with SELECT statements.
		**		b. This Stored Procedure is not called by ETL.
		**	=====================================================================================================
		**	Parameter Description:
		**	@ContextId: To get required instance/context
		**	=====================================================================================================
		**	Version		ModifiedDate		ModifiedBy			Description
		**	=====================================================================================================
		**	1.0			19 Oct 2022			Suhas De			Initial Version
		**	=====================================================================================================
		*/
		SET NOCOUNT ON;
		
		BEGIN TRY
			
			DELETE [dbo].[ADQCCheckColumnsReports] WHERE [CheckContextId] = @ContextId;
			DELETE [dbo].[ADQCCheckReports] WHERE [CheckContextId] = @ContextId;

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