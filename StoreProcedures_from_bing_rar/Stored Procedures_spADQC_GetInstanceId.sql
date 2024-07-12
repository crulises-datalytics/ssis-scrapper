CREATE PROCEDURE [dbo].[spADQC_GetInstanceId]
	@InstanceName NVARCHAR(100)
AS
	BEGIN
		/*	=====================================================================================================
		**	Procedure Name: [dbo].[spADQC_GetInstanceId]
		**	Description : This procedure is to get ID of Check Instance
		**	Created By: Suhas De
		**	Created Date: 19 Oct 2022
		**	Comments : 
		**		a. SET NOCOUNT ON added to prevent extra result sets from interfering with SELECT statements.
		**		b. This Stored Procedure is only called by ETL to read DQ Checks.
		**	=====================================================================================================
		**	Parameter Description:
		**	@InstanceName: To get ID from Instance Name
		**	=====================================================================================================
		**	Version		ModifiedDate		ModifiedBy			Description
		**	=====================================================================================================
		**	1.0			19 Oct 2022			Suhas De			Initial Version
		**	=====================================================================================================
		*/
		SET NOCOUNT ON;
		
		BEGIN TRY
			
			SELECT
				[InstanceId]
			FROM [dbo].[ADQCInstances]
			WHERE [InstanceName] = @InstanceName;

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