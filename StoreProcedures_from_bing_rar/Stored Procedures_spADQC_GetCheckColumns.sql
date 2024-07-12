CREATE PROCEDURE [dbo].[spADQC_GetCheckColumns]
	@ContextId BIGINT,
	@DQCheckId BIGINT
AS
	BEGIN
		/*	=====================================================================================================
		**	Procedure Name: [dbo].[spADQC_GetCheckColumns]
		**	Description : This procedure is to get column details of a DQ check
		**	Created By: Suhas De
		**	Created Date: 19 Oct 2022
		**	Comments : 
		**		a. SET NOCOUNT ON added to prevent extra result sets from interfering with SELECT statements.
		**		b. This Stored Procedure is only called by ETL to read DQ Check Columns.
		**	=====================================================================================================
		**	Parameter Description:
		**	@ContextId: To get columns for required ContextId
		**	@DQCheckId: To get columns for required CheckId
		**	=====================================================================================================
		**	Version		ModifiedDate		ModifiedBy			Description
		**	=====================================================================================================
		**	1.0			19 Oct 2022			Suhas De			Initial Version
		**	=====================================================================================================
		*/
		SET NOCOUNT ON;
		
		BEGIN TRY
			
			IF (@DQCheckId IS NOT NULL)
				BEGIN
					SELECT
						[CheckColumnId], UPPER([SourceColumnName]) AS [SourceColumnName], UPPER([TargetColumnName]) AS [TargetColumnName],
						CONVERT(INT, [IsPrimaryKey]) AS [IsPrimaryKey], [ToleranceLimit], CONVERT(INT, [IsTolerancePercent]) AS [IsTolerancePercent]
					FROM [dbo].[ADQCCheckColumnsReports]
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