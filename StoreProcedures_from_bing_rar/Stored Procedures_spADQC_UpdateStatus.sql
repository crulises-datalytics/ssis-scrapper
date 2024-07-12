CREATE PROCEDURE [dbo].[spADQC_UpdateStatus]
	@ContextId BIGINT,
	@DQCheckId BIGINT,
	@DQStatusCodeName NVARCHAR(50),
	@Message NVARCHAR(MAX) = NULL
AS
	BEGIN
		/*	=====================================================================================================
		**	Procedure Name: [dbo].[spADQC_UpdateStatus]
		**	Description : This procedure is to update status of an in-progress check
		**	Created By: Suhas De
		**	Created Date: 19 Oct 2022
		**	Comments : 
		**		a. SET NOCOUNT ON added to prevent extra result sets from interfering with SELECT statements.
		**		b. This Stored Procedure is only called by ETL to update DQ Check status.
		**	=====================================================================================================
		**	Parameter Description:
		**	@ContextId: To update check for required instance/context
		**  @DQCheckId: To update check
		**  @DQStatusCodeName: Status Code sent by Package
		**	@Message: Any message that needs to be logged
		**	=====================================================================================================
		**	Version		ModifiedDate		ModifiedBy			Description
		**	=====================================================================================================
		**	1.0			19 Oct 2022			Suhas De			Initial Version
		**	=====================================================================================================
		*/
		SET NOCOUNT ON;
		
		BEGIN TRY
			SELECT @Message = NULLIF(@Message, 'NULL');
			SELECT @DQStatusCodeName = NULLIF(@DQStatusCodeName, 'NULL');
			
			UPDATE [dbo].[ADQCCheckReports]
				SET [StatusCodeId] =
						CASE
							WHEN @DQStatusCodeName IS NULL THEN [StatusCodeId]
							ELSE (SELECT [StatusCodeId] FROM [dbo].[ADQCStatusCodes] WHERE [StatusCodeName] = @DQStatusCodeName)
						END,
					[CheckEndDatetime] = 
						CASE
							WHEN ISNULL(@DQStatusCodeName, '@DQStatusCodeName') IN ('Failed', 'Validation Succeeded', 'Validation Failed', 'Skipped') THEN GETDATE()
							ELSE NULL
						END,
					[StatusMessages] = 
						CASE	
							WHEN [StatusMessages] IS NULL AND @Message IS NULL THEN NULL
							WHEN [StatusMessages] IS NULL AND @Message IS NOT NULL THEN @Message
							WHEN [StatusMessages] IS NOT NULL AND @Message IS NULL THEN [StatusMessages]
							ELSE [StatusMessages] + '; ' + @Message
						END
			WHERE [CheckContextId] = @ContextId
			AND [CheckId] = @DQCheckId;

			IF ((SELECT [IsFailFast] FROM [dbo].[ADQCCheckReports] WHERE [CheckContextId] = @ContextId AND [CheckId] = @DQCheckId) = 1)
				BEGIN
					UPDATE [dbo].[ADQCCheckReports]
						SET [StatusCodeId] = (SELECT [StatusCodeId] FROM [dbo].[ADQCStatusCodes] WHERE [StatusCodeName] = 'Skipped')
					WHERE [CheckContextId] = @ContextId
					AND [CheckGroupId] = (
						SELECT
							[CheckGroupId]
						FROM [dbo].[ADQCCheckReports]
						WHERE [CheckContextId] = @ContextId
						AND [CheckId] = @DQCheckId
					)
					AND [StatusCodeId] = (SELECT [StatusCodeId] FROM [dbo].[ADQCStatusCodes] WHERE [StatusCodeName] = 'Pending')
				END

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