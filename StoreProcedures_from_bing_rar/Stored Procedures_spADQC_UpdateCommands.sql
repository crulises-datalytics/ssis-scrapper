CREATE PROCEDURE [dbo].[spADQC_UpdateCommands]
	@ContextId BIGINT,
	@DQCheckId BIGINT,
	@SourceCommand NVARCHAR(MAX),
	@TargetCommand NVARCHAR(MAX)
AS
	BEGIN
		/*	=====================================================================================================
		**	Procedure Name: [dbo].[spADQC_UpdateCommands]
		**	Description : This procedure is to update the Command Texts of an in-progress check
		**	Created By: Suhas De
		**	Created Date: 19 Oct 2022
		**	Comments : 
		**		a. SET NOCOUNT ON added to prevent extra result sets from interfering with SELECT statements.
		**		b. This Stored Procedure is only called by ETL to update DQ Check status.
		**	=====================================================================================================
		**	Parameter Description:
		**	@ContextId: To update check for required instance/context
		**  @DQCheckId: To update check
		**  @SourceCommand: The updated source command
		**	@TargetCommand: The updated source command
		**	=====================================================================================================
		**	Version		ModifiedDate		ModifiedBy			Description
		**	=====================================================================================================
		**	1.0			19 Oct 2022			Suhas De			Initial Version
		**	=====================================================================================================
		*/
		SET NOCOUNT ON;
		
		BEGIN TRY
			IF (
				((SELECT [SourceCommand] FROM [dbo].[ADQCCheckReports] WHERE [CheckContextId] = @ContextId AND [CheckId] = @DQCheckId) != @SourceCommand)
				OR
				((SELECT [TargetCommand] FROM [dbo].[ADQCCheckReports] WHERE [CheckContextId] = @ContextId AND [CheckId] = @DQCheckId) != @TargetCommand)
			)
				BEGIN
					UPDATE [dbo].[ADQCCheckReports]
						SET [SourceCommand] = @SourceCommand,
							[ShouldDeriveSourceCommand] = 0,
							[TargetCommand] = @TargetCommand,
							[ShouldDeriveTargetCommand] = 0
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