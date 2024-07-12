/***********************************************************************************************
	*	Procedure Name	: spEDWBeginAuditLog
	*
	*	Date			: 30-Jun-2016
	*
	*	Author			: Harshitha
	*
	*	Parameters		: @AuditId
	*
	*	Purpose         : Procedure is used to insert a record into EDW audit table 
						  before the DML operation on the table
	*
	*	Change History:
	*	Date                  	  Programmer     		 Reason
	*	--------------------      -------------------    ------------------------------
	*	06-30-2016    			  Harshitha	    		 Initial Version
	*	17-10-2016    			  Harshitha	    		 Modified the procedure to add TaskName in the BeginAudit itself
	*   20-07-2023				  Suhas De				 BI-8263: Changes for Historical Target Reload
	**************************************************************************************************/
CREATE PROCEDURE [dbo].[spEDWBeginAuditLog] 
	-- parameters for the stored procedure
	@AuditId					BIGINT OUTPUT,
	@SourceName					VARCHAR(100),
	@LastBatchProcessDate		DATETIME = NULL,
	@IsOverride					BIT = NULL
AS
	BEGIN
		BEGIN TRANSACTION;

		BEGIN TRY
			-- SET NOCOUNT ON added to prevent extra result sets from
			-- interfering with SELECT statements.

			SET NOCOUNT ON;
			SET LOCK_TIMEOUT 10000;

			INSERT INTO [dbo].[EDWAuditLog] (
				[StartTime], [StatusCode], [StatusName], [TaskName], [LastBatchProcessedDate], [IsOverride]
			) VALUES (
				GETDATE(), 0, 'InProcess', @SourceName, @LastBatchProcessDate, @IsOverride
			);

			SET @AuditId = SCOPE_IDENTITY();

			SET NOCOUNT OFF;
			COMMIT TRANSACTION;
		END TRY

		BEGIN CATCH

			IF (@@TRANCOUNT > 0)
				ROLLBACK TRANSACTION;

			EXEC spEDWErrorAuditLog @AuditId = @AuditId;

		END CATCH
	END;