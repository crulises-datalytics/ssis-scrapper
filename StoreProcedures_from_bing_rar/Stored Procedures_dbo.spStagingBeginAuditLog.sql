

/***********************************************************************************************
	*	Procedure Name	: spStagingBeginAuditLog
	*
	*	Date			: 01-Mar-2017
	*
	*	Author			: Harshitha
	*
	*	Parameters		: @AuditId,
						  @SourceName
	*
	*	Purpose         : Procedure is used to insert a record into staging audit table 
						  before the DML operation on the table
	*
	*	Change History	:
	*	Date                  	  Programmer     		 Reason
	*	--------------------      -------------------    ----------------------------------------------
	*	03-01-2017    			  Harshitha	    		 Initial Version

	**************************************************************************************************/
CREATE PROCEDURE [dbo].[spStagingBeginAuditLog] 
	-- parameters for the stored procedure
@AuditId		Bigint Output,
@SourceName		VARCHAR(100)
AS
BEGIN
BEGIN TRY
BEGIN TRANSACTION
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
	SET LOCK_TIMEOUT 10000;

INSERT INTO StagingAuditLog
					(
						StartTime, StatusCode, StatusName, TaskName
					)
VALUES
					(
						GETDATE(), 0, 'InProcess', @SourceName
					)

SET @AuditId = SCOPE_IDENTITY();

	SET NOCOUNT OFF;
COMMIT TRANSACTION
END TRY

BEGIN CATCH

ROLLBACK TRANSACTION
	EXEC spStagingErrorAuditLog @AuditId = @AuditId

END CATCH


END