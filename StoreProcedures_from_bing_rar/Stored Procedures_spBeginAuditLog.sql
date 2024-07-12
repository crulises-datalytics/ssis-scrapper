

CREATE PROCEDURE [dbo].[spBeginAuditLog] 
@AuditId		   Bigint Output,
@SourceName		   VARCHAR(100),
@ExecutionID       VARCHAR(300) = NULL,
@BatchSplitByName  VARCHAR(50) = NULL,
@BatchSplitByValue DATE = NULL
AS
BEGIN
/***********************************************************************************************    
=========================================================================

Procedure:	 [spBeginAuditLog]
Purpose  :	 Procedure is used to update  audit table  before the DML operation on the table 	  

-------------------------------------------------------------------------
-- Change Log:		   
-- ----------
-- Date         Modified By     Comments
-- ----         -----------     --------   
 04/13/2020     hhebbalu        Initial Version    

**************************************************************************************************/   
BEGIN TRANSACTION
BEGIN TRY
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
	SET LOCK_TIMEOUT 10000;

INSERT INTO AuditLog
					(
						StartTime, StatusCode, StatusName, TaskName, BatchSplitByName, BatchSplitByValue, ExecutionID
					)
VALUES
					(
						GETDATE(), 0, 'InProcess', @SourceName, @BatchSplitByName, @BatchSplitByValue, @ExecutionID
					)

SET @AuditId = SCOPE_IDENTITY();

	SET NOCOUNT OFF;
COMMIT TRANSACTION
END TRY

BEGIN CATCH

ROLLBACK TRANSACTION
	EXEC spErrorAuditLog @AuditId = @AuditId

END CATCH


END