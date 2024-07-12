
CREATE PROC	[dbo].[spEndAuditLog] 
@InsertCount		INT,
@UpdateCount		INT,
@DeleteCount		INT,
@SourceCount		INT,
@AuditId			BIGINT
AS
BEGIN
/***********************************************************************************************    
=========================================================================

Procedure:   [spEndAuditLog]
Purpose         : Procedure is used to update  audit table after the DML operation on the table

-------------------------------------------------------------------------
-- Change Log:		   
-- ----------
-- Date         Modified By     Comments
-- ----         -----------     --------   
 04/13/2020     hhebbalu        Initial Version    
     
**************************************************************************************************/ 

BEGIN TRY
BEGIN TRANSACTION

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

DECLARE	@StatusCode INT;
SELECT @StatusCode = StatusCode FROM AuditLog(NOLOCK) WHERE AuditId = @AuditId;




UPDATE	AuditLog

        SET    EndTime  = GETDATE() ,
            StatusCode = CASE WHEN StatusCode = 0 THEN 1  ELSE StatusCode END,
            StatusName = CASE WHEN StatusCode = 0 THEN 'Complete'  ELSE StatusName END,
            SourceCount =    @SourceCount,
            InsertCount =    @InsertCount,
            UpdateCount =    @UpdateCount,
            DeleteCount =    @DeleteCount
WHERE AuditId=@AuditId 

SET NOCOUNT OFF;

COMMIT TRANSACTION
END TRY

BEGIN CATCH
ROLLBACK TRANSACTION
	EXEC spErrorAuditLog @AuditId = @AuditId
END CATCH

END