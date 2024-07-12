
CREATE PROC    [dbo].[spStagingEndAuditLog] 
    -- parameters for the stored procedure
@InsertCount        INT,
@UpdateCount        INT,
@DeleteCount        INT,
@SourceCount        INT,
@AuditId            BIGINT
AS
BEGIN
BEGIN TRY
BEGIN TRANSACTION

    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON;

UPDATE    StagingAuditLog
        SET    EndTime  = GETDATE(),
            StatusCode  = CASE WHEN StatusCode = 0 THEN 1  ELSE StatusCode END,
            StatusName  = CASE WHEN StatusCode = 0 THEN 'Complete'  ELSE StatusName END,
            InsertCount = CASE WHEN StatusCode = 0 OR StatusCode = 1 THEN @InsertCount ELSE 0 END,
            SourceCount = CASE WHEN StatusCode = 0 OR StatusCode = 1 THEN @SourceCount ELSE 0 END,
            UpdateCount = CASE WHEN StatusCode = 0 OR StatusCode = 1 THEN @UpdateCount ELSE 0 END,
            DeleteCount = CASE WHEN StatusCode = 0 OR StatusCode = 1 THEN @DeleteCount ELSE 0 END
WHERE AuditId=@AuditId 

/*

UPDATE    StagingAuditLog
        SET    EndTime  = GETDATE() ,
            StatusCode = CASE WHEN StatusCode = 0 THEN 1  ELSE StatusCode END,
            StatusName = CASE WHEN StatusCode = 0 THEN 'Complete'  ELSE StatusName END,
            InsertCount =    @InsertCount,
            SourceCount =    @SourceCount,
            TaskName =        @SourceName ,
            UpdateCount =    @UpdateCount ,
            DeleteCount =    @DeleteCount
WHERE AuditId=@AuditId 

*/

    
    SET NOCOUNT OFF;

COMMIT TRANSACTION
END TRY

BEGIN CATCH
ROLLBACK TRANSACTION
    EXEC spStagingErrorAuditLog @AuditId = @AuditId
END CATCH

END