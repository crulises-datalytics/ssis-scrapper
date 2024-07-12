
CREATE PROCEDURE [dbo].[spErrorAuditLog]    
@AuditId  BIGINT    
AS    
BEGIN        
/***********************************************************************************************    
=========================================================================

Procedure:   spErrorAuditLog
Purpose         : Procedure is used to update  audit table when an error occurs  	  

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
 SET LOCK_TIMEOUT 10000;    
     
    
UPDATE AuditLog     
  SET  EndTime  = GETDATE(),    
    StatusCode  = 2, --FAILED    
    StatusName  =  'Failed',
	InsertCount = 0,
	SourceCount = 0,
	UpdateCount = 0,
	DeleteCount = 0 
WHERE AuditId  = @AuditId
   
COMMIT TRANSACTION    
END TRY    
    
BEGIN CATCH    
    
ROLLBACK TRANSACTION    
   EXEC spErrorAuditLog @AuditId = @AuditId    
END CATCH    
    
 SET NOCOUNT OFF;    
END