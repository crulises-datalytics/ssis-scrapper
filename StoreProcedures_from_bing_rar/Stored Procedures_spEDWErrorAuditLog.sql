    
/***********************************************************************************************    
 * Procedure Name : spEDWErrorAuditLog    
 *    
 * Date   : 30-Jun-2016    
 *    
 * Author   : Harshitha    
 *    
 * Parameters  : @AuditId    
 *    
 * Purpose         : Procedure is used to update EDW audit table when an error occurs    
 *    
 * Change History:    
 * Date                     Programmer        Reason    
 * --------------------      -------------------    -------------------------    
 * 30-Jun-2016         Harshitha        Initial Version    
     
 **************************************************************************************************/    
CREATE PROCEDURE [dbo].[spEDWErrorAuditLog]    
	@AuditId  BIGINT    
AS    
	BEGIN    
     
		BEGIN TRY    
			BEGIN TRANSACTION  
    
			-- SET NOCOUNT ON added to prevent extra result sets from    
			-- interfering with SELECT statements.    
     
			SET NOCOUNT ON;    
			SET LOCK_TIMEOUT 10000;    
     
    
			UPDATE EDWAuditLog     
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
			EXEC spEDWErrorAuditLog @AuditId = @AuditId    

		END CATCH    
    
		SET NOCOUNT OFF;    
	END    