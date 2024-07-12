


/***********************************************************************************************
	*	Procedure Name	: Staging_Etl_Event_OnError
	*
	*	Date			: 27-June-2017
	*
	*	Author			: Rakesh
	*
	*	Parameters		: @AuditId
	*
	*	Purpose         : Procedure is used to update staging audit table when an error occurs
	*
	*	Change History:
	*	Date                  	  Programmer     		 Reason
	*	--------------------      -------------------    -------------------------
	*	06-27-2016    			  Rakesh		    		 Initial Version
	
	**************************************************************************************************/
CREATE PROCEDURE [dbo].[spStagingErrorAuditLog]
@AuditId		bigint
AS
BEGIN
	
	
BEGIN TRY
BEGIN TRANSACTION

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	
	SET NOCOUNT ON;
	SET LOCK_TIMEOUT 10000;
 

UPDATE	StagingAuditLog 
		SET		EndTime  = GETDATE(),
				StatusCode  = 2,	--FAILED
				StatusName  =  'Failed'
WHERE	AuditId  = @AuditId
COMMIT TRANSACTION
END TRY

BEGIN CATCH

ROLLBACK TRANSACTION
   EXEC spStagingErrorAuditLog @AuditId = @AuditId
END CATCH

	SET NOCOUNT OFF;
END