
/***********************************************************************************************
	*	Procedure Name	: spEDWEndAuditLog
	*
	*	Date			: 30-Jun-2016
	*
	*	Author			: Harshitha
	*
	*	Parameters		: @InsertCount,
						  @UpdateCount,
						  @DeleteCount,
						  @SourceCount,
						  @AuditId,
						  @SourceName
	*
	*	Purpose         : Procedure is used to update EDW audit table after the DML operation on the table
	*
	*	Change History:
	*	Date                  	  Programmer     		 Reason
	*	--------------------      -------------------    ---------------------------------------------
	*	30-Jun-2016    			  Harshitha	    		 Initial Version
	*   15-Jun-2016               Harshitha              Commented the "ELSE" condition which updates 
	                                                     the Audit Log to '0' on Failure on Vikas's 
														 recommendation
	**************************************************************************************************/
CREATE PROC	[dbo].[spEDWEndAuditLog] 
	-- parameters for the stored procedure
	@InsertCount		INT,
	@UpdateCount		INT,
	@DeleteCount		INT,
	@SourceCount		INT,
	@AuditId			BIGINT
AS
	BEGIN
		BEGIN TRY
			BEGIN TRANSACTION

				-- SET NOCOUNT ON added to prevent extra result sets from
				-- interfering with SELECT statements.
				SET NOCOUNT ON;

			DECLARE	@StatusCode INT;
			SELECT @StatusCode = StatusCode FROM EDWAuditLog(NOLOCK) WHERE AuditId = @AuditId;


			IF(@StatusCode = 0 or @StatusCode = 1)
				BEGIN
					UPDATE	EDWAuditLog
							SET	EndTime  =    GETDATE() ,
								StatusCode =  1,
								StatusName =  'Complete',
								InsertCount = @InsertCount,
								SourceCount = @SourceCount,
								UpdateCount = @UpdateCount,
								--ABS(@UpdateCount - @DeleteCount),
								DeleteCount = @DeleteCount 
					WHERE AuditId=@AuditId
				END
			ELSE
				BEGIN
					UPDATE	EDWAuditLog
							SET	EndTime  = GETDATE() ,
								StatusCode =  StatusCode,
								StatusName =  StatusName,
								InsertCount = 0,
								SourceCount = 0,
								UpdateCount = 0,
								DeleteCount = 0 
					WHERE AuditId=@AuditId
				END	

			/*
			UPDATE	EDWAuditLog
					SET	EndTime  = GETDATE() ,
						StatusCode =  CASE WHEN StatusCode = 0 THEN 1  ELSE StatusCode END,
						StatusName =  CASE WHEN StatusCode = 0 THEN 'Complete'  ELSE StatusName END,
						InsertCount = CASE WHEN StatusCode = 0 THEN	@InsertCount ELSE 0 END,
						SourceCount = CASE WHEN StatusCode = 0 THEN	@SourceCount ELSE 0 END,
						UpdateCount = CASE WHEN StatusCode = 0 THEN	@UpdateCount ELSE 0 END  ,
						DeleteCount = CASE WHEN StatusCode = 0 THEN	@DeleteCount ELSE 0 END  
			WHERE AuditId=@AuditId
			*/
			SET NOCOUNT OFF;

			COMMIT TRANSACTION
		END TRY

		BEGIN CATCH

			ROLLBACK TRANSACTION
			EXEC spEDWErrorAuditLog @AuditId = @AuditId

		END CATCH

	END


