CREATE PROCEDURE [dbo].[spCMS_SourceToStaging_ARBalanceSnapshot]
(@FiscalWeekEndDate DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_SourceToStaging_ARBalanceSnapshot
    --
    -- Purpose:            Performs the Insert / Update / Delete ETL process for
    --                         the ARBalanceSnapshot table from Source (via the tfnARBalance() UDF) 
    --                         to the staging table dbo.ARBalanceSnapshot.
    --

    -- Parameters:         @FiscalWeekEndDate - Used to populate any 'current date' fields, rather than
    --                         making numerous GETDATE() calls  
    --                     @DebugMode - Used just for development & debug purposes,
    --                         outputting helpful info back to the caller.  Not
    --                         required for Production, and does not affect any
    --                         core logic.			   
    --
    -- Usage:              EXEC dbo.spCMS_SourceToStaging_ARBalanceSnapshot @FiscalWeekEndDate = @FiscalWeekEndDate
	--						, @DebugMode = 1	
    -- 
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date        Modified By         Comments
    -- --------    -----------         --------
    --
    --  2/21/18     sburke             BNG-1211 - Convert from SSIS DFT to stored proc, 
    --                                     and correct error in columns returned from
    --                                     tfnARBalance() and inserted into target.
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'ARBalanceSnapshot ' + CONVERT(VARCHAR(10), @FiscalWeekEndDate);
         DECLARE @AuditId BIGINT;

	    --
	    -- ETL status Variables
	    --
         DECLARE @RowCount INT;
         DECLARE @Error INT;
         DECLARE @SourceCount INT= 0;
         DECLARE @InsertCount INT= 0;
         DECLARE @UpdateCount INT= 0;
         DECLARE @DeleteCount INT= 0;

	    --

         IF @DebugMode = 1
             SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Starting.';
         PRINT @DebugMsg;

	    --
	    -- Write to AuditLog we are starting
	    --

         EXEC [dbo].[spStagingBeginAuditLog]
              @SourceName = @SourceName,
              @AuditId = @AuditId OUTPUT;
         
		 BEGIN TRY

             -- ================================================================================
             -- Delete the existing week (we do a reload of data)         
             -- ================================================================================
		BEGIN TRANSACTION;

             DELETE FROM ARBalanceSnapshot
             WHERE AsOfFiscalWeekEndDate = @FiscalWeekEndDate;
             SELECT @DeleteCount = @@ROWCOUNT;
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Deleted '+CONVERT(NVARCHAR(20), @DeleteCount)+' rows from Source for '+CONVERT(NVARCHAR(20), @FiscalWeekEndDate)+'.';
             PRINT @DebugMsg;
	      
             -- ================================================================================
             -- Insert data for the existing week      
             -- ================================================================================
             INSERT INTO ARBalanceSnapshot
             (AsOfFiscalWeekEndDate,
              ARAgingDays,
              ARBalanceType,
              idSite,
              idStudent,
              idSponsor,
              idSubsidyAgency,
              TransactionNumber,
              TransactionDate,
              ARAgingDate,
              ARBalanceAmount,
              AppliedAmount,
              TransactionAmount
             )
                    SELECT AsOfDate,
                           ARAgingDays,
                           ARBalanceType,
                           idSite,
                           idStudent,
                           idSponsor,
                           idSubsidyAgency,
                           TransactionNumber,
                           TransactionDate,
                           ARAgingDate,
                           ARBalanceAmount,
                           AppliedAmount,
                           TransactionAmount
                    FROM dbo.tfnARBalance(@FiscalWeekEndDate);
             SELECT @SourceCount = @@ROWCOUNT;
             SELECT @InsertCount = @SourceCount;

             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(NVARCHAR(20), @InsertCount)+' rows into Target for '+CONVERT(NVARCHAR(20), @FiscalWeekEndDate)+'.';
                     PRINT @DebugMsg;
             END;             

		  
             -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Committing transaction.';
             PRINT @DebugMsg;
		   
		   --
		   -- Commit the successful transaction 
		   --

             COMMIT TRANSACTION;

		   --
		   -- Write our successful run to the AuditLog 
		   --

             EXEC [dbo].[spStagingEndAuditLog]
                  @InsertCount = @InsertCount,
                  @UpdateCount = @UpdateCount,
                  @DeleteCount = @DeleteCount,
                  @SourceCount = @SourceCount,
                  @AuditId = @AuditId;


		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Completing successfully.';
             PRINT @DebugMsg;
         END TRY

         BEGIN CATCH
	    	  -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Rolling back transaction.';
             PRINT @DebugMsg;

		   -- Rollback the transaction
             ROLLBACK TRANSACTION;

		   --
		   -- Write our failed run to the EDW AuditLog 
		   --
             EXEC [dbo].[spStagingErrorAuditLog]
                  @AuditId = @AuditId;

		   --
		   -- Raiserror
		   --

             DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
             SELECT @ErrMsg = ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;

     END;
GO