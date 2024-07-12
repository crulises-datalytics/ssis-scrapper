CREATE PROCEDURE [dbo].[spGL_StagingToEDW_FactGLBalance]
(@EDWRunDateTime    DATETIME2 = NULL, 
 @AuditId           BIGINT,
 @FiscalPeriodNumber	INT,
 @DebugMode         INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spGL_StagingToEDW_FactGLBalance
    --
    -- Purpose:            Performs the Insert / Update / Delete ETL process for
    --					  the FactGLBalance table from Staging to BING_EDW.
    --
    --                     Step 1: Create temporary landing #table
    --                     Step 2: Populate the Landing table from Source by calling
    --                         sub-procedure spGL_StagingTransfrom_FactGLBalance, 
    --                         and create any helper indexes.
    --                         The sub-proc can be called with a flag to bring back data
    --                         for a given time period or all data (@ReturnAll), which can
    --                         be leveraged for batched loads for performance / log space concerns.
    --                     Step 3: Perform the Insert / Update (Merge) / deletes required 
    --                          for this EDW table load
    --                     Step 4: Execute any automated tests associated with this EDW table load
    --                     Step 5: Output Source / Insert / Update / Delete counts to caller, 
    --                         commit the transaction, and tidy-up
    --
    -- Parameters:		  @EDWRunDateTime
    --                     @AuditId - This is the Audit tracking ID used both in the EDWAuditLog and EDWBatchLoadLog tables 
    --                     @FiscalYearNumber - Fiscal Year.  This proc has the option
    --                         of only returning data for a given Fiscal Year, which can 
    --                         be leveraged as part of a batched process (so we don't
    --                         extract and load a huge dataset in one go).
    --                     @DebugMode - Used just for development & debug purposes,
    --                         outputting helpful info back to the caller.  Not
    --                         required for Production, and does not affect any
    --                         core logic.
    --
    --
    -- Usage:              EXEC dbo.spGL_StagingToEDW_FactGLBalance @EDWRunDateTime = @EDWRunDateTime, @DebugMode = 1
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date		Modified By		Comments
    -- ----		-----------		--------
    --
    -- 11/07/17  Bhanu		    BNG-787 - Refactored EDW FactGLBalance load
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @TaskName VARCHAR(100)= 'FactGLBalance';
		 

	    --
	    -- For larger Fact (and Dimension) loads we will split the loads
	    --
         DECLARE @BatchSplitByName VARCHAR(50)= 'FiscalPeriodNumber';
         DECLARE @BatchSplitByValue INT= @FiscalPeriodNumber

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
	    --
	    -- If we do not get an @EDWRunDateTime input, set to current date
	    --
         IF @EDWRunDateTime IS NULL
             SET @EDWRunDateTime = GETDATE(); 
	    --
         IF @DebugMode = 1
             SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Starting.';
         PRINT @DebugMsg;

	    -- --------------------------------------------------------------------------------
	    -- Extract from Source, Upserts and Deletes contained in a single transaction.  
	    --	 Rollback on error
	    -- --------------------------------------------------------------------------------
         BEGIN TRY
             BEGIN TRANSACTION;
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Beginning transaction.';
             PRINT @DebugMsg;
		   -- ================================================================================
		   --
		   -- S T E P   1.
		   --
		   -- Create temporary landing #table
		   --
		   -- ================================================================================
             CREATE TABLE #FactGLBalanceUpsert
			 ([DateKey]                     INT NOT NULL,
	          [OrgKey]                      INT NOT NULL,
	          [LocationKey]                 INT NOT NULL,
	          [CompanyKey]                  INT NOT NULL,
	          [CostCenterTypeKey]           INT NOT NULL,
	          [CostCenterKey]               INT NOT NULL,
	          [AccountSubaccountKey]        INT NOT NULL,
	          [DataScenarioKey]             INT NOT NULL,
	          [GLMetricTypeKey]             INT NOT NULL,
	          [GLBalanceAmount]             NUMERIC(19, 4) NOT NULL,
	          [EDWCreatedDate] [datetime2](7) NOT NULL,
	          [Deleted] [datetime2](7) NULL
			  );
             
          

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table from Source, and create any helper indexes
		   --
		   -- ================================================================================
             INSERT INTO #FactGLBalanceUpsert
             EXEC dbo.spGL_StagingTransform_FactGLBalance
                  @EDWRunDateTime = @EDWRunDateTime,
                  @FiscalPeriodNumber = @FiscalPeriodNumber; -- @FiscalPeriodNumber needs to be provided


		   -- Get how many rows were extracted from source 

             SELECT @SourceCount = COUNT(1)
             FROM #FactGLBalanceUpsert;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Source.';
             PRINT @DebugMsg;

		   -- ================================================================================
		   --
		   -- S T E P   3.
		   --
		   -- Perform the Insert / Update (Merge) / deletes required for this EDW table load
		   --
		   -- For this Fact load, we do not update or merge - instead we delete and reload
		   --     the data for a given Fiscal Period, hence why we are so careful about batching
		   --     up the ETL.
		   --
		   -- ================================================================================
		   
		   --
		   -- Delete data for the given Fiscal Period
		   --
             DELETE BING_EDW.dbo.FactGLBalance
             FROM BING_EDW.dbo.FactGLBalance T
                  INNER JOIN #FactGLBalanceUpsert S ON S.DateKey = T.DateKey;
                                                        
             SELECT @DeleteCount = @@ROWCOUNT;

		   -- Debug output progress
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Deleted '+CONVERT(NVARCHAR(20), @DeleteCount)+' rows from Target.';
                     PRINT @DebugMsg;
             END;
		   --
		   -- Insert the data for the Fiscal Period
		   --
             INSERT INTO BING_EDW.dbo.FactGLBalance
             ([DateKey]
             ,[OrgKey]
             ,[LocationKey]
             ,[CompanyKey]
             ,[CostCenterTypeKey]
             ,[CostCenterKey]
             ,[AccountSubaccountKey]
             ,[DataScenarioKey]
             ,[GLMetricTypeKey]
             ,[GLBalanceAmount]
             ,[EDWCreatedDate]
             ,[Deleted]
             )
                    SELECT *
                    FROM #FactGLBalanceUpsert;
             SELECT @InsertCount = @@ROWCOUNT;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(NVARCHAR(20), @InsertCount)+' rows into Target.';
                     PRINT @DebugMsg;
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Updated '+CONVERT(NVARCHAR(20), @UpdateCount)+' rows into Target.';
                     PRINT @DebugMsg;
             END;             
		   

		   -- ================================================================================
		   --
		   -- S T E P   4.
		   --
		   -- Execute any automated tests associated with this EDW table load
		   --
		   -- ================================================================================


		   -- ================================================================================
		   --
		   -- S T E P   5.
		   --
		   -- Output Source / Insert / Update / Delete counts to caller, commit the transaction,
		   --	and tidy tup.
		   --
		   -- ================================================================================
		  
		  -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Committing transaction.';
             PRINT @DebugMsg;
		   
		   --
		   -- Commit the successful transaction 
		   --
             COMMIT TRANSACTION;

		   --
		   -- Drop the temp table
		   --
             DROP TABLE #FactGLBalanceUpsert;

		   --
		   -- Write our successful run to the EDW AuditLog 
		   --
             EXEC BING_EDW.dbo.spInsertEDWBatchLoadLog
                  @AuditId = @AuditId,
                  @TaskName = @TaskName,
                  @BatchSplitByName = @BatchSplitByName,
                  @BatchSplitByValue = @BatchSplitByValue,
                  @SourceCount = @SourceCount,
                  @InsertCount = @InsertCount,
                  @UpdateCount = @UpdateCount,
                  @DeleteCount = @DeleteCount,
                  @StartTime = @EDWRunDateTime;

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
		   -- Raiserror
		   --	
             DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
             SELECT @ErrMsg = ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;