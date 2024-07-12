CREATE PROCEDURE [dbo].[spHOR_BatchLoad_StagingToEDW_FactNetRevenue]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHOR_BatchLoad_StagingToEDW_FactNetRevenue
    --
    -- Purpose:            Performs the Insert / Update / Delete ETL process for
    --					  the FactNetRevenue table from Staging to BING_EDW, loading
    --                          data in manageable batch sizes for larger datasets.  This
    --                          is to ensure we do not fill the log when performing 
    --                          inserts / updates on our larger tables
    --
    -- Parameters:		  @EDWRunDateTime
    --                     @DebugMode - Used just for development & debug purposes,
    --                         outputting helpful info back to the caller.  Not
    --                         required for Production, and does not affect any
    --                         core logic.
    --
    --
    -- Usage:              EXEC dbo.spHOR_BatchLoad_StagingToEDW_FactNetRevenue @EDWRunDateTime = @EDWRunDateTime, @DebugMode = 1
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date        Modified By         Comments
    -- ----        -----------         --------
    --
    -- 11/07/17    harshitha			BNG-785 - Refactored EDW FactNetRevenue (Horizon Source) load
	--  05/23/19     hhebbalu            Removed the 14 days delete and reload logic and added the logic to load 
	--									 all that is changed in the staging table since last run
    --				 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @TaskName VARCHAR(100)= 'FactNetRevenue - HOR';

	    --
	    -- ETL status Variables
	    --
         DECLARE @RowCount INT;
         DECLARE @Error INT;

	    --
	    -- ETL variables specific to this load
	    --
         DECLARE @AuditId BIGINT;
         DECLARE @FiscalWeekEndDate DATE;
         DECLARE @SourceCount INT= 0;
         DECLARE @InsertCount INT= 0;
         DECLARE @UpdateCount INT= 0;
         DECLARE @DeleteCount INT= 0;

	    --
	    -- If we do not get an @EDWRunDateTime input, set to current date
	    --
         IF @EDWRunDateTime IS NULL
             SET @EDWRunDateTime = GETDATE(); 
	    --
         IF @DebugMode = 1
             SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Starting.';
         PRINT @DebugMsg;
	    
	    -- Write to AuditLog that we are starting, and get the AuditId we use for each batch we load		  
         EXEC [BING_EDW].[dbo].[spEDWBeginAuditLog]
              @SourceName = @TaskName,
              @AuditId = @AuditId OUTPUT;

	    --
	    -- Determine how far back in history we have extract data for
	    --
         DECLARE @LastProcessedDate DATETIME=
         (
             SELECT LastProcessedDate
             FROM CMS_Staging..EDWETLBatchControl(NOLOCK)
             WHERE EventName = @TaskName
         );
         IF @LastProcessedDate IS NULL
             SET @LastProcessedDate = '19000101';   -- If no previous load logged in EDWETLBatchControl, assume we bring in everything

         BEGIN TRY

		   -- ================================================================================
		   -- STEP 1.
		   -- 
		   -- Ascertain by what criteria we are splitting this batch load by, and build a loop
		   -- ================================================================================
		     DECLARE @BatchByFiscalWeekEndDate TABLE(FiscalWeekEndDate DATE);
		     DECLARE @SourceFiscalDate TABLE(FiscalDate DATE);

             INSERT INTO @SourceFiscalDate(FiscalDate)
                    SELECT DISTINCT CAST(TransactionDate AS DATE)
					FROM dbo.finGeneralLedger(NOLOCK)
					WHERE StgModifiedDate > @LastProcessedDate;
			 
			 INSERT INTO @BatchByFiscalWeekEndDate(FiscalWeekEndDate)
                    SELECT DISTINCT
                           FiscalWeekEndDate
                    FROM BING_EDW.dbo.DimDate(NOLOCK)d
					JOIN @SourceFiscalDate s ON d.FullDate = s.FiscalDate
					WHERE FiscalWeekStartDate <= @EDWRunDateTime;
		   -- ================================================================================
		   -- STEP 2.
		   -- 
		   -- Loop through each Fiscal Year, and execute the ETL Upsert Stored Proc
		   -- ================================================================================
             DECLARE csr_fact_nrevenue_hor_fisc_weekend CURSOR
             FOR
                 SELECT FiscalWeekEndDate
                 FROM @BatchByFiscalWeekEndDate
                 ORDER BY FiscalWeekEndDate;             
		   --
		   -- Use cursor to loop through the values we have chosen to split this batch by
		   --
             OPEN csr_fact_nrevenue_hor_fisc_weekend;
             FETCH NEXT FROM csr_fact_nrevenue_hor_fisc_weekend INTO @FiscalWeekEndDate;
             WHILE @@FETCH_STATUS = 0
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Processing data for Fiscal Week End Date '+CONVERT(VARCHAR(10), @FiscalWeekEndDate);
                     PRINT @DebugMsg;
				 -- ================================================================================
				 -- Execute the main StagingToEDW stored proc to load data for the given batch
				 -- ================================================================================
                     EXEC dbo.spHOR_StagingToEDW_FactNetRevenue
                          @AuditId = @AuditId,
                          @FiscalWeekEndDate = @FiscalWeekEndDate;
                     FETCH NEXT FROM csr_fact_nrevenue_hor_fisc_weekend INTO @FiscalWeekEndDate;
                 END;
             CLOSE csr_fact_nrevenue_hor_fisc_weekend;
             DEALLOCATE csr_fact_nrevenue_hor_fisc_weekend;

		   -- ================================================================================
		   -- STEP 3.
		   --
		   -- Once we have successfully ran all the batch loads, collect all the Source / Insert 
		   --     / Update / Delete numbers from all the batch loads for this table, and use them
		   --     to pupulate EDWEndAuditLog
		   -- ================================================================================

             SELECT @SourceCount = SUM(SourceCount)
             FROM BING_EDW.dbo.EDWBatchLoadLog
             WHERE AuditId = @AuditId;
             SELECT @InsertCount = SUM(InsertCount)
             FROM BING_EDW.dbo.EDWBatchLoadLog
             WHERE AuditId = @AuditId;
             SELECT @UpdateCount = SUM(UpdateCount)
             FROM BING_EDW.dbo.EDWBatchLoadLog
             WHERE AuditId = @AuditId;
             SELECT @DeleteCount = SUM(DeleteCount)
             FROM BING_EDW.dbo.EDWBatchLoadLog
             WHERE AuditId = @AuditId;

		   -- Write the successful load to EDWAuditLog
             EXEC [BING_EDW].[dbo].[spEDWEndAuditLog]
                  @InsertCount = @InsertCount,
                  @UpdateCount = @UpdateCount,
                  @DeleteCount = @DeleteCount,
                  @SourceCount = @SourceCount,
                  @AuditId = @AuditId;

		   -- Also write the successful load to EDWETLBatchControl, so we know how far back in history
		   --     we have to go in the next ETL run
             EXEC CMS_Staging.dbo.spCMS_StagingEDWETLBatchControl
                  @TaskName;
         END TRY
         BEGIN CATCH
             EXEC [BING_EDW].[dbo].[spEDWErrorAuditLog]
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