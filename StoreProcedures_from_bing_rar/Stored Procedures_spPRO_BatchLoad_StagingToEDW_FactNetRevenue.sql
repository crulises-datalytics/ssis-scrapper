
CREATE PROCEDURE [dbo].[spPRO_BatchLoad_StagingToEDW_FactNetRevenue]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spPRO_BatchLoad_StagingToEDW_FactNetRevenue
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
    -- Usage:              EXEC dbo.spPRO_BatchLoad_StagingToEDW_FactNetRevenue @EDWRunDateTime = @EDWRunDateTime, @DebugMode = 1
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date        Modified By         Comments
    -- ----        -----------         --------
    --
    -- 11/10/17    	Harshitha			BNG-786 - Refactored EDW FactNetRevenue (PRO Source) load
	-- 05/23/19     hhebbalu            Removed the 14 days delete and reload logic and added the logic to load 
	--									all that is changed in the staging table since last run
    -- 
    --	03/18/22     Adevabhakthuni      BI-5598  Filted the stored Proc not to load after 202109 			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @TaskName VARCHAR(100)= 'FactNetRevenue - PRO';

	    --
	    -- ETL status Variables
	    --
         DECLARE @RowCount INT;
         DECLARE @Error INT;

	    --
	    -- ETL variables specific to this load
	    --
         DECLARE @AuditId BIGINT;
         DECLARE @FiscalWeekNumber INT;
         DECLARE @SourceCount INT= 0;
         DECLARE @InsertCount INT= 0;
         DECLARE @UpdateCount INT= 0;
         DECLARE @DeleteCount INT= 0;
		 	    -- ================================================================================
	    -- T H I S   I S   T H E   PRO   V E R S I O N   O F   F A C T  NET REVENUE  
	    --
	    -- ================================================================================
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
              DECLARE @BatchByFiscalWeekNumber TABLE(FiscalWeekNumber INT);
              DECLARE @SourceFiscalWeekNumber TABLE(FiscalWeekNumber INT);
              
			  INSERT INTO @SourceFiscalWeekNumber(FiscalWeekNumber)
                 SELECT (CAST([FiscalYear] AS INT) * 100 + CAST([FiscalWeekNbr] AS INT))
                 FROM dbo.CambridgeRevenue(NOLOCK)
				 WHERE StgModifiedDate > @LastProcessedDate

              INSERT INTO @BatchByFiscalWeekNumber(FiscalWeekNumber)
                    SELECT DISTINCT
                           d.FiscalWeekNumber
                    FROM BING_EDW.dbo.DimDate(NOLOCK)d
					JOIN @SourceFiscalWeekNumber s ON d.FiscalWeekNumber = s.FiscalWeekNumber
					WHERE FiscalWeekStartDate <= @EDWRunDateTime;

		   -- ================================================================================
		   -- STEP 2.
		   -- 
		   -- Loop through each Fiscal Year, and execute the ETL Upsert Stored Proc
		   -- ================================================================================
             DECLARE csr_fact_nrevenue_pro_fisc_weekNumber CURSOR
             FOR
                 SELECT FiscalWeekNumber
                 FROM @BatchByFiscalWeekNumber WHERE FiscalWeekNumber < 202109
                 ORDER BY FiscalWeekNumber;             
		   --
		   -- Use cursor to loop through the values we have chosen to split this batch by FiscalWeek
		   --
             OPEN csr_fact_nrevenue_pro_fisc_weekNumber;
             FETCH NEXT FROM csr_fact_nrevenue_pro_fisc_weekNumber INTO @FiscalWeekNumber;
             WHILE @@FETCH_STATUS = 0
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Processing data for Fiscal Week Number '+CONVERT(VARCHAR(10), @FiscalWeekNumber);
                     PRINT @DebugMsg;
				 -- ================================================================================
				 -- Execute the main StagingToEDW stored proc to load data for the given batch
				 -- ================================================================================
                     EXEC dbo.spPRO_StagingToEDW_FactNetRevenue
                          @AuditId = @AuditId,
                          @FiscalWeekNumber = @FiscalWeekNumber;
                     FETCH NEXT FROM csr_fact_nrevenue_pro_fisc_weekNumber INTO @FiscalWeekNumber;
                 END;
             CLOSE csr_fact_nrevenue_pro_fisc_weekNumber;
             DEALLOCATE csr_fact_nrevenue_pro_fisc_weekNumber;

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
             EXEC cms_Staging.dbo.spCMS_StagingEDWETLBatchControl
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