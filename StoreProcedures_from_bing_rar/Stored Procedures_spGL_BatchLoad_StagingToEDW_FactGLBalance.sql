
CREATE PROCEDURE [dbo].[spGL_BatchLoad_StagingToEDW_FactGLBalance]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL,
 @PeriodBack       INT       = 1 -- This is how many days back the process wil go from the LastProcessedDate (defaults to 1 month)
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spGL_BatchLoad_StagingToEDW_FactGLBalance
    --
    -- Purpose:            Performs the Insert / Update / Delete ETL process for
    --					  the FactGLBalance table from GL Staging to BING_EDW, loading
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
    -- Usage:              EXEC dbo.spGL_BatchLoad_StagingToEDW_FactGLBalance @EDWRunDateTime = @EDWRunDateTime, @DebugMode = 1
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date				Modified By         Comments
    -- ----				-----------         --------
    --
    -- 11/07/17			Bhanu		        BNG-787 - Refactored EDW FactGLBalance load
    -- 8/01/2023		Suhas.De			BI-8307 - Changes for Target Reload		 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @TaskName VARCHAR(100)= 'FactGLBalance';
		 DECLARE @SourceName SYSNAME = DB_NAME();
		 DECLARE @IsOverrideBatch BIT;

	    --
	    -- ETL status Variables
	    --
         DECLARE @RowCount INT;
         DECLARE @Error INT;

	    --
	    -- ETL variables specific to this load
	    --
         DECLARE @AuditId BIGINT;
         DECLARE @FiscalPeriodNumber INT;
         DECLARE @SourceCount INT= 0;
         DECLARE @InsertCount INT= 0;
         DECLARE @UpdateCount INT= 0;
         DECLARE @DeleteCount INT= 0;
	    -- ================================================================================
	    -- 
	    -- Determine how far back in history we have extract data for
	    --
		DECLARE @LastProcessedDate DATE, @MinProcessingDate DATE;
         
             SELECT @LastProcessedDate = LastProcessedDate, @MinProcessingDate = [MinProcessingDate]
             FROM GL_Staging..EDWETLBatchControl(NOLOCK)
             WHERE EventName = @TaskName;
         
         IF @LastProcessedDate IS NULL
             SET @LastProcessedDate = '19000101';   -- If no previous load logged in EDWETLBatchControl, assume we bring in everything
		--
		-- We use FiscalPeriodNumber for GL Balances.
		--
         DECLARE @StartingFiscalPeriodNumber INT=
         (
             SELECT FiscalPeriodNumber
             FROM BING_EDW..vDimFiscalPeriod
             WHERE DATEADD(MM, -@PeriodBack, @LastProcessedDate) BETWEEN FiscalPeriodStartDate AND FiscalPeriodEndDate
                   AND FiscalPeriodNumber > 0
         );
		 IF @StartingFiscalPeriodNumber IS NULL
             SELECT @StartingFiscalPeriodNumber = MIN(FiscalPeriodNumber)
             FROM BING_EDW..vDimFiscalPeriod
             WHERE FiscalPeriodNumber > 0;
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
         /*
		 EXEC [BING_EDW].[dbo].[spEDWBeginAuditLog]
              @SourceName = @TaskName,
              @AuditId = @AuditId OUTPUT;
		 */

         BEGIN TRY

		   -- ================================================================================
		   -- STEP 1.
		   -- 
		   -- Ascertain by what criteria we are splitting this batch load by, and build a loop
		   -- ================================================================================
				DROP TABLE IF EXISTS #OriginalProcessingRange;
				CREATE TABLE #OriginalProcessingRange ( [BatchIdentifier] SQL_VARIANT );
					INSERT INTO #OriginalProcessingRange([BatchIdentifier])
                    SELECT DISTINCT period_id AS FiscalPeriodNumber -- INTO #BatchByFiscalPeriodNumber                           
                    FROM vGLBalances
					WHERE period_id >= @StartingFiscalPeriodNumber;

				DROP TABLE IF EXISTS #FinalProcessingRange;
				CREATE TABLE #FinalProcessingRange ( [FiscalPeriodNumber] INT );

				EXEC [BING_EDW].[dbo].[spEDW_StagingToEDW_GetProcessingRange]
					@SourceName = @SourceName,
					@TaskName = @TaskName,
					@MinProcessingDate = @MinProcessingDate,
					@LastProcessedDate = @LastProcessedDate,
					@DimDateColumnName = 'FiscalPeriodNumber',
					@IsOverrideBatch = @IsOverrideBatch OUTPUT,
					@AuditID = @AuditId OUTPUT,
					@Debug = 0;

		   -- ================================================================================
		   -- STEP 2.
		   -- 
		   -- Loop through each Fiscal Year, and execute the ETL Upsert Stored Proc
		   -- ================================================================================
             DECLARE csr_fact_Balance_GL_fisc_PeriodNumber CURSOR
             FOR
                 SELECT FiscalPeriodNumber
                 FROM #FinalProcessingRange --#BatchByFiscalPeriodNumber
                 ORDER BY FiscalPeriodNumber;             
		   --
		   -- Use cursor to loop through the values we have chosen to split this batch by FiscalPeriod
		   --
             OPEN csr_fact_Balance_GL_fisc_PeriodNumber;
             FETCH NEXT FROM csr_fact_Balance_GL_fisc_PeriodNumber INTO @FiscalPeriodNumber;
             WHILE @@FETCH_STATUS = 0
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Processing data for Fiscal Period Number '+CONVERT(VARCHAR(10), @FiscalPeriodNumber);
                     PRINT @DebugMsg;
				 -- ================================================================================
				 -- Execute the main StagingToEDW stored proc to load data for the given batch
				 -- ================================================================================
                     EXEC dbo.spGL_StagingToEDW_FactGLBalance
                          @AuditId = @AuditId,
                          @FiscalPeriodNumber = @FiscalPeriodNumber;
                     FETCH NEXT FROM csr_fact_Balance_GL_fisc_PeriodNumber INTO @FiscalPeriodNumber;
                 END;
             CLOSE csr_fact_Balance_GL_fisc_PeriodNumber;
             DEALLOCATE csr_fact_Balance_GL_fisc_PeriodNumber;

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
		   IF (@IsOverrideBatch = 1)
				BEGIN
					UPDATE [BING_EDW].[dbo].[EDWBatchOverride]
						SET [IsActive] = 0
					WHERE [SourceName] = @SourceName
					AND [TaskName] = @TaskName
					AND [IsActive] = 1;
				END;
			ELSE
				BEGIN
					EXEC dbo.spGL_StagingEDWETLBatchControl @TaskName;
				END;
             
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