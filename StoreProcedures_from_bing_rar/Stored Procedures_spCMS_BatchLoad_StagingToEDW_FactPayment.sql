CREATE PROCEDURE dbo.spCMS_BatchLoad_StagingToEDW_FactPayment
	@EDWRunDateTime DATETIME2 = NULL,
	@DebugMode      INT       = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_BatchLoad_StagingToEDW_FactPayment
    --
    -- Purpose:            Performs the Insert / Update / Delete ETL process for
    --					  the FactPayment table from Staging to BING_EDW, loading
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
    -- Usage:              EXEC dbo.spCMS_BatchLoad_StagingToEDW_FactPayment @DebugMode = 1
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date				Modified By         Comments
    -- ----				-----------         --------
    --
    -- 10/17/17			sburke              BNG-639 / BNG-640.  Refactoring StagingToEDW process
    -- 08/09/2023		Suhas.De			BI-9051 - Changes for Target Reload
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @TaskName VARCHAR(100)= 'FactPayment';
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
         DECLARE @FiscalYearNumber INT;
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
        /* 
		EXEC [BING_EDW].[dbo].[spEDWBeginAuditLog]
            @SourceName = @TaskName,
            @AuditId = @AuditId OUTPUT;
		*/
		DECLARE @LastProcessedDate DATE, @MinProcessingDate DATE;
		SELECT
			@LastProcessedDate = ISNULL([LastProcessedDate], '2001-01-01'),
			@MinProcessingDate = ISNULL([MinProcessingDate], '2011-01-01')
		FROM [dbo].[EDWETLBatchControl] (NOLOCK)
		WHERE [EventName] = @TaskName;

		BEGIN TRY

		   -- ================================================================================
		   -- STEP 1.
		   -- 
		   -- Ascertain by what criteria we are splitting this batch load by, and build a loop
		   -- ================================================================================
              -- DECLARE @BatchByFiscalYear TABLE(FiscalYearNumber INT);
			  DROP TABLE IF EXISTS #OriginalProcessingRange;
			  CREATE TABLE #OriginalProcessingRange ( [BatchIdentifier] SQL_VARIANT );
		   
		   --
		   -- Populate the Fiscal Years we want to load from
		   --
			-- INSERT INTO @BatchByFiscalYear(FiscalYearNumber)
			INSERT INTO #OriginalProcessingRange([BatchIdentifier])
            SELECT DISTINCT
                    YEAR(PaymentDate)
            FROM dbo.finStudentPayment;

			DROP TABLE IF EXISTS #FinalProcessingRange;
			CREATE TABLE #FinalProcessingRange ( [FiscalYearNumber] INT );

			EXEC [BING_EDW].[dbo].[spEDW_StagingToEDW_GetProcessingRange]
				@SourceName = @SourceName,
				@TaskName = @TaskName,
				@MinProcessingDate = @MinProcessingDate,
				@LastProcessedDate = @LastProcessedDate,
				@DimDateColumnName = 'FiscalYearNumber',
				@IsOverrideBatch = @IsOverrideBatch OUTPUT,
				@AuditID = @AuditId OUTPUT,
				@Debug = 1;

		   -- ================================================================================
		   -- STEP 2.
		   -- 
		   -- Loop through each Fiscal Year, and execute the ETL Upsert Stored Proc
		   -- ================================================================================
             DECLARE csr_fact_payment_fiscal_year CURSOR
             FOR
                 SELECT FiscalYearNumber
                 -- FROM @BatchByFiscalYear
				 FROM #FinalProcessingRange
                 ORDER BY FiscalYearNumber;             
		   --
		   -- Use cursor to loop through the values we have chosen to split this batch by
		   --
             OPEN csr_fact_payment_fiscal_year;
             FETCH NEXT FROM csr_fact_payment_fiscal_year INTO @FiscalYearNumber;
             WHILE @@FETCH_STATUS = 0
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Processing data for Fiscal Year '+CONVERT(VARCHAR(10), @FiscalYearNumber);
                     PRINT @DebugMsg;
				 -- ================================================================================
				 -- Execute the main StagingToEDW stored proc to load data for the given batch
				 -- ================================================================================
                     EXEC dbo.spCMS_StagingToEDW_FactPayment
                          @AuditId = @AuditId,
                          @FiscalYearNumber = @FiscalYearNumber;
                     FETCH NEXT FROM csr_fact_payment_fiscal_year INTO @FiscalYearNumber;
                 END;
             CLOSE csr_fact_payment_fiscal_year;
             DEALLOCATE csr_fact_payment_fiscal_year;

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
					AND [TaskName] = @TaskName;
				END;
			ELSE
				BEGIN
					EXEC dbo.spCMS_StagingEDWETLBatchControl @TaskName = @TaskName;
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
GO