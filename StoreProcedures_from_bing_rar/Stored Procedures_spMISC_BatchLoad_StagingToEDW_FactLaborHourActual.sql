CREATE PROCEDURE [dbo].[spMISC_BatchLoad_StagingToEDW_FactLaborHourActual]
	@EDWRunDateTime DATETIME2 = NULL, 
	@DebugMode      INT       = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spMISC_BatchLoad_StagingToEDW_FactLaborHourActual
    --
    -- Purpose:            Performs the Insert / Update / Delete ETL process for
    --                         the FactLaborHour table from Staging to BING_EDW, loading
    --                         data in manageable batch sizes for larger datasets.  This
    --                         is to ensure we do not fill the log when performing 
    --                         inserts / updates on our larger tables
    --
    -- Parameters:         @EDWRunDateTime
    --                     @DebugMode - Used just for development & debug purposes,
    --                         outputting helpful info back to the caller.  Not
    --                         required for Production, and does not affect any
    --                         core logic.
    --
    --
    --Usage:              EXEC dbo.spMISC_BatchLoad_StagingToEDW_FactLaborHourActual
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date				Modified By         Comments
    -- ----				-----------         --------
    --
    -- 6/29/18			anmorales           BNG-1747 - Labor hour mapping - Write Logic that does look up for Fact table load
    -- 04/30/19			hhebbalu            BI-892 - Fix ETL for Fact Labor Hour Actual - the code was written to delete last 
    --											14 days from today in the fact and reload. So changed that to load any data that has changed in staging
    --											to the Fact.  	
	-- 03/05/20			Adevabhakthuni      BI-3569 Changed  the logic to get the FiscalWeekenddates 
	-- 06/28/22			Vishal.Sawant		BI-3495 Add missing paycode every run.
	-- 08/09/2023		Suhas.De			BI-9051 - Changes for Target Reload
    -- ================================================================================
    BEGIN
        SET NOCOUNT ON;

        --
        -- Housekeeping Variables
        --
        DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
        DECLARE @DebugMsg NVARCHAR(500);
        DECLARE @TaskName VARCHAR(100)= 'FactLaborHour';
		DECLARE @SourceName SYSNAME = DB_NAME();
		DECLARE @IsOverrideBatch BIT;
		DECLARE @RefDate DATE;

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
            SELECT @DebugMsg = @ProcName + ' : ' + CONVERT(NVARCHAR(20), GETDATE()) + ' - Starting.';
        PRINT @DebugMsg;

        -- Write to AuditLog that we are starting, and get the AuditId we use for each batch we load
		/*
        EXEC [BING_EDW].[dbo].[spEDWBeginAuditLog] 
             @SourceName = @TaskName, 
             @AuditId = @AuditId OUTPUT;
		*/

        --
        -- Determine how far back in history we have extract data for
        --
        DECLARE @LastProcessedDate DATE, @MinProcessingDate DATE;

        SELECT @LastProcessedDate = LastProcessedDate, @MinProcessingDate = [MinProcessingDate]
		FROM [MISC_Staging].[dbo].[EDWETLBatchControl] (NOLOCK)
		WHERE [EventName] = @TaskName;

        IF @LastProcessedDate IS NULL
            SET @LastProcessedDate = '19000101';   -- If no previous load logged in EDWETLBatchControl, assume we bring in everything

		SELECT
			@RefDate = MAX(CAST(DateWorked AS DATE))
		FROM dbo.vLaborHoursActuals (NOLOCK);
		-- WHERE StgCreatedDate > @LastProcessedDate;

        BEGIN TRY

            -- ================================================================================
            -- STEP 1.
            -- 
            -- Ascertain by what criteria we are splitting this batch load by, and build a loop
            -- ================================================================================
            -- DECLARE @BatchByFiscalWeekEndDate TABLE(FiscalWeekEndDate DATE);
			DROP TABLE IF EXISTS #OriginalProcessingRange;
			CREATE TABLE #OriginalProcessingRange ( [BatchIdentifier] SQL_VARIANT );

            DECLARE @SourceFiscalDate TABLE(FiscalDate DATE);
         
		 ---IF it is Sunday load all the weekendindate from filw\e
		 IF DateName(WEEKDAY, @RefDate) = 'Saturday'
                BEGIN
                    INSERT INTO @SourceFiscalDate(FiscalDate)
                           SELECT DISTINCT 
                                  CAST(WeekEndingDate AS DATE)
                           FROM dbo.vLaborHoursActuals(NOLOCK)
                           WHERE StgCreatedDate > @LastProcessedDate;
                    INSERT INTO #OriginalProcessingRange ( [BatchIdentifier] ) --@BatchByFiscalWeekEndDate(FiscalWeekEndDate)
                           SELECT DISTINCT 
                                  FiscalWeekEndDate
                           FROM BING_EDW.dbo.DimDate(NOLOCK) d
                                JOIN @SourceFiscalDate s ON d.FullDate = s.FiscalDate;
            END;
                ELSE -- If it is weekday just load max weekending date from file
                BEGIN
                    INSERT INTO @SourceFiscalDate(FiscalDate)
                           SELECT DISTINCT 
                                  MAX(CAST(WeekEndingDate AS DATE))
                           FROM dbo.vLaborHoursActuals(NOLOCK)
                           WHERE StgCreatedDate > @LastProcessedDate;
                    INSERT INTO #OriginalProcessingRange ( [BatchIdentifier] ) --@BatchByFiscalWeekEndDate(FiscalWeekEndDate)
                           SELECT DISTINCT 
                                  FiscalWeekEndDate
                           FROM BING_EDW.dbo.DimDate(NOLOCK) d
                                JOIN @SourceFiscalDate s ON d.FullDate = s.FiscalDate;
            END;

			DROP TABLE IF EXISTS #FinalProcessingRange;
			CREATE TABLE #FinalProcessingRange ( [FiscalWeekEndDate] DATE );

			EXEC [BING_EDW].[dbo].[spEDW_StagingToEDW_GetProcessingRange]
				@SourceName = @SourceName,
				@TaskName = @TaskName,
				@MinProcessingDate = @MinProcessingDate,
				@LastProcessedDate = @LastProcessedDate,
				@DimDateColumnName = 'FiscalWeekEndDate',
				@IsOverrideBatch = @IsOverrideBatch OUTPUT,
				@AuditID = @AuditId OUTPUT,
				@Debug = 1;

            -- ================================================================================
            -- STEP 2.
            -- 
            -- Loop through each Fiscal Year, and execute the ETL Upsert Stored Proc
            -- ================================================================================
            DECLARE csr_fact_laborhour_fisc_weekend CURSOR
            FOR SELECT FiscalWeekEndDate
                FROM #FinalProcessingRange --@BatchByFiscalWeekEndDate
                ORDER BY FiscalWeekEndDate;             
            --
            -- Use cursor to loop through the values we have chosen to split this batch by
            --
            OPEN csr_fact_laborhour_fisc_weekend;
            FETCH NEXT FROM csr_fact_laborhour_fisc_weekend INTO @FiscalWeekEndDate;
            WHILE @@FETCH_STATUS = 0
                BEGIN
                    SELECT @DebugMsg = @ProcName + ' : ' + CONVERT(NVARCHAR(20), GETDATE()) + ' - Processing data for Fiscal Week End Date ' + CONVERT(VARCHAR(10), @FiscalWeekEndDate);
                    PRINT @DebugMsg;
                    -- ================================================================================
                    -- Execute the main StagingToEDW stored proc to load data for the given batch
                    -- ================================================================================
                    EXEC dbo.spMISC_StagingToEDW_FactLaborHourActual 
                         @AuditId = @AuditId, 
                         @FiscalWeekEndDate = @FiscalWeekEndDate,
						 @RefDate = @RefDate;
                    FETCH NEXT FROM csr_fact_laborhour_fisc_weekend INTO @FiscalWeekEndDate;
                END;
            CLOSE csr_fact_laborhour_fisc_weekend;
            DEALLOCATE csr_fact_laborhour_fisc_weekend;

			-- ================================================================================
            -- STEP 3
            --
            -- Update the missing paycode
			-- ================================================================================

			EXEC BING_EDW.dbo.SpPopulateMissingPaycode

            -- STEP 4.
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
					EXEC dbo.spMISC_StagingEDWETLBatchControl @TaskName;
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
