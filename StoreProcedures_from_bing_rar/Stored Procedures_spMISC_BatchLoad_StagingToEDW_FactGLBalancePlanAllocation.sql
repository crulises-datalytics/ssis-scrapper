CREATE PROCEDURE [dbo].[spMISC_BatchLoad_StagingToEDW_FactGLBalancePlanAllocation]
	@EDWRunDateTime DATETIME2 = NULL, 
	@DebugMode      INT       = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spMISC_BatchLoad_StagingToEDW_FactGLBalancePlanAllocation
    --
    -- Purpose:            Performs the Insert / Update / Delete ETL process for
    --                         the FactGLBalancePlanAllocation table from Staging to BING_EDW, loading
    --                         data in manageable batch sizes for larger datasets.  This
    --                         is to ensure we do not fill the log when performing 
    --                         inserts / updates on our larger tables
    --
    -- Parameters:		  @EDWRunDateTime
    --                     @DebugMode - Used just for development & debug purposes,
    --                         outputting helpful info back to the caller.  Not
    --                         required for Production, and does not affect any
    --                         core logic.
    --                     @DaysBack - For FactGLBalancePlanAllocation load we have to go back and potentially reporcess
    --                         data we have already ingested that might have changed in the source.  As it
    --                         is not possible to uniquely identify records to do a Merge, we are forced to 
    --                         delete and re-insert.  This parameter controls how far back we go to reprocess
    --                         (as of Nov 2017 we are going back 2 weeks from the LastProcessedDate)
    --
    --
    -- Usage:              EXEC dbo.spMISC_BatchLoad_StagingToEDW_FactGLBalancePlanAllocation @EDWRunDateTime = @EDWRunDateTime, @DebugMode = 1
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date				Modified By         Comments
    -- ----				-----------         --------
    --
    -- 11/03/17    		sburke				BNG-789 - Refactored EDW FactGLBalancePlanAllocation (LegacyDW Source) load
    -- 6/21/18			anmorales			BNG-2711 - EDW - StagingToEDW FactGLBalancePlanAllocation for 2018 - New Plan Allocation Process
    -- 05/24/19			hhebbalu			Removed the 14 days delete and reload logic and added the logic to load 
    --											all that is changed in the staging table since last run
    -- 08/14/2019		Adevabhakthuni		Added Logic to get the FiscalPeriod Numbers from WeeklyPlanAlloactio(Current) table  
	-- 08/20/2019		anmorales			Fixed Logic to get the FiscalPeriod Numbers from WeeklyPlanAlloactio(Current) table
	-- 08/09/2023		Suhas.De			BI-9051 - Changes for Target Reload
    -- ================================================================================
    BEGIN
        SET NOCOUNT ON;

        --
        -- Housekeeping Variables
        --
        DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
        DECLARE @DebugMsg NVARCHAR(500) = '';
        DECLARE @TaskName VARCHAR(100)= 'FactGLBalancePlanAllocation';
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

        --
        -- If we do not get an @EDWRunDateTime input, set to current date
        --
        IF @EDWRunDateTime IS NULL
            SET @EDWRunDateTime = GETDATE(); 
        --
        IF @DebugMode = 1
            SELECT @DebugMsg = @ProcName + ' : ' + CONVERT(NVARCHAR(20), GETDATE()) + ' - Starting.';
        RAISERROR(@DebugMsg, 10, 1) WITH NOWAIT;

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
            SET @LastProcessedDate = '20150104';   -- If no previous load logged in EDWETLBatchControl, assume we bring in everything. This process has a start date of Period 1 of Fiscal Year 2015.
		
		DECLARE @LastProcessedUTCDate DATETIME = SWITCHOFFSET(CONVERT(DATETIMEOFFSET(0), @LastProcessedDate),
                                                              CASE
                                                                  WHEN @LastProcessedDate BETWEEN Salesforce_Staging.dbo.fnGetDSTStart(YEAR(@LastProcessedDate)) AND Salesforce_Staging.dbo.fnGetDSTEnd(YEAR(@LastProcessedDate))
                                                                  THEN+7
                                                                  ELSE+8
                                                              END * 60);
        BEGIN TRY

            -- ================================================================================
            -- STEP 1.
            -- 
            -- Ascertain by what criteria we are splitting this batch load by, and build a loop
            -- ================================================================================
            -- DECLARE @BatchByFiscalPeriodNumber TABLE(FiscalPeriodNumber INT);
			DROP TABLE IF EXISTS #OriginalProcessingRange;
			CREATE TABLE #OriginalProcessingRange ( [BatchIdentifier] SQL_VARIANT );
            
			INSERT INTO #OriginalProcessingRange ( [BatchIdentifier] ) --@BatchByFiscalPeriodNumber(FiscalPeriodNumber)
            (

                SELECT period_id
                FROM GL_Staging.dbo.vGLBalances(NOLOCK) AS gl
					JOIN BING_EDW.dbo.DimDataScenario AS dm_ds ON dm_ds.GLActualFlag = gl.actual_flag
						AND COALESCE(dm_ds.GLBudgetVersionID, - 1) = COALESCE(gl.budget_version_id, - 1) 
						AND dm_ds.DataScenarioName = 'Plan'
                WHERE StgModifiedDate > @LastProcessedDate
				GROUP BY period_id
				UNION

                SELECT 
                       dm_dt.FiscalPeriodNumber
                FROM BING_EDW.dbo.vDimFiscalWeek AS dm_dt
					JOIN GL_Staging.dbo.vGLBalances(NOLOCK) pd ON pd.period_id = dm_dt.FiscalPeriodNumber -- Periodic data is required for any timeperiod
					JOIN BING_EDW.dbo.DimDataScenario AS dm_ds ON dm_ds.GLActualFlag = pd.actual_flag
						AND COALESCE(dm_ds.GLBudgetVersionID, - 1) = COALESCE(pd.budget_version_id, - 1) 
						AND dm_ds.DataScenarioName = 'Plan'
					JOIN [dbo].[WeeklyPlanAllocationCurrent](NOLOCK) wk ON wk.FiscalWeekNumber = dm_dt.FiscalWeekNumber -- 
                WHERE wk.StgUTCModifiedDate > @LastProcessedUTCDate
				GROUP BY dm_dt.FiscalPeriodNumber
            );

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
				@Debug = 1;

            -- ================================================================================
            -- STEP 2.
            -- 
            -- Loop through each Fiscal Year, and execute the ETL Upsert Stored Proc
            -- ================================================================================
            DECLARE csr_fact_glbal_plan_alloc_fisc_weekend CURSOR
            FOR SELECT FiscalPeriodNumber
                FROM #FinalProcessingRange --@BatchByFiscalPeriodNumber
                ORDER BY FiscalPeriodNumber;

            --
            -- Use cursor to loop through the values we have chosen to split this batch by
            --
            OPEN csr_fact_glbal_plan_alloc_fisc_weekend;
            FETCH NEXT FROM csr_fact_glbal_plan_alloc_fisc_weekend INTO @FiscalPeriodNumber;
            WHILE @@FETCH_STATUS = 0
                BEGIN
                    SELECT @DebugMsg = @ProcName + ' : ' + CONVERT(NVARCHAR(20), GETDATE()) + ' - Processing data for Fiscal Period ' + CONVERT(NVARCHAR(20), @FiscalPeriodNumber);
                    RAISERROR(@DebugMsg, 10, 1) WITH NOWAIT;
                    -- ================================================================================
                    -- Execute the main StagingToEDW stored proc to load data for the given batch
                    -- ================================================================================
                    EXEC dbo.spMISC_StagingToEDW_FactGLBalancePlanAllocation 
                         @AuditId = @AuditId, 
                         @FiscalPeriodNumber = @FiscalPeriodNumber, 
                         @DebugMode = @DebugMode;
                    FETCH NEXT FROM csr_fact_glbal_plan_alloc_fisc_weekend INTO @FiscalPeriodNumber;
                END;
            CLOSE csr_fact_glbal_plan_alloc_fisc_weekend;
            DEALLOCATE csr_fact_glbal_plan_alloc_fisc_weekend;

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
GO