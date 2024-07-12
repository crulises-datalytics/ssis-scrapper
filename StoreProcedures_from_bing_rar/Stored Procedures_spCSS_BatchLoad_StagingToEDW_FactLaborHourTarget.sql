CREATE PROCEDURE [dbo].[spCSS_BatchLoad_StagingToEDW_FactLaborHourTarget]
	@EDWRunDateTime DATETIME2 = NULL,
	@DebugMode      INT       = NULL,
	@DaysBack       INT       = 14 -- This is how many days back the process wil go from the LastProcessedDate (defaults to 2 weeks)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCSS_BatchLoad_StagingToEDW_FactLaborHourTarget
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
    --                     @DaysBack - For FactLaborHour load we have to go back and potentially reporcess
    --                         data we have already ingested that might have changed in the source.  As it
    --                         is not possible to uniquely identify records to do a Merge, we are forced to 
    --                         delete and re-insert.  This parameter controls how far back we go to reprocess
    --                         (as of Nov 2017 we are going back 2 weeks from the LastProcessedDate)
    --
    --
    --Usage:              EXEC dbo.spCSS_BatchLoad_StagingToEDW_FactLaborHourTarget
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date				Modified By         Comments
    -- ----				-----------         --------
    --
    -- 07/10/18			Adevabhakthuni		BNG-3274 - Labor hour mapping - Write Logic that does look up for Fact table load
    -- 10/22/18			sburke				As of Oct 2018, the CSS source for Plan data has been superseded by a feed from ADP.			 
    --											However, the business have gone back and forth and Plan data solutions this year, so 
    --											we do not want to remove this code just yet.
    --											Keep this code in case they want to use CSS data again, and so add a step to grab
    --											the latest data from CSS.
	--08/21/2022		Vishal.Sawant		BI-4127 - Commented Csmlbrpr population code as this table not required now
	-- 08/09/2023		Suhas.De			BI-9051 - Changes for Target Reload
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @TaskName VARCHAR(100)= 'FactLaborHourTargetCSS';
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
		/*
         EXEC [BING_EDW].[dbo].[spEDWBeginAuditLog]
              @SourceName = @TaskName,
              @AuditId = @AuditId OUTPUT;
		*/

	    --
	    -- Determine how far back in history we have extract data for
	    --
         DECLARE @StartingFiscalWeekEndDate DATE;
		 DECLARE @LastProcessedDate DATE, @MinProcessingDate DATE;

		 SELECT @LastProcessedDate = LastProcessedDate, @MinProcessingDate = [MinProcessingDate]
		 FROM dbo.EDWETLBatchControl(NOLOCK)
		 WHERE EventName = @TaskName;

         IF @LastProcessedDate IS NULL
             SET @LastProcessedDate = '19000101';   -- If no previous load logged in EDWETLBatchControl, assume we bring in everything

         SELECT @StartingFiscalWeekEndDate = MIN(FiscalWeekEndDate)
         FROM BING_EDW.dbo.DimDate D
	    -- Go back a number of days from whatever the @LastProcessedDate is [SB - 11/02/17 The value is 14 days for now]
         WHERE FullDate >= DATEADD(DD, -@DaysBack, @LastProcessedDate)
               AND FullDate <= @EDWRunDateTime;

         BEGIN TRY
		   -- ================================================================================
		   -- STEP 0.
		   -- 
		   -- Grab the data from CSS Source
		   -- ================================================================================

           --  TRUNCATE TABLE [dbo].[Csmlbrpr];
           --  --
           --  INSERT INTO [dbo].[Csmlbrpr]
           --  ([ctr_no],
           --   [reg_no],
           --   [area_no],
           --   [tot_fte],
           --   [trgt_cc_hrs],
           --   [trgt_oper_hrs],
           --   [trgt_addnl_hrs],
           --   [trgt_lbr_prod],
           --   [act_cc_hrs],
           --   [act_oper_hrs],
           --   [act_cc_ot],
           --   [act_oper_ot],
           --   [trgt_hop_hrs],
           --   [act_hop_hrs],
           --   [train_hrs],
           --   [trgt_hom_hrs],
           --   [act_hom_hrs],
           --   [trgt_span_hrs],
           --   [act_span_hrs],
           --   [trgt_music_hrs],
           --   [act_music_hrs],
           --   [trgt_kma_hrs],
           --   [act_kma_hrs],
           --   [trgt_kpa_hrs],
           --   [act_kpa_hrs],
           --   [trgt_ksa_hrs],
           --   [act_ksa_hrs],
           --   [trgt_kra_hrs],
           --   [act_kra_hrs],
           --   [trgt_kmus_hrs],
           --   [act_kmus_hrs],
           --   [trgt_tut_hrs],
           --   [act_tut_hrs],
           --   [wk_end_date],
           --   [fiscal_yr],
           --   [period],
           --   [cc_var],
           --   [oper_var],
           --   [tot_act_hrs],
           --   [tot_trgt_hrs],
           --   [tot_hrs_var],
           --   [tot_pct_diff],
           --   [act_lbr_prod],
           --   [lbr_prod_var],
           --   [tot_act_ot],
           --   [StgCreatedDate],
           --   [StgCreatedBy],
           --   [StgModifiedDate],
           --   [StgModifiedBy],
           --   [Deleted]
           --  )
           --         SELECT css.*,
           --                GETDATE(),
           --                SUSER_NAME(),
           --                GETDATE(),
           --                SUSER_NAME(),
           --                NULL
           --         FROM OPENQUERY(cssrptd, 'SELECT ctr_no
           --,reg_no
           --,area_no
           --,tot_fte
           --,trgt_cc_hrs
           --,trgt_oper_hrs
           --,trgt_addnl_hrs
           --,trgt_lbr_prod
           --,act_cc_hrs
           --,act_oper_hrs
           --,act_cc_ot
           --,act_oper_ot
           --,trgt_hop_hrs
           --,act_hop_hrs
           --,train_hrs
           --,trgt_hom_hrs
           --,act_hom_hrs
           --,trgt_span_hrs
           --,act_span_hrs
           --,trgt_music_hrs
           --,act_music_hrs
           --,trgt_kma_hrs
           --,act_kma_hrs
           --,trgt_kpa_hrs
           --,act_kpa_hrs
           --,trgt_ksa_hrs
           --,act_ksa_hrs
           --,trgt_kra_hrs
           --,act_kra_hrs
           --,trgt_kmus_hrs
           --,act_kmus_hrs
           --,trgt_tut_hrs
           --,act_tut_hrs
           --,wk_end_date
           --,fiscal_yr
           --,period
           --,cc_var
           --,oper_var
           --,tot_act_hrs
           --,tot_trgt_hrs
           --,tot_hrs_var
           --,tot_pct_diff
           --,act_lbr_prod
           --,lbr_prod_var
           --,tot_act_ot FROM Csmlbrpr where fiscal_yr > 2010') css;


		   -- ================================================================================
		   -- STEP 1.
		   -- 
		   -- Ascertain by what criteria we are splitting this batch load by, and build a loop
		   -- ================================================================================
             -- DECLARE @BatchByFiscalWeekEndDate TABLE(FiscalWeekEndDate DATE);
			 DROP TABLE IF EXISTS #OriginalProcessingRange;
			 CREATE TABLE #OriginalProcessingRange ( [BatchIdentifier] SQL_VARIANT );

             DECLARE @CSSGLBalanceStartDate DATE=
             (
                 SELECT MIN(CAST(wk_end_date AS DATE))
                 FROM CSS_Staging.[dbo].[vLaborHoursTargetCSS]
             ); -- Use @CSSGLBalanceStartDate as the earliest dated transaction in CSS.  No point going further back than this
             INSERT INTO #OriginalProcessingRange ( [BatchIdentifier] ) --@BatchByFiscalWeekEndDate(FiscalWeekEndDate)
                SELECT DISTINCT
                        FiscalWeekEndDate
                FROM BING_EDW.dbo.DimDate
                WHERE FiscalWeekEndDate >= @StartingFiscalWeekEndDate
                        AND FiscalWeekEndDate >= @CSSGLBalanceStartDate
                        AND FiscalWeekStartDate <= @EDWRunDateTime;

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
             FOR
                 SELECT FiscalWeekEndDate
                 FROM #FinalProcessingRange --@BatchByFiscalWeekEndDate
                 ORDER BY FiscalWeekEndDate;             
		   --
		   -- Use cursor to loop through the values we have chosen to split this batch by
		   --
             OPEN csr_fact_laborhour_fisc_weekend;
             FETCH NEXT FROM csr_fact_laborhour_fisc_weekend INTO @FiscalWeekEndDate;
             WHILE @@FETCH_STATUS = 0
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Processing data for Fiscal Week End Date '+CONVERT(VARCHAR(10), @FiscalWeekEndDate);
                     PRINT @DebugMsg;
				 -- ================================================================================
				 -- Execute the main StagingToEDW stored proc to load data for the given batch
				 -- ================================================================================
                     EXEC dbo.spCSS_StagingToEDW_FactLaborHourTarget
                          @AuditId = @AuditId,
                          @FiscalWeekEndDate = @FiscalWeekEndDate;
                     FETCH NEXT FROM csr_fact_laborhour_fisc_weekend INTO @FiscalWeekEndDate;
                 END;
             CLOSE csr_fact_laborhour_fisc_weekend;
             DEALLOCATE csr_fact_laborhour_fisc_weekend;

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
					EXEC dbo.spCSS_StagingEDWETLBatchControl @TaskName;
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