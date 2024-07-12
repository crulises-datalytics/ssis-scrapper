CREATE PROCEDURE dbo.spCSS_BatchLoad_StagingToEDW_FactFTESnapshot
	@EDWRunDateTime DATETIME2 = NULL,
	@DebugMode      INT       = NULL,
	@DaysBack       INT       = 14 -- This is how many days back the process wil go from the LastProcessedDate (defaults to 2 weeks)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCSS_BatchLoad_StagingToEDW_FactFTESnapshot
    --
    -- Purpose:            Performs the Insert / Update / Delete ETL process for
    --                         the FactFTESnapshot table from Staging to BING_EDW, loading
    --                         data in manageable batch sizes for larger datasets.  This
    --                         is to ensure we do not fill the log when performing 
    --                         inserts / updates on our larger tables
    --
    -- Parameters:         @EDWRunDateTime
    --                     @DebugMode - Used just for development & debug purposes,
    --                         outputting helpful info back to the caller.  Not
    --                         required for Production, and does not affect any
    --                         core logic.
    --                     @DaysBack - For FactFTESnapshot load we have to go back and potentially reporcess
    --                         data we have already ingested that might have changed in the source.  As it
    --                         is not possible to uniquely identify records to do a Merge, we are forced to 
    --                         delete and re-insert.  This parameter controls how far back we go to reprocess
    --                         (as of Nov 2017 we are going back 2 weeks from the LastProcessedDate)
    --
    --
    --Usage:              EXEC dbo.spCSS_BatchLoad_StagingToEDW_FactFTESnapshot
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date				Modified By         Comments
    -- ----				-----------         --------
    --
    --  2/20/18			anmorales			BNG-249 - EDW FactFTESnapshot - CSS
    -- 08/09/2023		Suhas.De			BI-9051 - Changes for Target Reload
    -- ================================================================================
         BEGIN
             SET NOCOUNT ON;

             --
             -- Housekeeping Variables
             --
             DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
             DECLARE @DebugMsg NVARCHAR(500);
             DECLARE @TaskName VARCHAR(100)= 'FactFTESnapshot - CSS';
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
             DECLARE @FiscalWeekNumber INT;
             DECLARE @SourceCount INT= 0;
             DECLARE @InsertCount INT= 0;
             DECLARE @UpdateCount INT= 0;
             DECLARE @DeleteCount INT= 0;

             --
             -- If we do not get an @EDWRunDateTime input, set to current date
             --
             IF @EDWRunDateTime IS NULL
                 SET @EDWRunDateTime = GETDATE(); 

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
             -- Determine how far back in history we have to extract data for
             --
             DECLARE @StartingFiscalWeekNumber INT;
             DECLARE @LastProcessedDate DATE, @MinProcessingDate DATE;

			 SELECT @LastProcessedDate = LastProcessedDate, @MinProcessingDate = [MinProcessingDate]
			 FROM dbo.EDWETLBatchControl(NOLOCK)
			 WHERE EventName = @TaskName;

             IF @LastProcessedDate IS NULL
                 SET @LastProcessedDate = '19000101';   -- If no previous load logged in EDWETLBatchControl, assume we bring in everything
             
             SELECT @StartingFiscalWeekNumber = MIN(FiscalWeekNumber)
             FROM BING_EDW.dbo.DimDate D
             -- Go back a number of days from whatever the @LastProcessedDate is [SB - 11/02/17 The value is 14 days for now]
             WHERE FullDate >= DATEADD(DD, -@DaysBack, @LastProcessedDate)
                   AND FullDate <= @EDWRunDateTime;
             BEGIN TRY

                 -- ================================================================================
                 -- STEP 1.
                 -- 
                 -- Ascertain by what criteria we are splitting this batch load by, and build a loop
                 -- ================================================================================
                 -- DECLARE @BatchByFiscalWeekNumber TABLE(FiscalWeekNumber INT);
				 DROP TABLE IF EXISTS #OriginalProcessingRange;
				 CREATE TABLE #OriginalProcessingRange ( [BatchIdentifier] SQL_VARIANT );

                 DECLARE @CSSBalanceStartWeek INT=
                 (
                     SELECT TOP (1) [yr] * 100 + [week]
                     FROM dbo.vTransaction(NOLOCK)
                     WHERE [yr] IS NOT NULL
                           AND [week] IS NOT NULL
                     ORDER BY [yr],
                              [week]
                 ); -- Use @CSSGLBalanceStartDate as the earliest dated transaction in CSS.  No point going further back than this
                 INSERT INTO #OriginalProcessingRange([BatchIdentifier]) --@BatchByFiscalWeekNumber(FiscalWeekNumber)
                    SELECT DISTINCT
                            FiscalWeekNumber
                    FROM BING_EDW.dbo.DimDate
                    WHERE FiscalWeekNumber >= @StartingFiscalWeekNumber
                            AND FiscalWeekNumber >= @CSSBalanceStartWeek
                            AND FiscalWeekStartDate <= @EDWRunDateTime;

				 DROP TABLE IF EXISTS #FinalProcessingRange;
				 CREATE TABLE #FinalProcessingRange ( [FiscalWeekNumber] DATE );

				 EXEC [BING_EDW].[dbo].[spEDW_StagingToEDW_GetProcessingRange]
					@SourceName = @SourceName,
					@TaskName = @TaskName,
					@MinProcessingDate = @MinProcessingDate,
					@LastProcessedDate = @LastProcessedDate,
					@DimDateColumnName = 'FiscalWeekNumber',
					@IsOverrideBatch = @IsOverrideBatch OUTPUT,
					@AuditID = @AuditId OUTPUT,
					@Debug = 1;

                 -- ================================================================================
                 -- STEP 2.
                 -- 
                 -- Loop through each Fiscal Year, and execute the ETL Upsert Stored Proc
                 -- ================================================================================
                 DECLARE css_fact_ftesnapshot_fisc_week_no CURSOR
                 FOR SELECT FiscalWeekNumber
                     FROM #FinalProcessingRange --@BatchByFiscalWeekNumber
                     ORDER BY FiscalWeekNumber;
                 --
                 -- Use cursor to loop through the values we have chosen to split this batch by
                 --
                 OPEN css_fact_ftesnapshot_fisc_week_no;
                 FETCH NEXT FROM css_fact_ftesnapshot_fisc_week_no INTO @FiscalWeekNumber;
                 WHILE @@FETCH_STATUS = 0
                     BEGIN
                         SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Processing data for Fiscal Week '+CONVERT(VARCHAR(10), @FiscalWeekNumber);
                         PRINT @DebugMsg;
                         -- ================================================================================
                         -- Execute the main StagingToEDW stored proc to load data for the given batch
                         -- ================================================================================
                         EXEC dbo.spCSS_StagingToEDW_FactFTESnapshot
                              @AuditId = @AuditId,
                              @FiscalWeek = @FiscalWeekNumber,
                              @DebugMode = @DebugMode;
                         FETCH NEXT FROM css_fact_ftesnapshot_fisc_week_no INTO @FiscalWeekNumber;
                     END;
                 CLOSE css_fact_ftesnapshot_fisc_week_no;
                 DEALLOCATE css_fact_ftesnapshot_fisc_week_no;

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
GO