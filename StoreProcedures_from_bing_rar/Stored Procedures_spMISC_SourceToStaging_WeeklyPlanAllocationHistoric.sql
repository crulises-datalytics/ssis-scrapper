CREATE PROCEDURE [dbo].[spMISC_SourceToStaging_WeeklyPlanAllocationHistoric] (
   @EDWRunDateTime datetime2 = NULL
  ,@DebugMode      int       = NULL
)
AS
   -- ================================================================================
   -- 
   -- Stored Procedure:   spMISC_SourceToStaging_WeeklyPlanAllocationHistoric
   --
   -- Purpose:            Performs the Insert / Update / Delete ETL process for
   --                         the WeeklyPlanAllocationHistoricLanding table from Source 
   --                         to the staging table dbo.WeeklyPlanAllocationCurrent.
   --
   --                     We use the staging table here rather than simply calling the UDF in our
   --                         StagingToEDW - StagingTransform procs primarilly for performance reasons
   --                         (and to provide a more intuitive history for Snapshots).
   --
   -- Parameters:         @EDWRunDateTime - Used to populate any 'current date' fields, rather than
   --                         making numerous GETDATE() calls  
   --                     @DebugMode - Used just for development & debug purposes,
   --                         outputting helpful info back to the caller.  Not
   --                         required for Production, and does not affect any
   --                         core logic.			   
   --
   -- Usage:              EXEC dbo.spMISC_SourceToStaging_WeeklyPlanAllocationHistoric @DebugMode = 1	
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
   --  10/15/18     anmorales          BNG-3746 - Staging - Create ETL process to load 
   --                                     WeeklyPlanAllocationHistoric table in 
   --                                     MISC_Staging
   --			 
   -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

         --
         -- Housekeeping Variables
         --
         DECLARE @ProcName nvarchar(500) = OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg nvarchar(500);
         DECLARE @SourceName varchar(100) = 'WeeklyPlanAllocationHistoric';
        -- DECLARE @AuditId bigint;

         --
         -- ETL status Variables
         --
         DECLARE @RowCount int;
         DECLARE @Error int;
        -- DECLARE @SourceCount int = 0;
         DECLARE @InsertCount int = 0;
         DECLARE @UpdateCount int = 0;
         DECLARE @DeleteCount int = 0;
         DECLARE @tblMrgeActions_SCD2 TABLE (
            [MergeAction] varchar(250) NOT NULL
);

         --
         -- If we do not get an @EDWRunDateTime input, set to current date
         --
         IF @EDWRunDateTime IS NULL
             SET @EDWRunDateTime = GETDATE();
         IF @DebugMode = 1
             SELECT
                    @DebugMsg = @ProcName+' : '+CONVERT(nvarchar(20),GETDATE())+' - Starting.';
         PRINT @DebugMsg;
         
         --
         -- Write to AuditLog we are starting
         --
         --EXEC [dbo].[spStagingBeginAuditLog]
         --       @SourceName = @SourceName
         --      ,@AuditId = @AuditId OUTPUT;
		 BEGIN TRY
			 BEGIN TRANSACTION;
			 --
			 -- Delete records with bad data
			 --
          WITH CTEWeeklyPlanAllocationHistoric AS (
				SELECT *,ROW_NUMBER() OVER(PARTITION BY FISCAL_YEAR, FISCAL_PERIOD_NBR, COST_CENTER_NBR, ACCOUNT_NBR, SUB_ACCOUNT_NBR, ACCT_TYPE ORDER BY DW_UPDATE_DATE DESC) AS DuplicateIdentifier FROM WeeklyPlanAllocationHistoricLanding
			 ), CTEFiscalPeriodWeek AS (
            SELECT FiscalYearNumber, FiscalPeriodOfYearNumber, COUNT(DISTINCT FiscalWeekSequenceNumber) AS FiscalWeeksInPeriod FROM BING_EDW.dbo.DimDate GROUP BY FiscalYearNumber, FiscalPeriodOfYearNumber
          )
			 DELETE src
			    OUTPUT
					    deleted.PERIOD_END_DATE_KEY
				      ,deleted.CATEGORY
				      ,deleted.FISCAL_YEAR
				      ,deleted.FISCAL_PERIOD_NBR
				      ,deleted.COST_CENTER_NBR
				      ,deleted.ACCOUNT_NBR
				      ,deleted.SUB_ACCOUNT_NBR
				      ,deleted.ACCT_TYPE
				      ,deleted.WK01
				      ,deleted.WK02
				      ,deleted.WK03
				      ,deleted.WK04
				      ,deleted.WK05
				      ,deleted.WK06
				      ,deleted.DW_LOAD_BY
				      ,deleted.DW_LOAD_DATE
				      ,deleted.DW_UPDATE_BY
				      ,deleted.DW_UPDATE_DATE
				      ,STUFF(CONCAT(CASE WHEN deleted.DuplicateIdentifier > 1 THEN '; Duplicate record'
								    END,
								    CASE WHEN LEN(ISNULL(deleted.ACCOUNT_NBR,'')) <> 4
										    OR TRY_CONVERT(int, deleted.ACCOUNT_NBR) IS NULL 
									   THEN '; AccountID not 4 digits'
								    END,
								    CASE WHEN LEN(ISNULL(deleted.SUB_ACCOUNT_NBR,'')) <> 6
										    OR TRY_CONVERT(int, deleted.SUB_ACCOUNT_NBR) IS NULL 
									   THEN '; SubaccountID not 6 digits'
								    END,
								    CASE WHEN LEN(ISNULL(deleted.COST_CENTER_NBR,'')) <> 6
										    OR TRY_CONVERT(int, deleted.COST_CENTER_NBR) IS NULL 
									   THEN '; CostCenterNumber not 6 digits'
								    END,
								    CASE WHEN wk.[WK01] NOT BETWEEN 0 AND 1 
									   THEN '; Week 1 not between 0 and 1'
								    END,
								    CASE WHEN wk.[WK02] NOT BETWEEN 0 AND 1 
									   THEN '; Week 2 not between 0 and 1'
								    END,
								    CASE WHEN wk.[WK03] NOT BETWEEN 0 AND 1 
									   THEN '; Week 3 not between 0 and 1'
								    END,
								    CASE WHEN wk.[WK04] NOT BETWEEN 0 AND 1 
									   THEN '; Week 4 not between 0 and 1'
								    END,
								    CASE WHEN wk.[WK05] NOT BETWEEN 0 AND 1 
									   THEN '; Week 5 not between 0 and 1'
								    END,
								    CASE WHEN wk.[WK06] NOT BETWEEN 0 AND 1 
									   THEN '; Week 6 not between 0 and 1'
								    END,
								    CASE WHEN wk.[WK01] + wk.[WK02] + wk.[WK03] + wk.[WK04] + wk.[WK05] + wk.[WK06] NOT BETWEEN 0.999 AND 1.001
									   THEN '; Weeks equal to ' + FORMAT(wk.[WK01] + wk.[WK02] + wk.[WK03] + wk.[WK04] + wk.[WK05] + wk.[WK06],'0.####')
								    END
						   ),1,2,'')
					   INTO WeeklyPlanAllocationHistoricBadRecord (
					   PERIOD_END_DATE_KEY
				      ,CATEGORY
				      ,FISCAL_YEAR
				      ,FISCAL_PERIOD_NBR
				      ,COST_CENTER_NBR
				      ,ACCOUNT_NBR
				      ,SUB_ACCOUNT_NBR
				      ,ACCT_TYPE
				      ,WK01
				      ,WK02
				      ,WK03
				      ,WK04
				      ,WK05
				      ,WK06
				      ,DW_LOAD_BY
				      ,DW_LOAD_DATE
				      ,DW_UPDATE_BY
				      ,DW_UPDATE_DATE
				      ,RejectReasons)
             FROM CTEWeeklyPlanAllocationHistoric AS src
               JOIN CTEFiscalPeriodWeek AS fwp ON fwp.FiscalYearNumber = src.FISCAL_YEAR AND fwp.FiscalPeriodOfYearNumber = src.FISCAL_PERIOD_NBR
               CROSS APPLY (VALUES (
                   ISNULL(CASE WHEN fwp.FiscalWeeksInPeriod >= 1 THEN TRY_CONVERT(decimal(18,6), src.[WK01]) END, 0)
                  ,ISNULL(CASE WHEN fwp.FiscalWeeksInPeriod >= 2 THEN TRY_CONVERT(decimal(18,6), src.[WK02]) END, 0)
                  ,ISNULL(CASE WHEN fwp.FiscalWeeksInPeriod >= 3 THEN TRY_CONVERT(decimal(18,6), src.[WK03]) END, 0)
                  ,ISNULL(CASE WHEN fwp.FiscalWeeksInPeriod >= 4 THEN TRY_CONVERT(decimal(18,6), src.[WK04]) END, 0)
                  ,ISNULL(CASE WHEN fwp.FiscalWeeksInPeriod >= 5 THEN TRY_CONVERT(decimal(18,6), src.[WK05]) END, 0)
                  ,ISNULL(CASE WHEN fwp.FiscalWeeksInPeriod >= 6 THEN TRY_CONVERT(decimal(18,6), src.[WK06]) END, 0)
               )) AS wk(WK01, WK02, WK03, WK04, WK05, WK06)
				   WHERE LEN(ISNULL(src.ACCOUNT_NBR,'')) <> 4
					   OR TRY_CONVERT(int, src.ACCOUNT_NBR) IS NULL
					   OR LEN(ISNULL(src.SUB_ACCOUNT_NBR,'')) <> 6
					   OR TRY_CONVERT(int, src.SUB_ACCOUNT_NBR) IS NULL
					   OR LEN(ISNULL(src.COST_CENTER_NBR,'')) <> 6
				      OR TRY_CONVERT(int, src.COST_CENTER_NBR) IS NULL
					   OR wk.[WK01] NOT BETWEEN 0 AND 1
					   OR wk.[WK02] NOT BETWEEN 0 AND 1
					   OR wk.[WK03] NOT BETWEEN 0 AND 1
					   OR wk.[WK04] NOT BETWEEN 0 AND 1
					   OR wk.[WK05] NOT BETWEEN 0 AND 1
					   OR wk.[WK06] NOT BETWEEN 0 AND 1
					   OR wk.[WK01] + wk.[WK02] + wk.[WK03] + wk.[WK04] + wk.[WK05] + wk.[WK06] NOT BETWEEN 0.999 AND 1.001
					   OR src.DuplicateIdentifier > 1;

			 SET @DeleteCount = @@ROWCOUNT;
			 IF @DebugMode = 1
			 BEGIN
				 SELECT
						@DebugMsg = @ProcName+' : '+CONVERT(nvarchar(20),GETDATE())+' - Deleted '+CONVERT(nvarchar(20),@DeleteCount)+' bad rows from Source.';
				 PRINT @DebugMsg;
			 END;

			 IF OBJECT_ID('tempdb..#WeeklyPlanAllocationCurrent') IS NOT NULL
				 DROP TABLE #WeeklyPlanAllocationCurrent;
			 SELECT
					unpvt.CostCenterNumber
				   ,unpvt.AccountID+'.'+unpvt.SubaccountID AS AccountSubaccountID
				   ,ccMast.COMPANY AS CompanyID
				   ,dd.FiscalWeekNumber
				   ,ccMast.COST_CENTER_TYPE AS CostCenterTypeID
				   ,unpvt.GLMetricTypeName
				   ,unpvt.AllocationPercentage
			 INTO
					#WeeklyPlanAllocationCurrent
				FROM (
					  SELECT
							 FISCAL_YEAR AS FiscalYearNumber
							,FISCAL_PERIOD_NBR AS FiscalPeriodOfYearNumber
							,COST_CENTER_NBR AS CostCenterNumber
							,ACCOUNT_NBR AS AccountID
							,SUB_ACCOUNT_NBR AS SubaccountID
							,CASE
								WHEN ACCT_TYPE = 'USD' THEN 'Dollars'
								ELSE 'Statistics'
							 END AS GLMetricTypeName
							,NULLIF(TRY_CONVERT( decimal(18,6),WK01),0) AS [1]
							,NULLIF(TRY_CONVERT( decimal(18,6),WK02),0) AS [2]
							,NULLIF(TRY_CONVERT( decimal(18,6),WK03),0) AS [3]
							,NULLIF(TRY_CONVERT( decimal(18,6),WK04),0) AS [4]
							,NULLIF(TRY_CONVERT( decimal(18,6),WK05),0) AS [5]
							,NULLIF(TRY_CONVERT( decimal(18,6),WK06),0) AS [6]
						 FROM [dbo].[WeeklyPlanAllocationHistoricLanding]
					 ) AS src UNPIVOT(AllocationPercentage FOR FiscalWeekOfPeriodNumber IN(
					[1]
				   ,[2]
				   ,[3]
				   ,[4]
				   ,[5]
				   ,[6])) AS unpvt
					 JOIN BING_EDW.dbo.DimDate AS dd ON dd.FiscalYearNumber = unpvt.FiscalYearNumber
														AND dd.FiscalPeriodOfYearNumber = unpvt.FiscalPeriodOfYearNumber
														AND dd.FiscalWeekOfPeriodNumber = unpvt.FiscalWeekOfPeriodNumber
														AND dd.FullDate = dd.FiscalWeekEndDate
					 LEFT JOIN GL_Staging.dbo.xxklcCenterMaster AS ccMast ON unpvt.CostCenterNumber = ccMast.COST_CENTER;

			 --
			 -- Delete records from Period if they don't have a match
			 --
			 DELETE t
				FROM dbo.WeeklyPlanAllocationCurrent AS t
					 LEFT JOIN #WeeklyPlanAllocationCurrent AS s ON t.CostCenterNumber = s.CostCenterNumber
																	AND t.AccountSubaccountID = s.AccountSubaccountID
																	AND t.GLMetricTypeName = s.GLMetricTypeName
																	AND t.FiscalWeekNumber = s.FiscalWeekNumber
				WHERE
					t.FiscalWeekNumber IN(SELECT FiscalWeekNumber FROM #WeeklyPlanAllocationCurrent)
					AND s.AllocationPercentage IS NULL;
			 SET @DeleteCount = @@ROWCOUNT;

			 MERGE dbo.WeeklyPlanAllocationCurrent AS t
			 USING #WeeklyPlanAllocationCurrent AS s
			 ON t.CostCenterNumber = s.CostCenterNumber
				AND t.AccountSubaccountID = s.AccountSubaccountID
				AND t.GLMetricTypeName = s.GLMetricTypeName
				AND t.FiscalWeekNumber = s.FiscalWeekNumber
				WHEN MATCHED AND (t.AllocationPercentage <> s.AllocationPercentage
								  OR t.CompanyID <> s.CompanyID
								  OR t.CostCenterTypeID <> s.CostCenterTypeID)
				   THEN UPDATE SET
					t.CompanyID = s.CompanyID
				   ,t.CostCenterTypeID = s.CostCenterTypeID
				   ,t.AllocationPercentage = s.AllocationPercentage
				WHEN NOT MATCHED
				   THEN
				   INSERT(
					CostCenterNumber
				   ,AccountSubaccountID
				   ,CompanyID
				   ,CostCenterTypeID
				   ,GLMetricTypeName
				   ,FiscalWeekNumber
				   ,AllocationPercentage)
				   VALUES (
					s.CostCenterNumber
				   ,s.AccountSubaccountID
				   ,s.CompanyID
				   ,s.CostCenterTypeID
				   ,s.GLMetricTypeName
				   ,s.FiscalWeekNumber
				   ,s.AllocationPercentage)
 			-- We need to get the details of the records we updated, so we can insert a further row for them as the current row.      
			 OUTPUT
					$action
					INTO @tblMrgeActions_SCD2;
			 SELECT
					@InsertCount = SUM(Inserted)
				   ,@UpdateCount = SUM(Updated)
				FROM ( 
							  -- Count the number of inserts 
					 SELECT
							COUNT(*) AS Inserted
						   ,0 AS Updated
						FROM @tblMrgeActions_SCD2
						WHERE MergeAction = 'INSERT'
					 UNION ALL 
							   -- Count the number of updates
					 SELECT
							0 AS Inserted
						   ,COUNT(*) AS Updated
						FROM @tblMrgeActions_SCD2
						WHERE MergeAction = 'UPDATE'
					 ) AS merge_actions;
			 IF @DebugMode = 1
			 BEGIN
				 SELECT
						@DebugMsg = @ProcName+' : '+CONVERT(nvarchar(20),GETDATE())+' - Deleted '+CONVERT(nvarchar(20),@DeleteCount)+' rows from Target.';
				 PRINT @DebugMsg;
				 SELECT
						@DebugMsg = @ProcName+' : '+CONVERT(nvarchar(20),GETDATE())+' - Updated '+CONVERT(nvarchar(20),@UpdateCount)+' rows into Target.';
				 PRINT @DebugMsg;
				 SELECT
						@DebugMsg = @ProcName+' : '+CONVERT(nvarchar(20),GETDATE())+' - Inserted '+CONVERT(nvarchar(20),@InsertCount)+' rows into Target.';
				 PRINT @DebugMsg;
			 END;     

		  
						 -- Debug output progress
			 IF @DebugMode = 1
				 SELECT
						@DebugMsg = @ProcName+' : '+CONVERT(nvarchar(20),GETDATE())+' - Committing transaction.';
			 PRINT @DebugMsg;
		   
					 --
					 -- Commit the successful transaction 
					 --
			 COMMIT TRANSACTION;



					 -- Debug output progress
			 IF @DebugMode = 1
				 SELECT
						@DebugMsg = @ProcName+' : '+CONVERT(nvarchar(20),GETDATE())+' - Completing successfully.';
			 PRINT @DebugMsg;
		 END TRY
		 BEGIN CATCH
	    				 -- Debug output progress
			 IF @DebugMode = 1
			 BEGIN
				 SELECT
						@DebugMsg = @ProcName+' : '+CONVERT(nvarchar(20),GETDATE())+' - Inserted '+CONVERT(nvarchar(20),@InsertCount)+' rows into Target.';
				 PRINT @DebugMsg;
			 END;

					 -- Rollback the transaction
			 ROLLBACK TRANSACTION;
					 --
					 -- Raiserror
					 --
			 DECLARE @ErrMsg      nvarchar(4000)
					,@ErrSeverity int;
			 SELECT
					@ErrMsg = ERROR_MESSAGE()
				   ,@ErrSeverity = ERROR_SEVERITY();
			 RAISERROR(@ErrMsg,@ErrSeverity,1);
		 END CATCH;
     END;