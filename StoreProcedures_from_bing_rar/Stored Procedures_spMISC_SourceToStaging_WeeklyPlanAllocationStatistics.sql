CREATE PROCEDURE [dbo].[spMISC_SourceToStaging_WeeklyPlanAllocationStatistics]
(@AuditId        bigint    = NULL,
 @SourceCount    int       = NULL,
 @EDWRunDateTime datetime2 = NULL,
 @DebugMode      int       = NULL
)
AS
   -- ================================================================================
   -- 
   -- Stored Procedure:   spMISC_SourceToStaging_WeeklyPlanAllocationStatistics
   --
   -- Purpose:            Performs the Insert / Update / Delete ETL process for
   --                         the WeeklyPlanAllocationStatisticsLanding table from Source 
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
   -- Usage:              EXEC dbo.spMISC_SourceToStaging_WeeklyPlanAllocationStatistics @DebugMode = 1	
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
   --  6/21/18     anmorales          BNG-2711 - Staging - Create ETL process to load 
   --                                     WeeklyPlanAllocationStatistics table in 
   --                                     MISC_Staging
   --			 
   -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

         --
         -- Housekeeping Variables
         --
         DECLARE @ProcName nvarchar(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg nvarchar(500);
         DECLARE @SourceName varchar(100)= 'WeeklyPlanAllocationStatistics';
         -- DECLARE @AuditId bigint;
        
         --
         -- ETL status Variables
         --
         DECLARE @RowCount int;
         DECLARE @Error int;
        -- DECLARE @SourceCount int = 0;
         DECLARE @InsertCount int= 0;
         DECLARE @UpdateCount int= 0;
         DECLARE @DeleteCount int= 0;
         DECLARE @tblMrgeActions_SCD2 TABLE([MergeAction] varchar(250) NOT NULL);

         --
         -- If we do not get an @EDWRunDateTime input, set to current date
         --
         IF @EDWRunDateTime IS NULL
             SET @EDWRunDateTime = GETDATE();
         IF @DebugMode = 1
             SELECT @DebugMsg = @ProcName+' : '+CONVERT(nvarchar(20), GETDATE())+' - Starting.';
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
         -- Insert data     
         --        
             IF OBJECT_ID('tempdb..#WeeklyPlanAllocationCurrent') IS NOT NULL
                 DROP TABLE #WeeklyPlanAllocationCurrent;
             SELECT ISNULL(LEFT(misc_pa.CostCenter, 6), -1) AS CostCenterNumber,
                    ISNULL(LEFT(misc_pa.AccountSubAccount, 11), '0000.000000') AS AccountSubaccountID,
                    ISNULL(gl_ctr.COMPANY, '-1') AS CompanyID,
                    ISNULL(gl_ctr.COST_CENTER_TYPE, 'Unknown Cost Center Type') AS CostCenterTypeID,
                    dd.FiscalPeriodNumber,
                    ISNULL(dd.FiscalWeekNumber, -1) AS FiscalWeekNumber,
                    ISNULL(CONVERT(decimal(19, 6), CAST(misc_pa.Allocation AS float)), 0) AS AllocationPercentage,
                    'Statistics' AS GLMetricTypeName,
                    SUM(ISNULL(CONVERT(decimal(19, 6), CAST(misc_pa.Allocation AS float)), 0)) OVER(PARTITION BY dd.FiscalPeriodNumber,
                                                                                                                 LEFT(misc_pa.CostCenter, 6),
                                                                                                                 LEFT(misc_pa.AccountSubAccount, 11)) AS PeriodTotal
             INTO #WeeklyPlanAllocationCurrent
             FROM dbo.WeeklyPlanAllocationStatisticsLanding AS misc_pa
                  JOIN BING_EDW.dbo.DimDate AS dd ON dd.FiscalWeekNumber = CONVERT(int, '20'+RIGHT(misc_pa.FiscalYearWeek, 2)+RIGHT('00'+RTRIM(SUBSTRING(misc_pa.FiscalYearWeek, 6, 2)), 2))
                                                     AND dd.FiscalWeekEndDate = dd.FullDate
                  LEFT JOIN GL_Staging.dbo.xxklcCenterMaster AS gl_ctr ON LEFT(misc_pa.CostCenter, 6) = gl_ctr.COST_CENTER
             WHERE ISNULL(CONVERT(decimal(19, 6), CAST(misc_pa.Allocation AS float)), 0) <> 0;
                  
             ---- Exit the process
             --IF EXISTS (
             --    SELECT *
             --    FROM #WeeklyPlanAllocationCurrent
             --    WHERE PeriodTotal NOT BETWEEN 0.999 AND 1.001
             --          OR AllocationPercentage NOT BETWEEN 0 AND 1
             --)
                -- RAISERROR('Periods not between 99.9% and 100.1% or Allocation Percentages not between 0% and 100% found. Rejecting file.', 16, 1);
                 
                 
             -- Delete data
             WITH CTEPeriodicPlanAllocation
                  AS (
                  SELECT DISTINCT
                         CostCenterNumber,
                         AccountSubaccountID,
                         GLMetricTypeName,
                         FiscalPeriodNumber
                  FROM #WeeklyPlanAllocationCurrent)
                  DELETE t
                  FROM dbo.WeeklyPlanAllocationCurrent AS t
                       JOIN BING_EDW.dbo.DimDate AS dd ON dd.FiscalWeekNumber = t.FiscalWeekNumber
                                                          AND dd.FiscalWeekEndDate = dd.FullDate
                       JOIN CTEPeriodicPlanAllocation AS c ON c.CostCenterNumber = t.CostCenterNumber
                                                              AND c.AccountSubaccountID = t.AccountSubaccountID
                                                              AND c.GLMetricTypeName = t.GLMetricTypeName
                                                              AND c.FiscalPeriodNumber = dd.FiscalPeriodNumber
                       LEFT JOIN #WeeklyPlanAllocationCurrent AS s ON s.CostCenterNumber = t.CostCenterNumber
                                                                      AND s.AccountSubaccountID = t.AccountSubaccountID
                                                                      AND s.GLMetricTypeName = t.GLMetricTypeName
                                                                      AND s.FiscalPeriodNumber = dd.FiscalPeriodNumber
                                                                      AND s.FiscalWeekNumber = t.FiscalWeekNumber
                  WHERE s.AllocationPercentage IS NULL;
             SET @DeleteCount = @@ROWCOUNT;

             -- Merge all other data
             MERGE dbo.WeeklyPlanAllocationCurrent AS t
             USING #WeeklyPlanAllocationCurrent AS s
             ON t.CostCenterNumber = s.CostCenterNumber
                AND t.AccountSubaccountID = s.AccountSubaccountID
                AND t.GLMetricTypeName = s.GLMetricTypeName
                AND t.FiscalWeekNumber = s.FiscalWeekNumber
                 WHEN MATCHED AND(t.AllocationPercentage <> s.AllocationPercentage
                                  OR t.CompanyID <> s.CompanyID
                                  OR t.CostCenterTypeID <> s.CostCenterTypeID)
                 THEN UPDATE SET
                                 t.CompanyID = s.CompanyID,
                                 t.CostCenterTypeID = s.CostCenterTypeID,
                                 t.AllocationPercentage = s.AllocationPercentage
                 WHEN NOT MATCHED
                 THEN
                   INSERT(CostCenterNumber,
                          AccountSubaccountID,
                          CompanyID,
                          CostCenterTypeID,
                          GLMetricTypeName,
                          FiscalWeekNumber,
                          AllocationPercentage)
                   VALUES (
                    s.CostCenterNumber,
                    s.AccountSubaccountID,
                    s.CompanyID,
                    s.CostCenterTypeID,
                    s.GLMetricTypeName,
                    s.FiscalWeekNumber,
                    s.AllocationPercentage
                   )
             -- We need to get the details of the records we updated, so we can insert a further row for them as the current row.  
             OUTPUT $action
                    INTO @tblMrgeActions_SCD2;
	         --
             SELECT @InsertCount = SUM(Inserted),
                    @UpdateCount = SUM(Updated)
             FROM ( 
		                -- Count the number of inserts 
                      SELECT COUNT(*) AS Inserted,
                             0 AS Updated
                      FROM @tblMrgeActions_SCD2
                      WHERE MergeAction = 'INSERT'
                      UNION ALL 
                  	 -- Count the number of updates
                      SELECT 0 AS Inserted,
                             COUNT(*) AS Updated
                      FROM @tblMrgeActions_SCD2
                      WHERE MergeAction = 'UPDATE'
                  ) AS merge_actions;
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(nvarchar(20), GETDATE())+' - Inserted '+CONVERT(nvarchar(20), @InsertCount)+' rows into Target.';
                     PRINT @DebugMsg;
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(nvarchar(20), GETDATE())+' - Updated '+CONVERT(nvarchar(20), @UpdateCount)+' rows into Target.';
                     PRINT @DebugMsg;
                 END;     

		  
             -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(nvarchar(20), GETDATE())+' - Committing transaction.';
             PRINT @DebugMsg;
		   
		   --
		   -- Commit the successful transaction 
		   --
             COMMIT TRANSACTION;

		   --
		   -- Write our successful run to the AuditLog 
		   --
             EXEC [dbo].[spStagingEndAuditLog]
                  @InsertCount = @InsertCount,
                  @UpdateCount = @UpdateCount,
                  @DeleteCount = @DeleteCount,
                  @SourceCount = @SourceCount,
                  @AuditId = @AuditId;


		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(nvarchar(20), GETDATE())+' - Completing successfully.';
             PRINT @DebugMsg;
         END TRY
         BEGIN CATCH
	    	  -- Debug output progress
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(nvarchar(20), GETDATE())+' - Inserted '+CONVERT(nvarchar(20), @InsertCount)+' rows into Target.';
                     PRINT @DebugMsg;
                 END;

		   -- Rollback the transaction
             ROLLBACK TRANSACTION;
		   --
		   -- Write our failed run to the EDW AuditLog 
		   --
             EXEC [dbo].[spStagingErrorAuditLog]
                  @AuditId = @AuditId;
		   --
		   -- Raiserror
		   --
             DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int;
             SELECT @ErrMsg = ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;