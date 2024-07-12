CREATE PROCEDURE [dbo].[spMISC_StagingToEDW_FactLaborHourACQ]
(@EDWRunDateTime    DATETIME2 = NULL,
 @SourceSystem VARCHAR(3),
 @DebugMode INT = NULL
)
AS
    -- ================================================================================
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date          Modified By        Comments
    -- ----          -----------        --------
    -- 03/02/2020    banandesi          BI- 3537 Created the proc to load Acquired Centers Labor Hour data from staging table
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;
	    --
	    -- Housekeeping Variables
	    -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
	     DECLARE @TaskName NVARCHAR(100)='FactLaborHour - ' + @SourceSystem;

	    --
	    -- ETL status Variables
	    --
		 DECLARE @AuditId BIGINT;
         DECLARE @RowCount INT;
         DECLARE @Error INT;
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
	    -- Write to AuditLog that we are starting, and get the AuditId we use for each batch we load		  
	    --
         EXEC [BING_EDW].[dbo].[spEDWBeginAuditLog]
			  @SourceName = @TaskName,
              @AuditId = @AuditId OUTPUT;

	    --
	    -- Determine how far back in history we have extract data for
	    --

		 DECLARE @LastProcessedDate DATETIME=
         (
             SELECT LastProcessedDate
             FROM MISC_Staging..EDWETLBatchControl(NOLOCK)
             WHERE EventName = @TaskName
         );
         IF @LastProcessedDate IS NULL
             SET @LastProcessedDate = '19000101'; -- If no previous load logged in EDWETLBatchControl, assume we bring in everything
	    --
         IF @DebugMode = 1
             SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Starting.';
         PRINT @DebugMsg;
	    -- --------------------------------------------------------------------------------
	    -- Extract from Source, Upserts and Deletes contained in a single transaction.  
	    --	 Rollback on error
	    -- --------------------------------------------------------------------------------
         BEGIN TRY
             BEGIN TRANSACTION;
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Beginning transaction.';
             PRINT @DebugMsg;
		   
		   -- Create temporary landing #table

		 CREATE TABLE #FactLaborHour(
	[EmployeeNumber] [varchar](50) NOT NULL,
	[CostCenterKey] [int] NOT NULL,
	[OrgKey] [int] NOT NULL,
	[DateKey] [int] NOT NULL,
	[AccountSubaccountKey] [int] NOT NULL,
	[IsTSEF] [varchar](10) NOT NULL,
	[PayBasisKey] [int] NOT NULL,
	[DataScenarioKey] [int] NOT NULL,
	[Hours] [decimal](15, 6) NOT NULL,
	[AdjustmentFactor] [numeric](19, 4) NOT NULL,
	[DollarExtension] [decimal](15, 6) NOT NULL,
	[SourceSystem] [varchar](20) NULL,
	[EDWCreatedDate] [datetime2](7) NOT NULL
         )
		   -- Populate the Landing table from Source

		 INSERT INTO #FactLaborHour
			(
	[EmployeeNumber],
	[CostCenterKey],
	[OrgKey],
	[DateKey],
	[AccountSubaccountKey],
	[IsTSEF],
	[PayBasisKey],
	[DataScenarioKey],
	[Hours],
	[AdjustmentFactor],
	[DollarExtension],
	[SourceSystem],
	[EDWCreatedDate]
			)
            SELECT 
				  '-2' AS EmployeeNumber
                  ,COALESCE(dc.CostCenterKey, -1) AS CostCenterKey
                  ,COALESCE(do.OrgKey, -1) AS OrgKey
                  ,COALESCE(dd.DateKey, -1) AS DateKey
                  ,COALESCE(da.AccountSubaccountKey, -1) AS AccountSubaccountKey
				  ,'Not TSEF' AS IsTSEF
                  ,-2 AS PayBasisKey
                  ,1 AS DataScenarioKey
                  ,Stg.[Stats] AS Hours
				  ,0 AS [AdjustmentFactor]
				  ,stg.USD AS [DollarExtension] 
                  ,@SourceSystem AS SourceSystem
                  ,GETDATE() AS EDWCreatedDate
		FROM AcquiredDollarandStats stg
		LEFT JOIN BING_EDW.dbo.DimDate dd
			ON stg.FiscalWeekEndDate = dd.FullDate
		LEFT JOIN BING_EDW.dbo.DimOrganization do
			ON stg.CostCenterNumber = do.CostCenterNumber
			AND do.EDWEndDate IS NULL
		LEFT JOIN BING_EDW.dbo.DimCostCenter dc
			ON stg.CostCenterNumber = dc.CostCenterNumber
			AND dc.EDWEndDate IS NULL
		LEFT JOIN BING_EDW.dbo.DimAccountSubaccount da 
			ON stg.AccountID = da.AccountID
			AND stg.SubaccountID = da.SubaccountID
		WHERE stg.AccountID Between '5000' AND '5999'
           AND stg.StgModifiedDate > @LastProcessedDate
			AND SourceSystem = @SourceSystem;

SELECT @SourceCount = COUNT(1) FROM #FactLaborHour;

		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Source.';
             PRINT @DebugMsg;

		   -- Delete data for the given Fiscal Week
		   --
             DELETE BING_EDW.dbo.FactLaborHour
			 FROM BING_EDW.dbo.FactLaborHour T
			 JOIN #FactLaborHour S ON (T.DateKey = S.DateKey AND T.CostCenterKey = S.CostCenterKey 
											AND T.OrgKey = S.OrgKey AND T.AccountSubaccountKey = S.AccountSubaccountKey 
											AND T.SourceSystem = S.SourceSystem)
             SELECT @DeleteCount = @@ROWCOUNT;
		   -- Debug output progress
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Deleted '+CONVERT(NVARCHAR(20), @DeleteCount)+' rows from Target.';
                     PRINT @DebugMsg;
             END;
		   --
		   -- Insert the data for the Fiscal Week
		   --			 			
				INSERT INTO BING_EDW.dbo.FactLaborHour
				SELECT *
                    FROM #FactLaborHour;
             SELECT @InsertCount = @@ROWCOUNT;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(NVARCHAR(20), @InsertCount)+' rows into Target.';
                     PRINT @DebugMsg;
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Updated '+CONVERT(NVARCHAR(20), @UpdateCount)+' rows into Target.';
                     PRINT @DebugMsg;
             END;  
		  -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Committing transaction.';
             PRINT @DebugMsg;
		   
		   --
		   -- Commit the successful transaction 
		   --
             COMMIT TRANSACTION;

		   --
		   -- Drop the temp table
		   --
             DROP TABLE #FactLaborHour;

		-- Write the successful load to EDWAuditLog

            EXEC [BING_EDW].[dbo].[spEDWEndAuditLog]
                 @InsertCount = @InsertCount,
                 @UpdateCount = @UpdateCount,
                 @DeleteCount = @DeleteCount,
                 @SourceCount = @SourceCount,
                 @AuditId = @AuditId;

		-- Also write the successful load to EDWETLBatchControl, so we know how far back in history
		--     we have to go in the next ETL run

            EXEC dbo.spMISC_StagingEDWETLBatchControl
                 @TaskName;
			
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Completing successfully.';
             PRINT @DebugMsg;
         END TRY
	    -- Catch, and throw the error back to the calling procedure or client
	    --
         BEGIN CATCH
		         EXEC [BING_EDW].[dbo].[spEDWErrorAuditLog] 
                 @AuditId = @AuditId;

             DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
             SELECT @ErrMsg = 'Sub-procedure '+@ProcName+' - '+ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;