

CREATE PROCEDURE [dbo].[spMISC_StagingToEDW_FactNetRevenueACQ]
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
    -- 02/24/2020    hhebbalu          BI- 2076 Created the proc to load Acquired Centers Net Revenue data from staging table
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;
	    --
	    -- Housekeeping Variables
	    -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
	     DECLARE @TaskName NVARCHAR(100)='FactNetRevenue - ' + @SourceSystem;

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

		 CREATE TABLE #FactNetRevenue(
			[DateKey] [int] NOT NULL,
			[OrgKey] [int] NOT NULL,
			[LocationKey] [int] NOT NULL,
			[StudentKey] [int] NOT NULL,
			[SponsorKey] [int] NOT NULL,
			[TuitionAssistanceProviderKey] [int] NOT NULL,
			[CompanyKey] [int] NOT NULL,
			[CostCenterTypeKey] [int] NOT NULL,
			[CostCenterKey] [int] NOT NULL,
			[AccountSubaccountKey] [int] NOT NULL,
			[TransactionCodeKey] [int] NOT NULL,
			[TierKey] [int] NOT NULL,
			[ProgramKey] [int] NOT NULL,
			[SessionKey] [int] NOT NULL,
			[ScheduleWeekKey] [int] NOT NULL,
			[FeeTypeKey] [int] NOT NULL,
			[DiscountTypeKey] [int] NOT NULL,
			[InvoiceTypeKey] [int] NOT NULL,
			[CreditMemoTypeKey] [int] NOT NULL,
			[LifecycleStatusKey] [int] NOT NULL,
			[TransactionNumber] [varchar](50) NOT NULL,
			[NetRevenueAmount] [numeric](19, 4) NOT NULL,
			[SourceSystem] [varchar](3) NOT NULL,
			[EDWCreatedDate] [datetime2](7) NOT NULL,
			[Deleted] [datetime2](7) NULL
         )
		   -- Populate the Landing table from Source

		 INSERT INTO #FactNetRevenue
			(
				[DateKey]
			   ,[OrgKey]
			   ,[LocationKey]
			   ,[StudentKey]
			   ,[SponsorKey]
			   ,[TuitionAssistanceProviderKey]
			   ,[CompanyKey]
			   ,[CostCenterTypeKey]
			   ,[CostCenterKey]
			   ,[AccountSubaccountKey]
			   ,[TransactionCodeKey]
			   ,[TierKey]
			   ,[ProgramKey]
			   ,[SessionKey]
			   ,[ScheduleWeekKey]
			   ,[FeeTypeKey]
			   ,[DiscountTypeKey]
			   ,[InvoiceTypeKey]
			   ,[CreditMemoTypeKey]
			   ,[LifecycleStatusKey]
			   ,[TransactionNumber]
			   ,[NetRevenueAmount]
			   ,[SourceSystem]
			   ,[EDWCreatedDate]
			   ,[Deleted]
			)
            SELECT 
						COALESCE(DD.DateKey, -1) AS DateKey
					  , COALESCE(DO.OrgKey, -1) AS OrgKey
					  , -2 AS LocationKey
					  , -2 AS StudentKey
					  , -2 AS SponsorKey
					  , -2 AS TuitionAssistanceProviderKey
					  , -2 AS CompanyKey
					  , -2 AS CostCenterTypeKey
					  , COALESCE(DC.CostCenterKey, -1) AS CostCenterKey
					  , COALESCE(DA.AccountSubaccountKey, -1) AS AccountSubaccountKey
					  , -2 AS TransactionCodeKey
					  , -2 AS TierKey
					  , -2 AS ProgramKey  
					  , -2 AS SessionKey
					  , -2 AS ScheduleWeekKey
					  , -2 AS FeeTypeKey
					  , -2 AS DiscountTypeKey
					  , -2 AS InvoiceTypeKey
					  , -2 AS CreditMemoTypeKey
					  , -2 AS LifecycleStatusKey
					  , -2 AS TransactionNumber
					  ,  [USD] AS NetRevenueAmount
					  , @SourceSystem AS SourceSystem
					  , GETDATE() AS EDWCreatedDate
					  , NULL AS [Deleted]
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
		WHERE ((stg.AccountID BETWEEN '4000' AND '4999') OR stg.AccountID IN ('5300', '5305', '5310'))
            AND stg.StgModifiedDate > @LastProcessedDate
			AND SourceSystem = @SourceSystem;

SELECT @SourceCount = COUNT(1) FROM #FactNetRevenue;

		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Source.';
             PRINT @DebugMsg;

		   -- Delete data for the given Fiscal Week
		   --
             DELETE BING_EDW.dbo.FactNetRevenue
			 FROM BING_EDW.dbo.FactNetRevenue T
			 JOIN #FactNetRevenue S ON (T.DateKey = S.DateKey AND T.CostCenterKey = S.CostCenterKey 
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
				INSERT INTO BING_EDW.dbo.FactNetRevenue
				SELECT *
                    FROM #FactNetRevenue;
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
             DROP TABLE #FactNetRevenue;

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