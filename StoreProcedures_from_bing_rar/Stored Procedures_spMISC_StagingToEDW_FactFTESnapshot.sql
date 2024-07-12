CREATE PROCEDURE [dbo].[spMISC_StagingToEDW_FactFTESnapshot]
(@EDWRunDateTime    DATETIME2 = NULL, 
 @DebugMode         INT       = NULL,
@SourceSytem VARCHAR(100) ='RBW'
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spMISC_StagingToEDW_FactFTESnapshot
    --
    -- Purpose:            Performs the Insert  ETL process for
    --					  the FactFTESnapshot table from Staging to BING_EDW.
    --
    --                     Step 1: Delete the Fact table with source system data
    --                     Step 2: Perform the Insert required 
    --                          for this EDW table load
    --                     Step 3: Execute any automated tests associated with this EDW table load
    --                     Step 4: Output Source / Insert / Update / Delete counts to caller, 
    --                         commit the transaction, and tidy-up
    --
    -- Parameters:		   @EDWRunDateTime
    --                     @AuditId - This is the Audit tracking ID used both in the EDWAuditLog and EDWBatchLoadLog tables 
    --                     @DebugMode - Used just for development & debug purposes,
    --                         outputting helpful info back to the caller.  Not
    --                         required for Production, and does not affect any
    --                         core logic.
    --
    --
    -- Usage:              EXEC dbo.spMISC_StagingToEDW_FactFTESnapshot @EDWRunDateTime = '', @DebugMode = 1
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date		Modified By		Comments
    -- ----		-----------		--------
    --
    -- 2/20/2020   Banandesi        BNG-3538 - Intital version of the proc
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'FactFTESnapshot-'+@SourceSytem;
		 DECLARE @AuditId BIGINT;
		 

	    --
	    -- ETL status Variables
	    --
         DECLARE @RowCount INT;
         DECLARE @Error INT;
         DECLARE @SourceCount INT= 0;
         DECLARE @InsertCount INT= 0;
         DECLARE @UpdateCount INT= 0;
         DECLARE @DeleteCount INT= 0;

	    --
	    --
	    -- If we do not get an @EDWRunDateTime input, set to current date
	    --
         IF @EDWRunDateTime IS NULL
             SET @EDWRunDateTime = GETDATE(); 
	    --
         IF @DebugMode = 1
             SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Starting.';
         PRINT @DebugMsg;

		  --
	    -- Write to EDW AuditLog we are starting
	    --
         EXEC [BING_EDW].[dbo].[spEDWBeginAuditLog]
			  @SourceName = @SourceName,
              @AuditId = @AuditId OUTPUT; 

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
		   
		   -- Get how many rows were extracted from source 

             SELECT @SourceCount = COUNT(1)
             FROM [dbo].[AcquiredDollarandStats]
			 WHERE AccountID BETWEEN '4000' AND '4999';
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Source.';
             PRINT @DebugMsg;
		   
		   --
		   -- Delete whole data by Source Sytem
		   --
			
			 SELECT @DeleteCount = COUNT(1)
             FROM BING_EDW.dbo.FactFTESnapshot where SourceSystem=@SourceSytem;
             
			 DELETE BING_EDW.dbo.FactFTESnapshot where SourceSystem=@SourceSytem

		   -- Debug output progress
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Deleted '+CONVERT(NVARCHAR(20), @DeleteCount)+' rows from Target.';
                     PRINT @DebugMsg;
             END;
			 
		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Perform the Insert required for this EDW table load
		   --
		   -- For this Fact load, we do not update or merge - instead we delete and reload
		   --     the data 
		   --
		   -- ================================================================================
		   --
		   -- Insert the whole dataset
		   --
             INSERT INTO BING_EDW.dbo.FactFTESnapshot
             ( 
			  [DateKey],
              [OrgKey],
              [LocationKey],
              [StudentKey],
              [SponsorKey],
              [TuitionAssistanceProviderKey],
              [CompanyKey],
              [CostCenterTypeKey],
              [CostCenterKey],
              [AccountSubaccountKey],
              [TransactionCodeKey],
              [TierKey],
              [ProgramKey],
              [SessionKey],
              [ScheduleWeekKey],
              [ClassroomKey],
              [FeeTypeKey],
              [LifecycleStatusKey],
              [ReferenceID],
              [FTE],
              [SourceSystem],
              [EDWCreatedDate]
             )
             (
              SELECT COALESCE(dm_dt.DateKey, -1) AS DateKey,
                    COALESCE(dm_org.OrgKey, -1) AS OrgKey,
                    COALESCE(dm_loc.LocationKey, -1) AS LocationKey,
                    -2 AS StudentKey,
                    -2 AS SponsorKey,
                    -2 AS TuitionAssistanceProviderKey,
                    -2 AS CompanyKey,
                    -2 AS CostCenterTypeKey,
                    COALESCE(dm_cctr.CostCenterKey, -1) AS CostCenterKey,
                    COALESCE(dm_acc_sub.AccountSubaccountKey, -1) AS AccountSubaccountKey,
                    -2 AS TransactionCodeKey,
                    -2 AS TierKey,
                    -2 AS ProgramKey,
                    -2 AS SessionKey,
                    -2 AS ScheduleWeekKey,
                    -2 AS ClassroomKey,
                    -2 AS FeeTypeKey,
                    -2 AS LifecycleStatusKey,
				    CAST(fct_FTE.FiscalWeekEndDate+'-'+fct_FTE.CostCenterNumber+'-'+fct_FTE.AccountID+'-'+fct_FTE.SubAccountID AS VARCHAR(50)) AS ReferenceID,
                    CAST([Stats] AS NUMERIC(19, 4)) AS FTE,
                    @SourceSytem AS SourceSystem,
                    GETDATE() AS EDWCreatedDate
             FROM dbo.AcquiredDollarandStats fct_FTE
                  LEFT JOIN BING_EDW.dbo.DimDate dm_dt ON fct_FTE.FiscalWeekEndDate = dm_dt.FullDate
                  LEFT JOIN BING_EDW.dbo.DimCostCenter dm_cctr ON fct_FTE.CostCenterNumber = dm_cctr.CostCenterNumber
                                                                  AND dm_cctr.EDWEndDate IS NULL -- DimCostCenter is SCD2, so get the latest version
                  
                  LEFT JOIN BING_EDW.dbo.DimOrganization dm_org ON dm_cctr.CostCenterNumber = dm_org.CostCenterNumber
			                                                 AND dm_org.EDWEndDate IS NULL -- DimOrganization is SCD2, so get the latest version
                  LEFT JOIN BING_EDW.dbo.DimLocation dm_loc ON dm_org.DefaultLocationID = dm_loc.LocationID
                                                               AND dm_loc.EDWEndDate IS NULL -- DimLocation is SCD2, so get the latest version
                  LEFT JOIN BING_EDW.dbo.DimAccountSubaccount dm_acc_sub ON fct_FTE.AccountID = dm_acc_sub.AccountID
                                                                    AND fct_FTE.SubaccountID = dm_acc_sub.SubaccountID
			      WHERE fct_FTE.AccountID BETWEEN '4000' AND '4999' );
             SELECT @InsertCount = @@ROWCOUNT;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(NVARCHAR(20), @InsertCount)+' rows into Target.';
                     PRINT @DebugMsg;
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Updated '+CONVERT(NVARCHAR(20), @UpdateCount)+' rows into Target.';
                     PRINT @DebugMsg;
             END;             
		   

		   -- ================================================================================
		   --
		   -- S T E P   3.
		   --
		   -- Execute any automated tests associated with this EDW table load
		   --
		   -- ================================================================================


		   
		  
		  -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Committing transaction.';
             PRINT @DebugMsg;
		   
		   --
		   -- Commit the successful transaction 
		   --
             COMMIT TRANSACTION;


		   --
		   -- Write our successful run to the EDW AuditLog 
		   --

             EXEC [BING_EDW].[dbo].[spEDWEndAuditLog]
                  @InsertCount = @InsertCount,
                  @UpdateCount = @UpdateCount,
                  @DeleteCount = @DeleteCount,
                  @SourceCount = @SourceCount,
                  @AuditId = @AuditId;

		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Completing successfully.';
             PRINT @DebugMsg;
         END TRY
         BEGIN CATCH
	    	  -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Rolling back transaction.';
             PRINT @DebugMsg;
		   -- Rollback the transaction
             ROLLBACK TRANSACTION;
		   --
		   -- Write our failed run to the EDW AuditLog 
		   --
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