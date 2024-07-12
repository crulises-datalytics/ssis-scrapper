/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spCMS_StagingToEDW_FactBilling'
)
    DROP PROCEDURE dbo.spCMS_StagingToEDW_FactBilling;
GO
*/
CREATE PROCEDURE dbo.spCMS_StagingToEDW_FactBilling
(@EDWRunDateTime   DATETIME2 = NULL,
 @AuditId          BIGINT    = NULL,
 @FiscalYearNumber INT       = NULL,
 @DebugMode        INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingToEDW_FactBilling
    --
    -- Purpose:            Performs the Insert / Update / Delete ETL process for
    --					  the FactBilling table from Staging to BING_EDW.
    --
    --                     Step 1: Create temporary landing #table
    --                     Step 2: Populate the Landing table from Source by calling
    --                         sub-procedure spCMS_StagingTransform_FactBilling, 
    --                         and create any helper indexes.
    --                         The sub-proc can be called with a flag to bring back data
    --                         for a given time period or all data (@ReturnAll), which can
    --                         be leveraged for batched loads for performance / log space concerns.
    --                     Step 3: Perform the Insert / Update (Merge) / deletes required 
    --                          for this EDW table load
    --                     Step 4: Execute any automated tests associated with this EDW table load
    --                     Step 5: Output Source / Insert / Update / Delete counts to caller, 
    --                         commit the transaction, and tidy-up
    --
    -- Parameters:		  @EDWRunDateTime
    --                     @FiscalYearNumber - Fiscal Year.  This proc has the option
    --                         of only returning data for a given Fiscal Year, which can 
    --                         be leveraged as part of a batched process (so we don't
    --                         extract and load a huge dataset in one go).
    --                     @DebugMode - Used just for development & debug purposes,
    --                         outputting helpful info back to the caller.  Not
    --                         required for Production, and does not affect any
    --                         core logic.
    --
    --
    -- Usage:              EXEC dbo.spCMS_StagingToEDW_FactAdjustment @EDWRunDateTime = @EDWRunDateTime, @DebugMode = 1
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date		Modified By		Comments
    -- ----		-----------		--------
    --
    -- 11/01/17    sburke              BNG-640.  Refactoring Fact table for Center Master
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @TaskName VARCHAR(100)= 'FactBilling';

	    --
	    -- For larger Fact (and Dimension) loads we will split the loads
	    --
         DECLARE @BatchSplitByName VARCHAR(50)= 'FiscalYearNumber';
         DECLARE @BatchSplitByValue INT= @FiscalYearNumber;

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
	    -- Merge statement action table variables
	    --
         DECLARE @tblMergeActions TABLE(MergeAction VARCHAR(20));
         DECLARE @tblDeleteActions TABLE(MergeAction VARCHAR(20));
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
	    -- If we do not get an @FiscalYearNumber input, set @ReturnAll to true, 
	    --     and bring back all data
	    --
         DECLARE @ReturnAll INT;
         IF @FiscalYearNumber IS NULL
             BEGIN
                 SET @ReturnAll = 1;  
			  
			  -- If we are returning all data we are not running in a loop, so AuditLog writing is done at this level
			  -- (If running in a loop, the calling proc takes care of the EDWAuditLog for us)
                 EXEC [BING_EDW].[dbo].[spEDWBeginAuditLog]
                      @SourceName = @TaskName,
                      @AuditId = @AuditId OUTPUT;
         END;

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
		   -- ================================================================================
		   --
		   -- S T E P   1.
		   --
		   -- Create temporary landing #table
		   --
		   -- ================================================================================
             CREATE TABLE #FactBillingUpsert
             ([ReferenceInvoiceID]           VARCHAR(50) NOT NULL,
              [InvoiceID]                    VARCHAR(50) NOT NULL,
              [BillingID]                    VARCHAR(50) NOT NULL,
              [BillingDateKey]               INT NOT NULL,
              [BillingStartDateKey]          INT NOT NULL,
              [BillingEndDateKey]            INT NOT NULL,
              [PaymentDueDateKey]            INT NOT NULL,
              [InvoiceVoidDateKey]           INT NOT NULL,
              [OrgKey]                       INT NOT NULL,
              [LocationKey]                  INT NOT NULL,
              [CompanyKey]                   INT NOT NULL,
              [CostCenterTypeKey]            INT NOT NULL,
              [CostCenterKey]                INT NOT NULL,
              [StudentKey]                   INT NOT NULL,
              [SponsorKey]                   INT NOT NULL,
              [TuitionAssistanceProviderKey] INT NOT NULL,
              [ProgramKey]                   INT NOT NULL,
              [SessionKey]                   INT NOT NULL,
              [ScheduleTypeKey]              INT NOT NULL,
              [TierKey]                      INT NOT NULL,
              [InvoiceTypeKey]               INT NOT NULL,
              [CreditMemoTypeKey]            INT NOT NULL,
              [FeeTypeKey]                   INT NOT NULL,
              [DiscountTypeKey]              INT NOT NULL,
              [BillingAmount]                NUMERIC(19, 4) NOT NULL,
              [EDWCreatedDate]               DATETIME2(7) NOT NULL,
              [Deleted]                      DATETIME2(7) NULL
             );
          

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table from Source, and create any helper indexes
		   --
		   -- ================================================================================
             INSERT INTO #FactBillingUpsert
             EXEC dbo.spCMS_StagingTransform_FactBilling
                  @EDWRunDateTime = @EDWRunDateTime,
                  @FiscalYearNumber = @FiscalYearNumber; -- If FiscalNumber IS NULL the sub-proc retruns all data;


		   -- Get how many rows were extracted from source 

             SELECT @SourceCount = COUNT(1)
             FROM #FactBillingUpsert;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Source.';
             PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
             CREATE NONCLUSTERED INDEX XAK1FactBillingUpsert ON #FactBillingUpsert
             ([BillingID] ASC, [BillingDateKey] ASC
             );

		   -- ================================================================================
		   --
		   -- S T E P   3.
		   --
		   -- Perform the Insert / Update (Merge) / deletes required for this EDW table load
		   --
		   -- ================================================================================
		   
		   --
		   -- Perform the Merge statement for insert / updates
		   --
             MERGE [BING_EDW].[dbo].[FactBilling] T
             USING #FactBillingUpsert S
             ON(S.BillingDateKey = T.BillingDateKey
                AND S.BillingID = T.BillingID)
                 WHEN MATCHED AND S.Deleted IS NULL
                                  AND (S.ReferenceInvoiceID <> T.ReferenceInvoiceID
                                       OR S.InvoiceID <> T.InvoiceID
                                       OR S.BillingStartDateKey <> T.BillingStartDateKey
                                       OR S.BillingEndDateKey <> T.BillingEndDateKey
                                       OR S.PaymentDueDateKey <> T.PaymentDueDateKey
                                       OR S.InvoiceVoidDateKey <> T.InvoiceVoidDateKey
                                       OR S.OrgKey <> T.OrgKey
                                       OR S.LocationKey <> T.LocationKey
                                       OR S.CompanyKey <> T.CompanyKey
                                       OR S.CostCenterTypeKey <> T.CostCenterTypeKey
                                       OR S.CostCenterKey <> T.CostCenterKey
                                       OR S.StudentKey <> T.StudentKey
                                       OR S.SponsorKey <> T.SponsorKey
                                       OR S.TuitionAssistanceProviderKey <> T.TuitionAssistanceProviderKey
                                       OR S.ProgramKey <> T.ProgramKey
                                       OR S.SessionKey <> T.SessionKey
                                       OR S.ScheduleTypeKey <> T.ScheduleTypeKey
                                       OR S.TierKey <> T.TierKey
                                       OR S.InvoiceTypeKey <> T.InvoiceTypeKey
                                       OR S.CreditMemoTypeKey <> T.CreditMemoTypeKey
                                       OR S.FeeTypeKey <> T.FeeTypeKey
                                       OR S.DiscountTypeKey <> T.DiscountTypeKey
                                       OR S.BillingAmount <> T.BillingAmount
                                       OR T.Deleted IS NOT NULL)
                 THEN UPDATE SET
                                 T.ReferenceInvoiceID = S.ReferenceInvoiceID,
                                 T.InvoiceID = S.InvoiceID,
                                 T.BillingStartDateKey = S.BillingStartDateKey,
                                 T.BillingEndDateKey = S.BillingEndDateKey,
                                 T.PaymentDueDateKey = S.PaymentDueDateKey,
                                 T.InvoiceVoidDateKey = S.InvoiceVoidDateKey,
                                 T.OrgKey = S.OrgKey,
                                 T.LocationKey = S.LocationKey,
                                 T.CompanyKey = S.CompanyKey,
                                 T.CostCenterTypeKey = S.CostCenterTypeKey,
                                 T.CostCenterKey = S.CostCenterKey,
                                 T.StudentKey = S.StudentKey,
                                 T.SponsorKey = S.SponsorKey,
                                 T.TuitionAssistanceProviderKey = S.TuitionAssistanceProviderKey,
                                 T.ProgramKey = S.ProgramKey,
                                 T.SessionKey = S.SessionKey,
                                 T.ScheduleTypeKey = S.ScheduleTypeKey,
                                 T.TierKey = S.TierKey,
                                 T.InvoiceTypeKey = S.InvoiceTypeKey,
                                 T.CreditMemoTypeKey = S.CreditMemoTypeKey,
                                 T.FeeTypeKey = S.FeeTypeKey,
                                 T.DiscountTypeKey = S.DiscountTypeKey,
                                 T.BillingAmount = S.BillingAmount,
                                 T.EDWCreatedDate = S.EDWCreatedDate,
                                 T.Deleted = S.Deleted
                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT(ReferenceInvoiceID,
                          InvoiceID,
                          BillingID,
                          BillingDateKey,
                          BillingStartDateKey,
                          BillingEndDateKey,
                          PaymentDueDateKey,
                          InvoiceVoidDateKey,
                          OrgKey,
                          LocationKey,
                          CompanyKey,
                          CostCenterTypeKey,
                          CostCenterKey,
                          StudentKey,
                          SponsorKey,
                          TuitionAssistanceProviderKey,
                          ProgramKey,
                          SessionKey,
                          ScheduleTypeKey,
                          TierKey,
                          InvoiceTypeKey,
                          CreditMemoTypeKey,
                          FeeTypeKey,
                          DiscountTypeKey,
                          BillingAmount,
                          EDWCreatedDate,
                          Deleted)
                   VALUES
             (ReferenceInvoiceID,
              InvoiceID,
              BillingID,
              BillingDateKey,
              BillingStartDateKey,
              BillingEndDateKey,
              PaymentDueDateKey,
              InvoiceVoidDateKey,
              OrgKey,
              LocationKey,
              CompanyKey,
              CostCenterTypeKey,
              CostCenterKey,
              StudentKey,
              SponsorKey,
              TuitionAssistanceProviderKey,
              ProgramKey,
              SessionKey,
              ScheduleTypeKey,
              TierKey,
              InvoiceTypeKey,
              CreditMemoTypeKey,
              FeeTypeKey,
              DiscountTypeKey,
              BillingAmount,
              EDWCreatedDate,
              Deleted
             )
             OUTPUT $action
                    INTO @tblMergeActions;
             SELECT @InsertCount = SUM(Inserted),
                    @UpdateCount = SUM(Updated)
             FROM
             ( 
		   --Count the number of inserts

                 SELECT COUNT(*) AS Inserted,
                        0 AS Updated
                 FROM @tblMergeActions
                 WHERE MergeAction = 'INSERT'
                 UNION ALL 
			  
			  -- Count the number of updates 

                 SELECT 0 AS Inserted,
                        COUNT(*) AS Updated
                 FROM @tblMergeActions
                 WHERE MergeAction = 'UPDATE'
             ) merge_actions;

		   
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(NVARCHAR(20), @InsertCount)+' rows into Target.';
                     PRINT @DebugMsg;
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Updated '+CONVERT(NVARCHAR(20), @UpdateCount)+' rows into Target.';
                     PRINT @DebugMsg;
             END;             
		   
		   --
		   -- Perform the Merge statement for soft deletes
		   --
             MERGE [BING_EDW].[dbo].[FactBilling] T
             USING #FactBillingUpsert S
             ON( 
		   -- S.BillingDateKey = T.BillingDateKey AND
             S.BillingID = T.BillingID)
                 WHEN MATCHED AND(S.Deleted IS NOT NULL
                                  AND t.Deleted IS NULL)
                 THEN UPDATE SET
                                 T.ReferenceInvoiceID = S.ReferenceInvoiceID,
                                 T.InvoiceID = S.InvoiceID,
                                 T.BillingStartDateKey = S.BillingStartDateKey,
                                 T.BillingEndDateKey = S.BillingEndDateKey,
                                 T.PaymentDueDateKey = S.PaymentDueDateKey,
                                 T.InvoiceVoidDateKey = S.InvoiceVoidDateKey,
                                 T.OrgKey = S.OrgKey,
                                 T.LocationKey = S.LocationKey,
                                 T.CompanyKey = S.CompanyKey,
                                 T.CostCenterTypeKey = S.CostCenterTypeKey,
                                 T.CostCenterKey = S.CostCenterKey,
                                 T.StudentKey = S.StudentKey,
                                 T.SponsorKey = S.SponsorKey,
                                 T.TuitionAssistanceProviderKey = S.TuitionAssistanceProviderKey,
                                 T.ProgramKey = S.ProgramKey,
                                 T.SessionKey = S.SessionKey,
                                 T.ScheduleTypeKey = S.ScheduleTypeKey,
                                 T.TierKey = S.TierKey,
                                 T.InvoiceTypeKey = S.InvoiceTypeKey,
                                 T.CreditMemoTypeKey = S.CreditMemoTypeKey,
                                 T.FeeTypeKey = S.FeeTypeKey,
                                 T.DiscountTypeKey = S.DiscountTypeKey,
                                 T.BillingAmount = S.BillingAmount,
                                 T.EDWCreatedDate = S.EDWCreatedDate,
                                 T.Deleted = S.Deleted
             OUTPUT $action
                    INTO @tblDeleteActions;
             SELECT @DeleteCount = SUM(Deleted)
             FROM
             ( 
		   -- Count the number of updates */

                 SELECT COUNT(*) AS Deleted
                 FROM @tblDeleteActions
                 WHERE MergeAction = 'UPDATE' -- It is a 'soft' delete, so shows up as an update in $action
             ) merge_actions;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Soft Deleted '+CONVERT(NVARCHAR(20), @DeleteCount)+' from into Target.';
                     PRINT @DebugMsg;
             END;

		   -- ================================================================================
		   --
		   -- S T E P   4.
		   --
		   -- Execute any automated tests associated with this EDW table load
		   --
		   -- ================================================================================


		   -- ================================================================================
		   --
		   -- S T E P   5.
		   --
		   -- Output Source / Insert / Update / Delete counts to caller, commit the transaction,
		   --	and tidy tup.
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
		   -- Drop the temp table
		   --
             DROP TABLE #FactBillingUpsert;

		   --
		   -- Write our successful run to the EDW AuditLog 
		   --
             EXEC BING_EDW.dbo.spInsertEDWBatchLoadLog
                  @AuditId = @AuditId,
                  @TaskName = @TaskName,
                  @BatchSplitByName = @BatchSplitByName,
                  @BatchSplitByValue = @BatchSplitByValue,
                  @SourceCount = @SourceCount,
                  @InsertCount = @InsertCount,
                  @UpdateCount = @UpdateCount,
                  @DeleteCount = @DeleteCount,
                  @StartTime = @EDWRunDateTime;
		   -- If we are returning all data we are not running in a loop, so AuditLog writing is done at this level
		   -- (If running in a loop, the calling proc takes care of the EDWAuditLog for us)
             IF @FiscalYearNumber IS NULL
                 BEGIN
                     EXEC [BING_EDW].[dbo].[spEDWEndAuditLog]
                          @InsertCount = @InsertCount,
                          @UpdateCount = @UpdateCount,
                          @DeleteCount = @DeleteCount,
                          @SourceCount = @SourceCount,
                          @AuditId = @AuditId;

		   -- Also write the successful load to EDWETLBatchControl, so we know how far back in history
		   --     we have to go in the next ETL run
                     EXEC dbo.spCMS_StagingEDWETLBatchControl
                          @TaskName = @TaskName;
             END;

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
		   -- If we are returning all data we are not running in a loop, so AuditLog writing is done at this level
		   -- (If running in a loop, the calling proc takes care of the EDWAuditLog for us)
             IF @FiscalYearNumber IS NULL
                 BEGIN
                     EXEC [BING_EDW].[dbo].[spEDWErrorAuditLog]
                          @AuditId = @AuditId;
             END;
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