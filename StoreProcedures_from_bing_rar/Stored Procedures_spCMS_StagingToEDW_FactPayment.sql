/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spCMS_StagingToEDW_FactPayment'
)
    DROP PROCEDURE dbo.spCMS_StagingToEDW_FactPayment;
GO
*/
CREATE PROCEDURE dbo.spCMS_StagingToEDW_FactPayment
(@EDWRunDateTime   DATETIME2 = NULL,
 @AuditId          INT,
 @FiscalYearNumber INT,
 @DebugMode        INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:	   spCMS_StagingToEDW_FactPayment
    --
    -- Purpose:		   Performs the Insert / Update / Delete ETL process for
    --					  the FactPayment table from Staging to BING_EDW.
    --
    --				   Step 1: Create temporary landing #table
    --				   Step 2: Populate the Landing table from Source by calling
    --							sub-procedure spCMS_StagingTransform_FactPayment, 
    --							and create any helper indexes
    --				   Step 3: Perform the Insert / Update (Merge) / deletes required 
    --							for this EDW table load
    --				   Step 4: Execute any automated tests associated with this EDW table load
    --				   Step 5: Output Source / Insert / Update / Delete counts to caller, 
    --						 commit the transaction, and tidy-up
    --
    -- Parameters:		   @DebugMode - Used just for development & debug purposes,
    --					  outputting helpful info back to the caller.  Not
    --					  required for Production, and does not affect any
    --					  core logic.
    --
    -- Returns:		   Single-row results set containing the following columns:
    --					  SourceCount	   -	  Number of rows extracted from source
    --					  InsertCount	   -	  Number or rows inserted to target table
    --					  UpdateCount	   -	  Number or rows updated in target table
    --					  DeleteCount	   -	  Number or rows deleted in target table
    --
    -- Usage:			   EXEC dbo.spCMS_StagingToEDW_FactPayment @EDWRunDateTime = @EDWRunDateTime, @DebugMode = 1
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date		Modified By		Comments
    -- ----		-----------		--------
    --
    -- 10/06/17    	sburke			Initial version of proc, converted from SSIS logic
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @TaskName VARCHAR(100)= 'FactPayment';

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
             CREATE TABLE #FactPaymentUpsert
             ([PaymentID]                    VARCHAR(50) NOT NULL,
              [OrgKey]                       INT NOT NULL,
              [LocationKey]                  INT NOT NULL,
              [CompanyKey]                   INT NOT NULL,
              [CostCenterTypeKey]            INT NOT NULL,
              [CostCenterKey]                INT NOT NULL,
              [StudentKey]                   INT NOT NULL,
              [SponsorKey]                   INT NOT NULL,
              [TuitionAssistanceProviderKey] INT NOT NULL,
              [PaymentTypeKey]               INT NOT NULL,
              [PaymentDateKey]               INT NOT NULL,
              [PaymentVoidDateKey]           INT NULL,
              [PaymentAmount]                NUMERIC(19, 4) NOT NULL,
              [PaymentUnappliedAmount]       NUMERIC(19, 4) NOT NULL,
              [EDWCreatedDate]               DATETIME2(7) NOT NULL,
              [EDWModifiedDate]              DATETIME2(7) NOT NULL,
              [Deleted]                      DATETIME2(7) NULL
             );
          

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table from Source, and create any helper indexes
		   --
		   -- ================================================================================
             INSERT INTO #FactPaymentUpsert
             EXEC dbo.spCMS_StagingTransform_FactPayment
                  @FiscalYearNumber = @FiscalYearNumber,
                  @EDWRunDateTime = @EDWRunDateTime;


		   -- Get how many rows were extracted from source 

             SELECT @SourceCount = COUNT(1)
             FROM #FactPaymentUpsert;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Source.';
             PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
             CREATE NONCLUSTERED INDEX XAK1FactPaymentUpsert ON #FactPaymentUpsert
             ([PaymentID] ASC, [PaymentDateKey] ASC
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
             MERGE [BING_EDW].[dbo].[FactPayment] T
             USING #FactPaymentUpsert S
             ON(S.PaymentDateKey = T.PaymentDateKey
                AND S.PaymentID = T.PaymentID)
                 WHEN MATCHED AND S.Deleted IS NULL
                                  AND (S.OrgKey <> T.OrgKey
                                       OR S.LocationKey <> T.LocationKey
                                       OR S.CompanyKey <> T.CompanyKey
                                       OR S.CostCenterTypeKey <> T.CostCenterTypeKey
                                       OR S.CostCenterKey <> T.CostCenterKey
                                       OR S.StudentKey <> T.StudentKey
                                       OR S.SponsorKey <> T.SponsorKey
                                       OR S.TuitionAssistanceProviderKey <> T.TuitionAssistanceProviderKey
                                       OR S.PaymentTypeKey <> T.PaymentTypeKey
                                       OR S.PaymentVoidDateKey <> T.PaymentVoidDateKey
                                       OR S.PaymentAmount <> T.PaymentAmount
                                       OR S.PaymentUnappliedAmount <> T.PaymentUnappliedAmount
                                       OR T.Deleted IS NOT NULL)
                 THEN UPDATE SET
                                 T.OrgKey = S.OrgKey,
                                 T.LocationKey = S.LocationKey,
                                 T.CompanyKey = S.CompanyKey,
                                 T.CostCenterTypeKey = S.CostCenterTypeKey,
                                 T.CostCenterKey = S.CostCenterKey,
                                 T.StudentKey = S.StudentKey,
                                 T.SponsorKey = S.SponsorKey,
                                 T.TuitionAssistanceProviderKey = S.TuitionAssistanceProviderKey,
                                 T.PaymentTypeKey = S.PaymentTypeKey,
                                 T.PaymentVoidDateKey = S.PaymentVoidDateKey,
                                 T.PaymentAmount = S.PaymentAmount,
                                 T.PaymentUnappliedAmount = S.PaymentUnappliedAmount,
                                 T.EDWCreatedDate = S.EDWCreatedDate,
                                 T.EDWModifiedDate = S.EDWModifiedDate,
                                 T.Deleted = S.Deleted
                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT(PaymentID,
                          OrgKey,
                          LocationKey,
                          CompanyKey,
                          CostCenterTypeKey,
                          CostCenterKey,
                          StudentKey,
                          SponsorKey,
                          TuitionAssistanceProviderKey,
                          PaymentTypeKey,
                          PaymentDateKey,
                          PaymentVoidDateKey,
                          PaymentAmount,
                          PaymentUnappliedAmount,
                          EDWCreatedDate,
                          EDWModifiedDate,
                          Deleted)
                   VALUES
             (PaymentID,
              OrgKey,
              LocationKey,
              CompanyKey,
              CostCenterTypeKey,
              CostCenterKey,
              StudentKey,
              SponsorKey,
              TuitionAssistanceProviderKey,
              PaymentTypeKey,
              PaymentDateKey,
              PaymentVoidDateKey,
              PaymentAmount,
              PaymentUnappliedAmount,
              EDWCreatedDate,
              EDWModifiedDate,
              Deleted
             )
             OUTPUT $action
                    INTO @tblMergeActions;
		  --

             SELECT @InsertCount = SUM(Inserted),
                    @UpdateCount = SUM(Updated)
             FROM
             ( 
		   -- Count the number of inserts

                 SELECT COUNT(*) AS Inserted,
                        0 AS Updated
                 FROM @tblMergeActions
                 WHERE MergeAction = 'INSERT'
                 UNION ALL 
			  
			  --Count the number of updates */

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
             MERGE [BING_EDW].[dbo].[FactPayment] T
             USING #FactPaymentUpsert S
             ON(S.PaymentDateKey = T.PaymentDateKey
                AND S.PaymentID = T.PaymentID)
                 WHEN MATCHED AND(S.Deleted IS NOT NULL
                                  AND t.Deleted IS NULL)
                 THEN UPDATE SET
                                 T.OrgKey = S.OrgKey,
                                 T.LocationKey = S.LocationKey,
                                 T.CompanyKey = S.CompanyKey,
                                 T.CostCenterTypeKey = S.CostCenterTypeKey,
                                 T.CostCenterKey = S.CostCenterKey,
                                 T.StudentKey = S.StudentKey,
                                 T.SponsorKey = S.SponsorKey,
                                 T.TuitionAssistanceProviderKey = S.TuitionAssistanceProviderKey,
                                 T.PaymentTypeKey = S.PaymentTypeKey,
                                 T.PaymentVoidDateKey = S.PaymentVoidDateKey,
                                 T.PaymentAmount = S.PaymentAmount,
                                 T.PaymentUnappliedAmount = S.PaymentUnappliedAmount,
                                 T.EDWCreatedDate = S.EDWCreatedDate,
                                 T.EDWModifiedDate = S.EDWModifiedDate,
                                 T.Deleted = S.Deleted
             OUTPUT $action
                    INTO @tblDeleteActions;
             SELECT @DeleteCount = SUM(Deleted)
             FROM
             ( 
		   -- Count the number of updates 

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
             DROP TABLE #FactPaymentUpsert;

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
		   -- Raiserror
		   --	
             DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
             SELECT @ErrMsg = ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;
GO