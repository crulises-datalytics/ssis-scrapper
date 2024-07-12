/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spCMS_StagingToEDW_FactTierAssignment'
)
    DROP PROCEDURE dbo.spCMS_StagingToEDW_FactTierAssignment;
GO
*/
CREATE PROCEDURE dbo.spCMS_StagingToEDW_FactTierAssignment
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingToEDW_FactTierAssignment
    --
    -- Purpose:            Performs the Insert / Update / Delete ETL process for
    --                         the FactTierAssignment table from Staging to BING_EDW.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                             sub-procedure spCMS_StagingTransform_FactTierAssignment, 
    --                             and create any helper indexes
    --                         Step 3: Perform the Insert / Update (Merge) / deletes required 
    --                             for this EDW table load
    --                         Step 4: Execute any automated tests associated with this EDW table load
    --                         Step 5: Output Source / Insert / Update / Delete counts to caller, 
    --                             commit the transaction, and tidy-up
    --
    -- Parameters:         @EDWRunDateTime - Used to populate any 'current date' fields, rather than
    --                         making numerous GETDATE() calls  
    --                     @DebugMode - Used just for development & debug purposes,
    --                         outputting helpful info back to the caller.  Not
    --                         required for Production, and does not affect any
    --                         core logic.
    --
    -- Usage:			   EXEC dbo.spCMS_StagingToEDW_FactTierAssignment @DebugMode = 1
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date		Modified By		Comments
    -- ----		-----------		--------
    --
    -- 11/07/17    	sburke			Initial version of proc, converted from SSIS logic
    --                              Note that this Stored Proc does not get called as part
    --                                  of a batch (like, for example, the FactNetRevenue loads)
    --                                  Performance and Logspace usage when running a full historical 
    --                                  load is not so onerous to require it.
    --  5/25/18     sburke              BNG-1759 - Correct MERGE logic that was causing constraint violations
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @TaskName VARCHAR(100)= 'FactTierAssignment';

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

	    -- Write to AuditLog that we are starting, and get the AuditId we use for each batch we load		  
         EXEC [BING_EDW].[dbo].[spEDWBeginAuditLog]
              @SourceName = @TaskName,
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
		   -- ================================================================================
		   --
		   -- S T E P   1.
		   --
		   -- Create temporary landing #table
		   --
		   -- ================================================================================
             CREATE TABLE #FactTierAssignmentUpsert
             ([OrgKey]                         [INT] NOT NULL,
              [LocationKey]                    [INT] NOT NULL,
              [CompanyKey]                     [INT] NOT NULL,
              [CostCenterTypeKey]              [INT] NOT NULL,
              [CostCenterKey]                  [INT] NOT NULL,
              [StudentKey]                     [INT] NOT NULL,
              [SponsorKey]                     [INT] NOT NULL,
              [TierKey]                        [INT] NOT NULL,
              [EnrollmentID]                   [INT] NOT NULL,
              [TierAssignmentEffectiveDateKey] [INT] NOT NULL,
              [TierAssignmentEndDateKey]       [INT] NOT NULL,
              [TierDatesEDWChosen]             [VARCHAR](25) NOT NULL,
              [EDWCreatedDate]                 [DATETIME2](7) NOT NULL,
              [EDWModifiedDate]                [DATETIME2](7) NOT NULL,
              [Deleted]                        [DATETIME2](7) NULL
             );
          

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table from Source, and create any helper indexes
		   --
		   -- ================================================================================
             INSERT INTO #FactTierAssignmentUpsert
             EXEC dbo.spCMS_StagingTransform_FactTierAssignment
                  @EDWRunDateTime;


		   -- Get how many rows were extracted from source 

             SELECT @SourceCount = COUNT(1)
             FROM #FactTierAssignmentUpsert;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Source.';
             PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
             CREATE NONCLUSTERED INDEX XAK1FactTierAssignmentUpsert ON #FactTierAssignmentUpsert
             ([LocationKey] ASC, [StudentKey] ASC, [SponsorKey] ASC, [EnrollmentID] ASC, [TierAssignmentEffectiveDateKey] ASC
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
             MERGE [BING_EDW].[dbo].[FactTierAssignment] T
             USING #FactTierAssignmentUpsert S
             ON(S.EnrollmentID = T.EnrollmentID 
                AND S.LocationKey = T.LocationKey 
                AND S.CostCenterTypeKey = T.CostCenterTypeKey
                AND S.SponsorKey = T.SponsorKey 
                AND S.StudentKey = T.StudentKey 
                AND S.TierAssignmentEffectiveDateKey = T.TierAssignmentEffectiveDateKey)
                 WHEN MATCHED AND S.Deleted IS NULL
                                  AND (S.TierKey <> T.TierKey
                                       OR S.TierAssignmentEndDateKey <> T.TierAssignmentEndDateKey
                                       OR S.TierDatesEDWChosen <> T.TierDatesEDWChosen
							    OR S.OrgKey <> T.OrgKey
							    OR S.CompanyKey <> T.CompanyKey
							    OR S.CostCenterKey <> T.CostCenterKey
                                       OR T.Deleted IS NOT NULL)
                 THEN UPDATE SET
                                 T.TierKey = S.TierKey,
                                 T.TierAssignmentEndDateKey = S.TierAssignmentEndDateKey,
						   T.OrgKey = S.OrgKey,
						   T.CompanyKey = S.CompanyKey,
						   T.CostCenterKey = S.CostCenterKey,
                                 T.TierDatesEDWChosen = S.TierDatesEDWChosen,
                                 T.EDWCreatedDate = S.EDWCreatedDate,
                                 T.EDWModifiedDate = S.EDWModifiedDate,
                                 T.Deleted = S.Deleted
                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT(EnrollmentID,
                          OrgKey,
                          LocationKey,
                          CompanyKey,
                          CostCenterTypeKey,
                          CostCenterKey,
                          StudentKey,
                          SponsorKey,
                          TierKey,
                          TierAssignmentEffectiveDateKey,
                          TierAssignmentEndDateKey,
                          TierDatesEDWChosen,
                          EDWCreatedDate,
                          EDWModifiedDate,
                          Deleted)
                   VALUES
             (EnrollmentID,
              OrgKey,
              LocationKey,
              CompanyKey,
              CostCenterTypeKey,
              CostCenterKey,
              StudentKey,
              SponsorKey,
              TierKey,
              TierAssignmentEffectiveDateKey,
              TierAssignmentEndDateKey,
              TierDatesEDWChosen,
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
             MERGE [BING_EDW].[dbo].[FactTierAssignment] T
             USING #FactTierAssignmentUpsert S
             ON(S.EnrollmentID = T.EnrollmentID
                AND S.OrgKey = T.OrgKey
                AND S.LocationKey = T.LocationKey
                AND S.CompanyKey = T.CompanyKey
                AND S.CostCenterTypeKey = T.CostCenterTypeKey
                AND S.CostCenterKey = T.CostCenterKey
                AND S.SponsorKey = T.SponsorKey
                AND S.StudentKey = T.StudentKey
                AND S.TierAssignmentEffectiveDateKey = T.TierAssignmentEffectiveDateKey)
                 WHEN MATCHED AND(S.Deleted IS NOT NULL
                                  AND t.Deleted IS NULL)
                 THEN UPDATE SET
                                 T.TierKey = S.TierKey,
                                 T.TierAssignmentEndDateKey = S.TierAssignmentEndDateKey,
                                 T.TierDatesEDWChosen = S.TierDatesEDWChosen,
                                 T.EDWCreatedDate = S.EDWCreatedDate,
                                 T.EDWModifiedDate = S.EDWModifiedDate,
                                 T.Deleted = S.Deleted
             OUTPUT $action
                    INTO @tblDeleteActions;
		  --

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
             DROP TABLE #FactTierAssignmentUpsert;

		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Completing successfully.';
             PRINT @DebugMsg;
		   -- Write the successful load to EDWAuditLog
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
         END TRY
         BEGIN CATCH
	    	  -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Rolling back transaction.';
             PRINT @DebugMsg;
		   -- Rollback the transaction
             ROLLBACK TRANSACTION;
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