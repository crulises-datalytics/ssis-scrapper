CREATE PROCEDURE dbo.spCMS_StagingToEDW_DimTuitionAssistanceProvider
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingToEDW_DimTuitionAssistanceProvider
    --
    -- Purpose:            Performs the Insert / Update / Delete ETL process for
    --                         the DimTuitionAssistanceProvider table from Staging to BING_EDW.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                                 sub-procedure spHR_StagingTransform_DimOrganization, 
    --                                 and create any helper indexes
    --                         Step 3: Perform the Insert / Update (Merge) / deletes required 
    --                                 for this EDW table load			 
    --                         Step 4: Execute any automated tests associated with this EDW table load
    --                         Step 5: Output Source / Insert / Update / Delete counts to caller, 
    --                                 commit the transaction, and tidy-up
    --
    -- Parameters:         @EDWRunDateTime - Used to populate any 'current date' fields, rather than
    --                         making numerous GETDATE() calls  
    --                     @DebugMode - Used just for development & debug purposes,
    --                         outputting helpful info back to the caller.  Not
    --                         required for Production, and does not affect any
    --                         core logic.
    --				   
    -- Returns:            Single-row results set containing the following columns:
    --                         SourceCount - Number of rows extracted from source
    --                         InsertCount - Number or rows inserted to target table
    --                         UpdateCount - Number or rows updated in target table
    --                         DeleteCount - Number or rows deleted in target table
    --
    -- Usage:              EXEC dbo.spCMS_StagingToEDW_DimTuitionAssistanceProvider @DebugMode = 1	
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
    --  1/24/18    sburke              BNG-655 - Convert from SSIS DFT to Stored Proc as part of
    --                                     refactoring effort
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimTuitionAssistanceProvider - CMS';
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
	    -- Write to EDW AuditLog we are starting
	    --
         EXEC [BING_EDW].[dbo].[spEDWBeginAuditLog]
              @SourceName = @SourceName,
              @AuditId = @AuditId OUTPUT; 


         -- ================================================================================
         --
         -- S T E P   1.
         --
         -- Create temporary landing #table
         --
         -- ================================================================================
         CREATE TABLE #DimTuitionAssistanceProviderUpsert
         ([TuitionAssistanceProviderID]                 INT NOT NULL,
          [TuitionAssistanceProviderName]               VARCHAR(100) NOT NULL,
          [TuitionAssistanceProviderType]               VARCHAR(100) NOT NULL,
          [TuitionAssistanceProviderAddress1]           VARCHAR(100) NOT NULL,
          [TuitionAssistanceProviderAddress2]           VARCHAR(100) NOT NULL,
          [TuitionAssistanceProviderCity]               VARCHAR(100) NOT NULL,
          [TuitionAssistanceProviderState]              CHAR(2) NOT NULL,
          [TuitionAssistanceProviderZIP]                VARCHAR(10) NOT NULL,
          [TuitionAssistanceProviderContact]            VARCHAR(100) NOT NULL,
          [TuitionAssistanceProviderProvidesSubsidy]    VARCHAR(50) NOT NULL,
          [TuitionAssistanceProviderBackupCare]         VARCHAR(50) NOT NULL,
          [TuitionAssistanceProviderCareSelectDiscount] VARCHAR(50) NOT NULL,
          [TuitionAssistanceProviderFirstContractDate]  DATE NULL,
          [CSSCenterNumber]                             VARCHAR(4) NOT NULL,
          [CSSCustomerCode]                             VARCHAR(4) NOT NULL,
          [SourceSystem]                                VARCHAR(3) NOT NULL,
          [EDWCreatedDate]                              DATETIME2(7) NOT NULL,
          [EDWCreatedBy]                                VARCHAR(50) NOT NULL,
          [EDWModifiedDate]                             DATETIME2(7) NOT NULL,
          [EDWModifiedBy]                               VARCHAR(50) NOT NULL,
          [Deleted]                                     DATETIME2(7) NULL
         );          

         -- ================================================================================
         --
         -- S T E P   2.
         --
         -- Populate the Landing table FROM Source, and create any helper indexes
         --
         -- ================================================================================
         INSERT INTO #DimTuitionAssistanceProviderUpsert
         EXEC dbo.spCMS_StagingTransform_DimTuitionAssistanceProvider;

         -- Get how many rows were extracted from source 
         SELECT @SourceCount = COUNT(1)
         FROM #DimTuitionAssistanceProviderUpsert;
		   
         -- Debug output progress
         IF @DebugMode = 1
             SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Source.';
         PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
         CREATE NONCLUSTERED INDEX XAK1DimTuitionAssistanceProviderUpsert ON #DimTuitionAssistanceProviderUpsert
         ([TuitionAssistanceProviderID] ASC, [SourceSystem] ASC
         );
	    -- --------------------------------------------------------------------------------
	    -- Upserts and Deletes contained in a single transaction.  
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
		   -- S T E P   3.
		   --
		   -- Perform the Insert / Update (Merge) / deletes required for this EDW table load
		   --
		   -- ================================================================================

		   --
		   -- Perform the Merge statement for insert / updates
		   --
             MERGE [BING_EDW].[dbo].[DimTuitionAssistanceProvider] T
             USING #DimTuitionAssistanceProviderUpsert S
             ON(S.SourceSystem = T.SourceSystem
                AND S.TuitionAssistanceProviderID = T.TuitionAssistanceProviderID)
                 WHEN MATCHED AND(S.TuitionAssistanceProviderName <> T.TuitionAssistanceProviderName
                                  OR S.TuitionAssistanceProviderType <> T.TuitionAssistanceProviderType
                                  OR S.TuitionAssistanceProviderAddress1 <> T.TuitionAssistanceProviderAddress1
                                  OR S.TuitionAssistanceProviderAddress2 <> T.TuitionAssistanceProviderAddress2
                                  OR S.TuitionAssistanceProviderCity <> T.TuitionAssistanceProviderCity
                                  OR S.TuitionAssistanceProviderState <> T.TuitionAssistanceProviderState
                                  OR S.TuitionAssistanceProviderZIP <> T.TuitionAssistanceProviderZIP
                                  OR S.TuitionAssistanceProviderContact <> T.TuitionAssistanceProviderContact
                                  OR S.TuitionAssistanceProviderProvidesSubsidy <> T.TuitionAssistanceProviderProvidesSubsidy
                                  OR S.TuitionAssistanceProviderBackupCare <> T.TuitionAssistanceProviderBackupCare
                                  OR S.TuitionAssistanceProviderCareSelectDiscount <> T.TuitionAssistanceProviderCareSelectDiscount
                                  OR S.TuitionAssistanceProviderFirstContractDate <> T.TuitionAssistanceProviderFirstContractDate
                                  OR S.CSSCenterNumber <> T.CSSCenterNumber
                                  OR S.CSSCustomerCode <> T.CSSCustomerCode)
                 THEN UPDATE SET
                                 T.TuitionAssistanceProviderName = S.TuitionAssistanceProviderName,
                                 T.TuitionAssistanceProviderType = S.TuitionAssistanceProviderType,
                                 T.TuitionAssistanceProviderAddress1 = S.TuitionAssistanceProviderAddress1,
                                 T.TuitionAssistanceProviderAddress2 = S.TuitionAssistanceProviderAddress2,
                                 T.TuitionAssistanceProviderCity = S.TuitionAssistanceProviderCity,
                                 T.TuitionAssistanceProviderState = S.TuitionAssistanceProviderState,
                                 T.TuitionAssistanceProviderZIP = S.TuitionAssistanceProviderZIP,
                                 T.TuitionAssistanceProviderContact = S.TuitionAssistanceProviderContact,
                                 T.TuitionAssistanceProviderProvidesSubsidy = S.TuitionAssistanceProviderProvidesSubsidy,
                                 T.TuitionAssistanceProviderBackupCare = S.TuitionAssistanceProviderBackupCare,
                                 T.TuitionAssistanceProviderCareSelectDiscount = S.TuitionAssistanceProviderCareSelectDiscount,
                                 T.TuitionAssistanceProviderFirstContractDate = S.TuitionAssistanceProviderFirstContractDate,
                                 T.CSSCenterNumber = S.CSSCenterNumber,
                                 T.CSSCustomerCode = S.CSSCustomerCode,
                                 T.EDWCreatedDate = S.EDWCreatedDate,
                                 T.EDWCreatedBy = S.EDWCreatedBy,
                                 T.EDWModifiedDate = S.EDWModifiedDate,
                                 T.EDWModifiedBy = S.EDWModifiedBy
                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT(TuitionAssistanceProviderID,
                          TuitionAssistanceProviderName,
                          TuitionAssistanceProviderType,
                          TuitionAssistanceProviderAddress1,
                          TuitionAssistanceProviderAddress2,
                          TuitionAssistanceProviderCity,
                          TuitionAssistanceProviderState,
                          TuitionAssistanceProviderZIP,
                          TuitionAssistanceProviderContact,
                          TuitionAssistanceProviderProvidesSubsidy,
                          TuitionAssistanceProviderBackupCare,
                          TuitionAssistanceProviderCareSelectDiscount,
                          TuitionAssistanceProviderFirstContractDate,
                          CSSCenterNumber,
                          CSSCustomerCode,
                          SourceSystem,
                          EDWCreatedDate,
                          EDWCreatedBy,
                          EDWModifiedDate,
                          EDWModifiedBy)
                   VALUES
             (TuitionAssistanceProviderID,
              TuitionAssistanceProviderName,
              TuitionAssistanceProviderType,
              TuitionAssistanceProviderAddress1,
              TuitionAssistanceProviderAddress2,
              TuitionAssistanceProviderCity,
              TuitionAssistanceProviderState,
              TuitionAssistanceProviderZIP,
              TuitionAssistanceProviderContact,
              TuitionAssistanceProviderProvidesSubsidy,
              TuitionAssistanceProviderBackupCare,
              TuitionAssistanceProviderCareSelectDiscount,
              TuitionAssistanceProviderFirstContractDate,
              CSSCenterNumber,
              CSSCustomerCode,
              SourceSystem,
              EDWCreatedDate,
              EDWCreatedBy,
              EDWModifiedDate,
              EDWModifiedBy
             )
             OUTPUT $action
                    INTO @tblMergeActions;
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
		   -- Write our successful run to the EDW AuditLog 
		   --
             EXEC [BING_EDW].[dbo].[spEDWEndAuditLog]
                  @InsertCount = @InsertCount,
                  @UpdateCount = @UpdateCount,
                  @DeleteCount = @DeleteCount,
                  @SourceCount = @SourceCount,
                  @AuditId = @AuditId;

		   -- Also write the successful load to EDWETLBatchControl, so we know how far back in history
		   --     we have to go in the next ETL run
             EXEC dbo.spCMS_StagingEDWETLBatchControl
                  @TaskName = @SourceName;

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
GO