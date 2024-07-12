CREATE PROCEDURE dbo.spSalesForce_StagingToEDW_FactLeadPipeline
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spSalesForce_StagingToEDW_FactLeadPipeline
    --
    -- Purpose:            Performs the Insert / Update / Delete ETL process for
    --                         the FactLeadPipeline table from Staging to BING_EDW.
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
    -- Usage:              EXEC dbo.spSalesForce_StagingToEDW_FactLeadPipeline @DebugMode = 1	
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
    --  2/13/18    sburke              BNG-252 - Initial version of proc
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'FactLeadPipeline';
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
         CREATE TABLE #FactLeadPipelineUpsert
         ([LeadKey]                      INT NOT NULL,
          [OrgKey]                       INT NOT NULL,
          [LocationKey]                  INT NOT NULL,
          [CompanyKey]                   INT NOT NULL,
          [CostCenterTypeKey]            INT NOT NULL,
          [CostCenterKey]                INT NOT NULL,
          [WebCampaignKey]               INT NOT NULL,
          [InquiryDateKey]               INT NOT NULL,
          [InquiryDate]                  DATETIMEOFFSET(0) NULL,
          [FirstInteractionDate]         DATETIMEOFFSET(0) NULL,
          [FirstTourCreatedDate]         DATETIMEOFFSET(0) NULL,
          [FirstTourScheduledDate]       DATETIMEOFFSET(0) NULL,
          [FirstTourCompletedDate]       DATETIMEOFFSET(0) NULL,
          [ConversionDate]               DATETIMEOFFSET(0) NULL,
          [FirstEnrollmentDate]          DATETIMEOFFSET(0) NULL,
          [LeadPipelineLastModifiedDate] DATETIMEOFFSET(0) NULL,
          [InteractionCount]             INT NULL,
          [TourScheduledCount]           INT NULL,
          [TourCompletedCount]           INT NULL,
          [EDWCreatedDate]               DATETIME2(7) NOT NULL,
          [EDWModifiedDate]              DATETIME2(7) NOT NULL
         );          

         -- ================================================================================
         --
         -- S T E P   2.
         --
         -- Populate the Landing table FROM Source, and create any helper indexes
         --
         -- ================================================================================
         INSERT INTO #FactLeadPipelineUpsert
         EXEC dbo.spSalesForce_StagingTransform_FactLeadPipeline;

         -- Get how many rows were extracted from source 

         SELECT @SourceCount = COUNT(1)
         FROM #FactLeadPipelineUpsert;
		   
         -- Debug output progress
         IF @DebugMode = 1
             SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Source.';
         PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
         CREATE NONCLUSTERED INDEX XAK1FactLeadPipelineUpsert ON #FactLeadPipelineUpsert
         ([LeadKey] ASC
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
             MERGE [BING_EDW].[dbo].[FactLeadPipeline] T 
             USING #FactLeadPipelineUpsert S
             ON(S.LeadKey = T.LeadKey)
                 WHEN MATCHED AND(S.OrgKey <> T.OrgKey
                                  OR S.LocationKey <> T.LocationKey
                                  OR S.CompanyKey <> T.CompanyKey
                                  OR S.CostCenterTypeKey <> T.CostCenterTypeKey
                                  OR S.CostCenterKey <> T.CostCenterKey
                                  OR S.WebCampaignKey <> T.WebCampaignKey
                                  OR S.InquiryDateKey <> T.InquiryDateKey
                                  OR S.InquiryDate <> T.InquiryDate
                                  OR S.FirstInteractionDate <> T.FirstInteractionDate
                                  OR S.FirstTourCreatedDate <> T.FirstTourCreatedDate
                                  OR S.FirstTourScheduledDate <> T.FirstTourScheduledDate
                                  OR S.FirstTourCompletedDate <> T.FirstTourCompletedDate
                                  OR S.ConversionDate <> T.ConversionDate
                                  OR S.FirstEnrollmentDate <> T.FirstEnrollmentDate
                                  OR S.LeadPipelineLastModifiedDate <> T.LeadPipelineLastModifiedDate
                                  OR S.InteractionCount <> T.InteractionCount
                                  OR S.TourScheduledCount <> T.TourScheduledCount
                                  OR S.TourCompletedCount <> T.TourCompletedCount)
                 THEN UPDATE SET
                                 T.OrgKey = S.OrgKey,
                                 T.LocationKey = S.LocationKey,
                                 T.CompanyKey = S.CompanyKey,
                                 T.CostCenterTypeKey = S.CostCenterTypeKey,
                                 T.CostCenterKey = S.CostCenterKey,
                                 T.WebCampaignKey = S.WebCampaignKey,
                                 T.InquiryDateKey = S.InquiryDateKey,
                                 T.InquiryDate = S.InquiryDate,
                                 T.FirstInteractionDate = S.FirstInteractionDate,
                                 T.FirstTourCreatedDate = S.FirstTourCreatedDate,
                                 T.FirstTourScheduledDate = S.FirstTourScheduledDate,
                                 T.FirstTourCompletedDate = S.FirstTourCompletedDate,
                                 T.ConversionDate = S.ConversionDate,
                                 T.FirstEnrollmentDate = S.FirstEnrollmentDate,
                                 T.LeadPipelineLastModifiedDate = S.LeadPipelineLastModifiedDate,
                                 T.InteractionCount = S.InteractionCount,
                                 T.TourScheduledCount = S.TourScheduledCount,
                                 T.TourCompletedCount = S.TourCompletedCount,
                                 T.EDWCreatedDate = S.EDWCreatedDate,
                                 T.EDWModifiedDate = S.EDWModifiedDate
                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT(LeadKey,
                          OrgKey,
                          LocationKey,
                          CompanyKey,
                          CostCenterTypeKey,
                          CostCenterKey,
                          WebCampaignKey,
                          InquiryDateKey,
                          InquiryDate,
                          FirstInteractionDate,
                          FirstTourCreatedDate,
                          FirstTourScheduledDate,
                          FirstTourCompletedDate,
                          ConversionDate,
                          FirstEnrollmentDate,
                          LeadPipelineLastModifiedDate,
                          InteractionCount,
                          TourScheduledCount,
                          TourCompletedCount,
                          EDWCreatedDate,
                          EDWModifiedDate)
                   VALUES
             (LeadKey,
              OrgKey,
              LocationKey,
              CompanyKey,
              CostCenterTypeKey,
              CostCenterKey,
              WebCampaignKey,
              InquiryDateKey,
              InquiryDate,
              FirstInteractionDate,
              FirstTourCreatedDate,
              FirstTourScheduledDate,
              FirstTourCompletedDate,
              ConversionDate,
              FirstEnrollmentDate,
              LeadPipelineLastModifiedDate,
              InteractionCount,
              TourScheduledCount,
              TourCompletedCount,
              EDWCreatedDate,
              EDWModifiedDate
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
             EXEC dbo.spSalesForce_StagingEDWETLBatchControl
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