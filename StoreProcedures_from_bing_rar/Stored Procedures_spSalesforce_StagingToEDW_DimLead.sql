
/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spSalesforce_StagingToEDW_DimLead'
)
    DROP PROCEDURE dbo.spSalesforce_StagingToEDW_DimLead;
GO
*/
CREATE PROCEDURE [dbo].[spSalesforce_StagingToEDW_DimLead]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spSalesforce_StagingToEDW_DimLead
    --
    -- Purpose:            Performs the Insert / Update / Delete ETL process for
    --                         the DimLead table from Staging to BING_EDW.
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
    -- Usage:              EXEC dbo.spSalesforce_StagingToEDW_DimLead @DebugMode = 1	
    -- 
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date			Modified By			Comments
    -- ----			-----------			--------
    --
    -- 02/02/18		hhebbalu			BNG-257 - EDW DimLead
	-- 03/14/18		hhebbalu			BNG-1380 - Correct DimLead load merge failures when running in Incremental loads
	--									The code is failing after we loaded SponsorManagementLeadID column in DimSponsor from Family Builder.
	--									In CSS, there are cases where a family has multiple famno and all those families have the same accountID from family builder.
	--									This creates duplicates in DimLead when joined with DimSponsor to pull the SponsorKey. This is fixed.
    -- 11/23/2021   Banandesi           BI-5140 Added new attaributes 		 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimLead';
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
       --  DECLARE @tblDeleteActions TABLE(MergeAction VARCHAR(20));
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
	    -- Extract FROM Source, Upserts and Deletes contained in a single transaction.  
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
             CREATE TABLE #DimLeadUpsert
             ([SponsorKey] [int] NOT NULL,
			  [LeadID] [varchar](25) NOT NULL,
			  [LeadName] [varchar](500) NOT NULL,
			  [LeadContact] [varchar](250) NOT NULL,
			  [LeadAddress] [varchar](500) NOT NULL,
			  [LeadCity] [varchar](50) NOT NULL,
			  [LeadState] [varchar](100) NOT NULL,
			  [LeadZIP] [varchar](25) NOT NULL,
			  [LeadPhone] [varchar](50) NOT NULL,
			  [LeadMobilePhone] [varchar](50) NOT NULL,
			  [LeadEmail] [varchar](100) NOT NULL,
			  [LeadStatus] [varchar](50) NOT NULL,
			  [InquiryBrand] [varchar](8000) NOT NULL,
			  [InquirySourceType] [varchar](1300) NOT NULL,
			  [InquirySource] [varchar](1300) NOT NULL,
			  [InquiryType] [varchar](50) NOT NULL,
			  [IsWebInquiry] [bit]  NOT NULL,
	          [IsContactedWithin24Hours] [bit]  NOT NULL,
	          [IsCreatedMondayThursdayLocal] [bit]  NOT NULL,
			  [MethodOfContact] [varchar](500) NOT NULL,
			  [ContactPreference] [varchar](500) NOT NULL,
			  [PreviousSponsorKey] [int] NOT NULL,
			  [EDWCreatedDate] [datetime2](7) NOT NULL,
			  [EDWModifiedDate] [datetime2](7) NOT NULL
             );          

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table FROM Source, and create any helper indexes
		   --
		   -- ================================================================================
             INSERT INTO #DimLeadUpsert
             EXEC dbo.spSalesforce_StagingTransform_DimLead
                  @EDWRunDateTime;

		   -- Get how many rows were extracted FROM source 

             SELECT @SourceCount = COUNT(1)
             FROM #DimLeadUpsert;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows FROM Source.';
             PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
             CREATE NONCLUSTERED INDEX XAK1DimLeadUpsert ON #DimLeadUpsert
             ([LeadID], [SponsorKey] ASC
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
             MERGE [BING_EDW].[dbo].[DimLead] T
             USING #DimLeadUpsert S
             ON(S.LeadID = T.LeadID AND -- Always join on Lead ID
					(S.PreviousSponsorKey = T.SponsorKey OR S.SponsorKey = T.SponsorKey)) -- Use Previous Sponsor Key for recently changed dimension or Sponsor Key if not changed.
                 WHEN MATCHED AND (S.SponsorKey <> T.SponsorKey
                                       OR S.LeadName <> T.LeadName
									   OR S.LeadContact <> T.LeadContact
                                       OR S.LeadAddress <> T.LeadAddress
                                       OR S.LeadCity <> T.LeadCity
                                       OR S.LeadState <> T.LeadState
									   OR S.LeadZIP <> T.LeadZIP
									   OR S.LeadPhone <> T.LeadPhone
									   OR S.LeadMobilePhone <> T.LeadMobilePhone
									   OR S.LeadEmail <> T.LeadEmail
									   OR S.LeadStatus <> T.LeadStatus
									   OR S.InquiryBrand <> T.InquiryBrand
									   OR S.InquirySourceType <> T.InquirySourceType
									   OR S.InquirySource <> T.InquirySource
									   OR S.InquiryType <> T.InquiryType
									   OR S.IsWebInquiry <> T.IsWebInquiry
	                                   OR S.IsContactedWithin24Hours <> T.IsContactedWithin24Hours
	                                   OR S.IsCreatedMondayThursdayLocal <> T.IsCreatedMondayThursdayLocal
									   OR S.MethodOfContact <> T.MethodOfContact
									   OR S.ContactPreference <> T.ContactPreference)
                 THEN UPDATE SET
                                 T.SponsorKey = S.SponsorKey,
                                 T.LeadName = S.LeadName,
								 T.LeadContact = S.LeadContact,
                                 T.LeadAddress = S.LeadAddress,
                                 T.LeadCity = S.LeadCity,
                                 T.LeadState = S.LeadState,
								 T.LeadZIP = S.LeadZIP,
								 T.LeadPhone = S.LeadPhone,
								 T.LeadMobilePhone = S.LeadMobilePhone,
								 T.LeadEmail = S.LeadEmail,
								 T.LeadStatus = S.LeadStatus,
								 T.InquiryBrand = S.InquiryBrand,
								 T.InquirySourceType = S.InquirySourceType,
								 T.InquirySource = S.InquirySource,
								 T.InquiryType = S.InquiryType,
								 T.IsWebInquiry = S.IsWebInquiry,
								 T.IsContactedWithin24Hours= S.IsContactedWithin24Hours,
								 T.IsCreatedMondayThursdayLocal = S.IsCreatedMondayThursdayLocal,
								 T.MethodOfContact = S.MethodOfContact,
								 T.ContactPreference = S.ContactPreference,
                                 T.EDWCreatedDate = S.EDWCreatedDate,
                                 T.EDWModifiedDate = S.EDWModifiedDate
                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT(SponsorKey,
						  LeadID,
						  LeadName,
						  LeadContact,
						  LeadAddress,
						  LeadCity,
						  LeadState,
						  LeadZIP,
						  LeadPhone,
  						  LeadMobilePhone,
						  LeadEmail,
						  LeadStatus,
						  InquiryBrand,
						  InquirySourceType,
						  InquirySource,
						  InquiryType,
						  IsWebInquiry,
						  IsContactedWithin24Hours,
						  IsCreatedMondayThursdayLocal,
						  MethodOfContact,
						  ContactPreference,
						  EDWCreatedDate,
						  EDWModifiedDate)
                   VALUES
                         (SponsorKey,
						  LeadID,
						  LeadName,
						  LeadContact,
						  LeadAddress,
						  LeadCity,
						  LeadState,
						  LeadZIP,
						  LeadPhone,
  						  LeadMobilePhone,
						  LeadEmail,
						  LeadStatus,
						  InquiryBrand,
						  InquirySourceType,
						  InquirySource,
						  InquiryType,
						  IsWebInquiry,
						  IsContactedWithin24Hours,
						  IsCreatedMondayThursdayLocal,
						  MethodOfContact,
						  ContactPreference,
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
		   
		   --
		   -- Perform the Merge statement for soft deletes

		   -- Debug output progress

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
		      DELETE BING_EDW.dbo.DimLead
             FROM BING_EDW.dbo.DimLead T
                  LEFT  JOIN vLeads S ON S.LeadID = T.LeadID
             WHERE  S.LeadID is Null;
	  
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
             DROP TABLE #DimLeadUpsert;

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