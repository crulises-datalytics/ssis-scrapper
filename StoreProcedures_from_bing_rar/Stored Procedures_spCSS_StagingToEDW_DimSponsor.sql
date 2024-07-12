

CREATE PROCEDURE [dbo].[spCSS_StagingToEDW_DimSponsor]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCSS_StagingToEDW_DimSponsor
    --
    -- Purpose:            Performs the Insert / Update / Delete ETL process for
    --                         the DimSponsor table from Staging to BING_EDW.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                             sub-procedure spCSS_StagingTransfrom_DimSponsor, 
    --                             and create any helper indexes
    --                         Step 3: Perform the Insert / Update (Merge) / deletes required 
    --                             for this EDW table load
    --                         Step 4: Execute any automated tests associated with this EDW table load
    --                         Step 5: Output Source / Insert / Update / Delete counts to caller, 
    --                             commit the transaction, and tidy-up
    --
    -- Parameters:             @EDWRunDateTime - Used to populate any 'current date' fields, rather than
    --                             making numerous GETDATE() calls  
    --                         @DebugMode - Used just for development & debug purposes,
    --                             outputting helpful info back to the caller.  Not
    --                             required for Production, and does not affect any
    --                             core logic.
    --
    -- Usage:			   EXEC dbo.spCSS_StagingToEDW_DimSponsor @DebugMode = 1 
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By     Comments
    -- ----         -----------     --------
    --
    -- 12/12/17    ADevabhakthuni          BNG-258 - Refactor DimSponsor staging to EDW load.
	--                                  
	--                                 
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimSponsor';
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
             CREATE TABLE #DimSponsorUpsert
             ([SponsorID]                  [INT] NOT NULL,
              [SponsorFirstName]           [VARCHAR](100) NOT NULL,
              [SponsorMiddleName]          [VARCHAR](100) NOT NULL,
              [SponsorLastName]            [VARCHAR](100) NOT NULL,
              [SponsorFullName]            [VARCHAR](400) NOT NULL,
              [SponsorPhonePrimary]        [VARCHAR](20) NOT NULL,
              [SponsorPhoneSecondary]      [VARCHAR](20) NOT NULL,
              [SponsorPhoneTertiary]       [VARCHAR](20) NOT NULL,
              [SponsorEmailPrimary]        [VARCHAR](100) NOT NULL,
              [SponsorEmailSecondary]      [VARCHAR](100) NOT NULL,
              [SponsorAddress1]            [VARCHAR](100) NOT NULL,
              [SponsorAddress2]            [VARCHAR](100) NOT NULL,
              [SponsorCity]                [VARCHAR](100) NOT NULL,
              [SponsorState]               [CHAR](2) NOT NULL,
              [SponsorZIP]                 [VARCHAR](10) NOT NULL,
              [SponsorStudentRelationship] [VARCHAR](100) NOT NULL,
              [SponsorGender]              [VARCHAR](10) NOT NULL,
              [SponsorInternalEmployee]    [VARCHAR](50) NOT NULL,
              [SponsorStatus]              [VARCHAR](100) NOT NULL,
              [SponsorDoNotEmail]          [VARCHAR](25) NOT NULL,
              [SponsorLeadManagementID]    [VARCHAR](100) NOT NULL,
              [CSSCenterNumber]            [VARCHAR](4) NOT NULL,
              [CSSFamilyNumber]            [VARCHAR](4) NOT NULL,
              [SourceSystem]               [VARCHAR](3) NOT NULL,
              [EDWEffectiveDate]           [DATETIME2](7) NOT NULL,
              [EDWCreatedDate]             [DATETIME2](7) NOT NULL,
              [EDWCreatedBy]               [VARCHAR](50) NOT NULL,
              [Deleted]                    [DATETIME2](7) NULL,
             );

          

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table from Source, and create any helper indexes
		   --
		   -- ================================================================================
             INSERT INTO #DimSponsorUpsert WITH(TABLOCK)
             EXEC dbo.spCSS_StagingTransform_DimSponsor
                  @EDWRunDateTime;


		   -- Get how many rows were extracted from source 

             SELECT @SourceCount = COUNT(1)
             FROM #DimSponsorUpsert;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Source.';
             PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
             CREATE NONCLUSTERED INDEX XAK1DimSponsorUpsert ON #DimSponsorUpsert
             ([CSSCenterNumber] ASC, [CSSFamilyNumber] ASC, [SourceSystem] ASC
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
             MERGE [BING_EDW].[dbo].[DimSponsor] T
             USING #DimSponsorUpsert S
             ON(S.CSSCenterNumber = T.CSSCenterNumber
                AND S.CSSFamilyNumber = T.CSSFamilyNumber -- S.CSSFamilyNumber = S.CSSFamilyNumber -- SB fix 12/13/2017
                AND T.SourceSystem = 'CSS')
                 WHEN MATCHED AND(S.SponsorFirstName <> T.SponsorFirstName
                                  OR S.SponsorMiddleName <> T.SponsorMiddleName
                                  OR S.SponsorLastName <> T.SponsorLastName
                                  OR S.SponsorFullName <> T.SponsorFullName
                                  OR S.SponsorPhonePrimary <> T.SponsorPhonePrimary
                                  OR S.SponsorPhoneSecondary <> T.SponsorPhoneSecondary
                                  OR S.SponsorAddress1 <> T.SponsorAddress1
                                  OR S.SponsorAddress2 <> T.SponsorAddress2
                                  OR S.SponsorCity <> T.SponsorCity
                                  OR S.SponsorState <> T.SponsorState
                                  OR S.SponsorZIP <> T.SponsorZIP
                                  OR S.SponsorGender <> T.SponsorGender
                                  OR S.SponsorInternalEmployee <> T.SponsorInternalEmployee)
                 THEN UPDATE SET
                                 T.SponsorFirstName = S.SponsorFirstName,
                                 T.SponsorMiddleName = S.SponsorMiddleName,
                                 T.SponsorLastName = S.SponsorLastName,
                                 T.SponsorFullName = S.SponsorFullName,
                                 T.SponsorPhonePrimary = S.SponsorPhonePrimary,
                                 T.SponsorPhoneSecondary = S.SponsorPhoneSecondary,
                                 T.SponsorAddress1 = S.SponsorAddress1,
                                 T.SponsorAddress2 = S.SponsorAddress2,
                                 T.SponsorCity = S.SponsorCity,
                                 T.SponsorState = S.SponsorState,
                                 T.SponsorZIP = S.SponsorZIP,
                                 T.SponsorStudentRelationship = S.SponsorStudentRelationship,
                                 T.SponsorGender = S.SponsorGender,
                                 T.SponsorInternalEmployee = S.SponsorInternalEmployee
--T.SponsorStatus = S.SponsorStatus,
--T.SponsorDoNotEmail = S.SponsorDoNotEmail,
--T.SponsorLeadManagementID = S.SponsorLeadManagementID,
--T.CSSCenterNumber = S.CSSCenterNumber,
--T.CSSFamilyNumber = S.CSSFamilyNumber,
--T.SourceSystem = S.SourceSystem,
--T.EDWEffectiveDate = S.EDWEffectiveDate,
--T.EDWCreatedDate = S.EDWCreatedDate,
--T.EDWCreatedBy = S.EDWCreatedBy,
--T.Deleted = S.Deleted
                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT(SponsorID,
                          SponsorFirstName,
                          SponsorMiddleName,
                          SponsorLastName,
                          SponsorFullName,
                          SponsorPhonePrimary,
                          SponsorPhoneSecondary,
                          SponsorPhoneTertiary,
                          SponsorEmailPrimary,
                          SponsorEmailSecondary,
                          SponsorAddress1,
                          SponsorAddress2,
                          SponsorCity,
                          SponsorState,
                          SponsorZIP,
                          SponsorStudentRelationship,
                          SponsorGender,
                          SponsorInternalEmployee,
                          SponsorStatus,
                          SponsorDoNotEmail,
                          SponsorLeadManagementID,
                          CSSCenterNumber,
                          CSSFamilyNumber,
                          SourceSystem,
                          EDWEffectiveDate,
                          EDWCreatedDate,
                          EDWCreatedBy,
                          Deleted)
                   VALUES
             (S.SponsorID,
              S.SponsorFirstName,
              S.SponsorMiddleName,
              S.SponsorLastName,
              S.SponsorFullName,
              S.SponsorPhonePrimary,
              S.SponsorPhoneSecondary,
              S.SponsorPhoneTertiary,
              S.SponsorEmailPrimary,
              S.SponsorEmailSecondary,
              S.SponsorAddress1,
              S.SponsorAddress2,
              S.SponsorCity,
              S.SponsorState,
              S.SponsorZIP,
              S.SponsorStudentRelationship,
              S.SponsorGender,
              S.SponsorInternalEmployee,
              S.SponsorStatus,
              S.SponsorDoNotEmail,
              S.SponsorLeadManagementID,
              S.CSSCenterNumber,
              S.CSSFamilyNumber,
              S.SourceSystem,
              S.EDWEffectiveDate,
              S.EDWCreatedDate,
              S.EDWCreatedBy,
              S.Deleted
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
		   
	
--                 WHEN Not MATCHED By Source AND(T.SourceSystem ='CSS'
--                                  AND T.Deleted IS  NULL)
--                 THEN UPDATE SET
                                
----T.SponsorFirstName = S.SponsorFirstName,
----T.SponsorMiddleName = S.SponsorMiddleName,
----T.SponsorLastName = S.SponsorLastName,
----T.SponsorFullName = S.SponsorFullName,
----T.SponsorPhonePrimary = S.SponsorPhonePrimary,
----T.SponsorPhoneSecondary = S.SponsorPhoneSecondary,
----T.SponsorPhoneTertiary = S.SponsorPhoneTertiary,
----T.SponsorEmailPrimary = S.SponsorEmailPrimary,
----T.SponsorEmailSecondary = S.SponsorEmailSecondary,
----T.SponsorAddress1 = S.SponsorAddress1,
----T.SponsorAddress2 = S.SponsorAddress2,
----T.SponsorCity = S.SponsorCity,
----T.SponsorState = S.SponsorState,
----T.SponsorZIP = S.SponsorZIP,
----T.SponsorStudentRelationship = S.SponsorStudentRelationship,
----T.SponsorGender = S.SponsorGender,
----T.SponsorInternalEmployee = S.SponsorInternalEmployee,
----T.SponsorStatus = S.SponsorStatus,
----T.SponsorDoNotEmail = S.SponsorDoNotEmail,
----T.SponsorLeadManagementID = S.SponsorLeadManagementID,
----T.CSSCenterNumber = S.CSSCenterNumber,
----T.CSSFamilyNumber = S.CSSFamilyNumber,
----T.SourceSystem = S.SourceSystem,
----T.EDWEffectiveDate = S.EDWEffectiveDate,
----T.EDWCreatedDate = S.EDWCreatedDate,
----T.EDWCreatedBy = S.EDWCreatedBy,
--T.Deleted=getdate()

--             OUTPUT $action
--                    INTO @tblDeleteActions;
----

     --        SELECT @DeleteCount = SUM(Updated)
     --        FROM
     --        ( 
			  ---- Count the number of updates 

     --            SELECT COUNT(*) AS Updated
     --            FROM @tblDeleteActions
     --            WHERE MergeAction = 'UPDATE' -- Soft deletes show up as Updates
     --        ) merge_actions;
		   
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
             DROP TABLE #DimSponsorUpsert;

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
		   -- Raise error
		   --			 
             DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
             SELECT @ErrMsg = ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;