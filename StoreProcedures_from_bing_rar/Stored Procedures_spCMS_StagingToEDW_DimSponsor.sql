

CREATE PROCEDURE [dbo].[spCMS_StagingToEDW_DimSponsor]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingToEDW_DimSponosr
    --
    -- Purpose:            Performs the Insert / Update (Type 2 SCD) ETL process for
    --                         the DimSponsortable from Staging to BING_EDW.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                                 sub-procedure spCMS_StagingTransform_DimSponsor, 
    --                                 and create any helper indexes
    --                         Step 3: Perform the Insert / Update (SCD2) required for this EDW
    --                                 table load
    --                             (a) Perform a Merge that inserts new rows, and updates any existing 
    --                                 current rows to be a previous version
    --                             (b) For any updated records from step 3(a), we insert those rows to 
    --                                 create a new, additional current record, in-line with a 
    --                                 Type 2 Slowly Changing Dimension				 
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
    -- Usage:              EXEC dbo.spCMS_StagingToEDW_DimSponsor @DebugMode = 1
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date        Modified By         Comments
    -- ----        -----------         --------
    --
    -- 12/12/17    ADevabhakthuni     BNG-258 - Refactoring of DimSponsor load to move away 
    --                                   from SSIS building temp tables in EDW to using stored
    --                                   proc
    -- 08/29/22      hhebbalu           Added PartnerID BI-6478				 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimSponsor_CMS';
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
	    -- Merge statement action table variable - for SCD2 we add the unique key columns inaddition to the action
	    --
         DECLARE @tblMrgeActions_SCD2 TABLE
         ([MergeAction]      VARCHAR(250) NOT NULL,
	    -- Column(s) that make up the unique business key for the table we are loading
          [SponsorID]        INT NOT NULL,
          [EDWEffectiveDate] DATETIME2(7) NOT NULL
         );

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
             (
				[SponsorID] [int] NOT NULL,
				[PartnerID] [INT] NOT NULL,
				[SponsorFirstName] [varchar](100) NOT NULL,
				[SponsorMiddleName] [varchar](100) NOT NULL,
				[SponsorLastName] [varchar](100) NOT NULL,
				[SponsorFullName] [varchar](400) NOT NULL,
				[SponsorPhonePrimary] [varchar](20) NOT NULL,
				[SponsorPhoneSecondary] [varchar](20) NOT NULL,
				[SponsorPhoneTertiary] [varchar](20) NOT NULL,
				[SponsorEmailPrimary] [varchar](100) NOT NULL,
				[SponsorEmailSecondary] [varchar](100) NOT NULL,
				[SponsorAddress1] [varchar](100) NOT NULL,
				[SponsorAddress2] [varchar](100) NOT NULL,
				[SponsorCity] [varchar](100) NOT NULL,
				[SponsorState] [char](2) NOT NULL,
				[SponsorZIP] [varchar](10) NOT NULL,
				[SponsorStudentRelationship] [varchar](100) NOT NULL,
				[SponsorGender] [varchar](10) NOT NULL,
				[SponsorInternalEmployee] [varchar](50) NOT NULL,
				[SponsorStatus] [varchar](100) NOT NULL,
				[SponsorDoNotEmail] [varchar](25) NOT NULL,
				[SponsorLeadManagementID] [varchar](100) NOT NULL,
				[CSSCenterNumber] [varchar](4) NOT NULL,
				[CSSFamilyNumber] [varchar](4) NOT NULL,
				[SourceSystem] [varchar](3) NOT NULL,
                [EDWEffectiveDate] [DATETIME2](7) NOT NULL,
                [EDWEndDate]       [DATETIME2](7) NULL,
                [EDWCreatedDate]   [DATETIME2](7) NOT NULL,
                [EDWCreatedBy]     [VARCHAR](50) NOT NULL,
                [Deleted]          [DATETIME2](7) NULL,
                );
          

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table from Source, and create any helper indexes
		   --
		   -- ================================================================================
             INSERT INTO #DimSponsorUpsert
             EXEC dbo.spCMS_StagingTransform_DimSponsor
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
             ([SponsorID] ASC, [EDWEffectiveDate] ASC
             );

		   
		   --update #DimSponsorUpsert set SponsorMiddleName = 'Update 1' where SponsorID = 10001


		   -- ================================================================================	
		   --
		   -- S T E P   3.
		   --
		   -- Perform the Inserts for new records, and SCD Type 2 for updated records.
		   --
		   -- The first MERGE statement performs the inserts for any new rows, and the first
		   -- part of the SCD2 update process for changed existing records, but setting the
		   -- EDWEndDate to the current run-date (an EDWEndDate of NULL means it is the current
		   -- record.
		   --
		   -- After the initial merge has completed, we collect the details of the updates from 
		   -- $action and use that to execute a second insert into the target table, this time 
		   -- creating a new record for each updated record, with an EDW EffectiveDate of the
		   -- current run date, and an EDWEndDate of NLL (current record).
		   --
		   -- ================================================================================
		   
		   --
		   -- Perform the Merge statement for insert / updates
		   --
             MERGE [BING_EDW].[dbo].[DimSponsor] T
             USING #DimSponsorUpsert S
             ON(S.SponsorID = T.SponsorID)
                 WHEN MATCHED AND S.Deleted IS NULL
                                  AND T.SourceSystem = 'CMS'
                                  AND T.EDWEndDate IS NULL -- The 'current' record in target
                                  AND (S.PartnerID <> T.PartnerID
							  OR S.SponsorFirstName <> T.SponsorFirstName
							  OR S.SponsorMiddleName <> T.SponsorMiddleName
							  OR S.SponsorLastName <> T.SponsorLastName
							  OR S.SponsorFullName <> T.SponsorFullName
							  OR S.SponsorPhonePrimary <> T.SponsorPhonePrimary
							  OR S.SponsorPhoneSecondary <> T.SponsorPhoneSecondary
							  OR S.SponsorPhoneTertiary <> T.SponsorPhoneTertiary
							  OR S.SponsorEmailPrimary <> T.SponsorEmailPrimary
							  OR S.SponsorEmailSecondary <> T.SponsorEmailSecondary
							  OR S.SponsorAddress1 <> T.SponsorAddress1
							  OR S.SponsorAddress2 <> T.SponsorAddress2
							  OR S.SponsorCity <> T.SponsorCity
							  OR S.SponsorState <> T.SponsorState
							  OR S.SponsorZIP <> T.SponsorZIP
							  OR S.SponsorGender <> T.SponsorGender
							  OR S.SponsorInternalEmployee <> T.SponsorInternalEmployee
							  OR S.SponsorStatus <> T.SponsorStatus
							  OR S.SponsorDoNotEmail <> T.SponsorDoNotEmail
							  OR S.SponsorLeadManagementID <> T.SponsorLeadManagementID
                              OR S.CSSCenterNumber <> T.CSSCenterNumber
                              OR S.CSSFamilyNumber <> T.CSSFamilyNumber
                              OR S.SourceSystem <> T.SourceSystem
                              OR T.Deleted IS NOT NULL)
                 THEN UPDATE SET
                                 T.EDWEndDate = S.EDWEffectiveDate -- Updates the EDWEndDate from NULL (current) to the current date	
                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT(EDWEndDate,
                         SponsorID,
						 PartnerID,
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
             (NULL, -- Updates EDWEndDate so it is the current record
			  S.SponsorID,
			  S.PartnerID,
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
             -- We need to get the details of the records we updated, so we can insert a further row for them as the current row.
             OUTPUT $action,
                    S.SponsorID,
                    S.EDWEffectiveDate
                    INTO @tblMrgeActions_SCD2;
	         --

             SELECT @InsertCount = SUM(Inserted),
                    @UpdateCount = SUM(Updated)
             FROM
             ( 
		         -- Count the number of inserts 

                 SELECT COUNT(*) AS Inserted,
                        0 AS Updated
                 FROM @tblMrgeActions_SCD2
                 WHERE MergeAction = 'INSERT'
                 UNION ALL 
			     -- Count the number of updates

                 SELECT 0 AS Inserted,
                        COUNT(*) AS Updated
                 FROM @tblMrgeActions_SCD2
                 WHERE MergeAction = 'UPDATE'
             ) merge_actions;
             --
		   
		   
		     -- Debug output progress
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(NVARCHAR(20), @InsertCount)+' rows into Target.';
                     PRINT @DebugMsg;
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Updated [Closed-out previous version] '+CONVERT(NVARCHAR(20), @UpdateCount)+' rows into Target.';
                     PRINT @DebugMsg;
             END;             
		   
		   --
		   -- Perform the Insert for new updated records for Type 2 SCD
		   --
             INSERT INTO BING_EDW.dbo.DimSponsor
             (			 EDWEndDate,
						 SponsorID,
						 PartnerID,
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
						 Deleted
						   )
				        SELECT NULL, -- [EDWEndDate]                                                      
					 S.SponsorID,
					 S.PartnerID,
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
                    FROM #DimSponsorUpsert S
                         INNER JOIN @tblMrgeActions_SCD2 scd2 ON S.SponsorID = scd2.SponsorID
                                                                 AND s.EDWEffectiveDate = scd2.EDWEffectiveDate
                    WHERE scd2.MergeAction = 'UPDATE';
             SELECT @UpdateCount = @@ROWCOUNT;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Updated [Inserted new current SCD2 row] '+CONVERT(NVARCHAR(20), @UpdateCount)+' from into Target.';
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