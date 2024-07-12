CREATE PROCEDURE dbo.spCMS_StagingToEDW_DimStudent
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingToEDW_DimStudent
    --
    -- Purpose:            Performs the Insert / Update (Type 2 SCD) ETL process for
    --                         the DimStudent table from Staging to BING_EDW.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                                 sub-procedure spCMS_StagingTransform_DimStudent, 
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
    -- Usage:              EXEC dbo.spCMS_StagingToEDW_DimStudent @DebugMode = 1
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date        Modified By         Comments
    -- ----        -----------         --------
    --
    -- 12/11/17    sburke              BNG-527 - Refactoring of DimStudent load to move away 
    --                                   from SSIS building temp tables in EDW to using stored
    --                                   proc
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimStudent';
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
          [StudentID]        INT NOT NULL,
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
             CREATE TABLE #DimStudentUpsert
             ([StudentID]                  INT NOT NULL,
              [StudentFirstName]           VARCHAR(100) NOT NULL,
              [StudentMiddleName]          VARCHAR(100) NOT NULL,
              [StudentLastName]            VARCHAR(100) NOT NULL,
              [StudentSuffixName]          VARCHAR(100) NOT NULL,
              [StudentPreferredName]       VARCHAR(50) NOT NULL,
              [StudentFullName]            VARCHAR(400) NOT NULL,
              [StudentBirthDate]           DATE NOT NULL,
              [StudentGender]              VARCHAR(10) NOT NULL,
              [StudentEthnicity]           VARCHAR(50) NOT NULL,
              [StudentStatus]              VARCHAR(100) NOT NULL,
              [StudentLifetimeSubsidy]     VARCHAR(100) NOT NULL,
              [StudentCategory]            VARCHAR(100) NOT NULL,
              [StudentSubCategory]         VARCHAR(100) NOT NULL,
              [StudentPhone]               VARCHAR(20) NOT NULL,
              [StudentAddress1]            VARCHAR(100) NOT NULL,
              [StudentAddress2]            VARCHAR(100) NOT NULL,
              [StudentCity]                VARCHAR(100) NOT NULL,
              [StudentState]               CHAR(2) NOT NULL,
              [StudentZIP]                 VARCHAR(10) NOT NULL,
              [StudentPrimaryLanguage]     VARCHAR(50) NOT NULL,
              [StudentFirstEnrollmentDate] DATE NULL,
              [StudentCareSelectStatus]    VARCHAR(10) NOT NULL,
              [CSSCenterNumber]            VARCHAR(4) NOT NULL,
              [CSSFamilyNumber]            VARCHAR(4) NOT NULL,
              [CSSStudentNumber]           VARCHAR(2) NOT NULL,
              [SourceSystem]               VARCHAR(3) NOT NULL,
              [EDWEffectiveDate]           DATETIME2(7) NOT NULL,
              [EDWEndDate]                 DATETIME2(7) NULL,
              [EDWCreatedDate]             DATETIME2(7) NOT NULL,
              [EDWCreatedBy]               VARCHAR(50) NOT NULL,
              [Deleted]                    DATETIME2(7) NULL,
             );
          

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table from Source, and create any helper indexes
		   --
		   -- ================================================================================
             INSERT INTO #DimStudentUpsert
             EXEC dbo.spCMS_StagingTransform_DimStudent
                  @EDWRunDateTime;

		   -- Get how many rows were extracted from source 

             SELECT @SourceCount = COUNT(1)
             FROM #DimStudentUpsert;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Source.';
             PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
             CREATE NONCLUSTERED INDEX XAK1DimStudentUpsert ON #DimStudentUpsert
             ([StudentID] ASC, [EDWEffectiveDate] ASC
             );


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
             MERGE [BING_EDW].[dbo].[DimStudent] T
             USING #DimStudentUpsert S
             ON(S.StudentID = T.StudentID)
                 WHEN MATCHED AND S.Deleted IS NULL
                                  AND T.SourceSystem = 'CMS'
                                  AND T.EDWEndDate IS NULL -- The 'current' record in target
                                  AND (S.StudentFirstName <> T.StudentFirstName
                                       OR S.StudentMiddleName <> T.StudentMiddleName
                                       OR S.StudentLastName <> T.StudentLastName
                                       OR S.StudentSuffixName <> T.StudentSuffixName
                                       OR S.StudentPreferredName <> T.StudentPreferredName
                                       OR S.StudentFullName <> T.StudentFullName
                                       OR S.StudentBirthDate <> T.StudentBirthDate
                                       OR S.StudentGender <> T.StudentGender
                                       OR S.StudentEthnicity <> T.StudentEthnicity
                                       OR S.StudentStatus <> T.StudentStatus
                                       OR S.StudentLifetimeSubsidy <> T.StudentLifetimeSubsidy
                                       OR S.StudentCategory <> T.StudentCategory
                                       OR S.StudentSubCategory <> T.StudentSubCategory
                                       OR S.StudentPhone <> T.StudentPhone
                                       OR S.StudentAddress1 <> T.StudentAddress1
                                       OR S.StudentAddress2 <> T.StudentAddress2
                                       OR S.StudentCity <> T.StudentCity
                                       OR S.StudentState <> T.StudentState
                                       OR S.StudentZIP <> T.StudentZIP
                                       OR S.StudentPrimaryLanguage <> T.StudentPrimaryLanguage
                                       OR S.StudentFirstEnrollmentDate <> T.StudentFirstEnrollmentDate
                                       OR S.StudentCareSelectStatus <> T.StudentCareSelectStatus
                                       OR S.CSSCenterNumber <> T.CSSCenterNumber
                                       OR S.CSSFamilyNumber <> T.CSSFamilyNumber
                                       OR S.CSSStudentNumber <> T.CSSStudentNumber
                                       OR S.SourceSystem <> T.SourceSystem
                                       OR T.Deleted IS NOT NULL)
                 THEN UPDATE SET
                                 T.EDWEndDate = S.EDWEffectiveDate -- Updates the EDWEndDate from NULL (current) to the current date	
                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT(EDWEndDate,
                          StudentID,
                          StudentFirstName,
                          StudentMiddleName,
                          StudentLastName,
                          StudentSuffixName,
                          StudentPreferredName,
                          StudentFullName,
                          StudentBirthDate,
                          StudentGender,
                          StudentEthnicity,
                          StudentStatus,
                          StudentLifetimeSubsidy,
                          StudentCategory,
                          StudentSubCategory,
                          StudentPhone,
                          StudentAddress1,
                          StudentAddress2,
                          StudentCity,
                          StudentState,
                          StudentZIP,
                          StudentPrimaryLanguage,
                          StudentFirstEnrollmentDate,
                          StudentCareSelectStatus,
                          CSSCenterNumber,
                          CSSFamilyNumber,
                          CSSStudentNumber,
                          SourceSystem,
                          EDWEffectiveDate,
                          EDWCreatedDate,
                          EDWCreatedBy,
                          Deleted)
                   VALUES
             (NULL, -- Updates EDWEndDate so it is the current record
              S.StudentID,
              S.StudentFirstName,
              S.StudentMiddleName,
              S.StudentLastName,
              S.StudentSuffixName,
              S.StudentPreferredName,
              S.StudentFullName,
              S.StudentBirthDate,
              S.StudentGender,
              S.StudentEthnicity,
              S.StudentStatus,
              S.StudentLifetimeSubsidy,
              S.StudentCategory,
              S.StudentSubCategory,
              S.StudentPhone,
              S.StudentAddress1,
              S.StudentAddress2,
              S.StudentCity,
              S.StudentState,
              S.StudentZIP,
              S.StudentPrimaryLanguage,
              S.StudentFirstEnrollmentDate,
              S.StudentCareSelectStatus,
              S.CSSCenterNumber,
              S.CSSFamilyNumber,
              S.CSSStudentNumber,
              S.SourceSystem,
              S.EDWEffectiveDate,
              S.EDWCreatedDate,
              S.EDWCreatedBy,
              S.Deleted
             )
             -- We need to get the details of the records we updated, so we can insert a further row for them as the current row.
             OUTPUT $action,
                    S.StudentID,
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
             INSERT INTO BING_EDW.dbo.DimStudent
             (EDWEndDate,
              StudentID,
              StudentFirstName,
              StudentMiddleName,
              StudentLastName,
              StudentSuffixName,
              StudentPreferredName,
              StudentFullName,
              StudentBirthDate,
              StudentGender,
              StudentEthnicity,
              StudentStatus,
              StudentLifetimeSubsidy,
              StudentCategory,
              StudentSubCategory,
              StudentPhone,
              StudentAddress1,
              StudentAddress2,
              StudentCity,
              StudentState,
              StudentZIP,
              StudentPrimaryLanguage,
              StudentFirstEnrollmentDate,
              StudentCareSelectStatus,
              CSSCenterNumber,
              CSSFamilyNumber,
              CSSStudentNumber,
              SourceSystem,
              EDWEffectiveDate,
              EDWCreatedDate,
              EDWCreatedBy,
              Deleted
             )
                    SELECT NULL, -- [EDWEndDate]                                                      
                           S.StudentID,
                           S.StudentFirstName,
                           S.StudentMiddleName,
                           S.StudentLastName,
                           S.StudentSuffixName,
                           S.StudentPreferredName,
                           S.StudentFullName,
                           S.StudentBirthDate,
                           S.StudentGender,
                           S.StudentEthnicity,
                           S.StudentStatus,
                           S.StudentLifetimeSubsidy,
                           S.StudentCategory,
                           S.StudentSubCategory,
                           S.StudentPhone,
                           S.StudentAddress1,
                           S.StudentAddress2,
                           S.StudentCity,
                           S.StudentState,
                           S.StudentZIP,
                           S.StudentPrimaryLanguage,
                           S.StudentFirstEnrollmentDate,
                           S.StudentCareSelectStatus,
                           S.CSSCenterNumber,
                           S.CSSFamilyNumber,
                           S.CSSStudentNumber,
                           S.SourceSystem,
					  S.EDWEffectiveDate,
                           S.EDWCreatedDate,
                           S.EDWCreatedBy,
                           S.Deleted
                    FROM #DimStudentUpsert S
                         INNER JOIN @tblMrgeActions_SCD2 scd2 ON S.StudentID = scd2.StudentID
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
             DROP TABLE #DimStudentUpsert;

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
GO