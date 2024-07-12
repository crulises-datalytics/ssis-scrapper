
/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spHR_StagingToEDW_FactEmployeeCompliance'
)
    DROP PROCEDURE dbo.spHR_StagingToEDW_FactEmployeeCompliance;
GO
*/

CREATE PROCEDURE [dbo].[spHR_StagingToEDW_FactEmployeeCompliance]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingToEDW_FactEmployeeCompliance
    --
    -- Purpose:            Performs the Insert / Update ETL process for
    --                         the FactEmployeeCompliance table from Staging to BING_EDW.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                                 sub-procedure spHR_StagingTransform_FactEmployeeCompliance, 
    --                                 and create any helper indexes
    --                         Step 3: Perform the Insert / Update (Merge) required 
    --                                 for this EDW table load			 
    --                         Step 4: Execute any automated tests associated with this EDW table load
    --                         Step 5: Output Source / Insert / Update counts to caller, 
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
    -- Usage:              EXEC dbo.spHR_StagingToEDW_FactEmployeeCompliance @DebugMode = 1	
    -- 
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date        Modified By         Comments
    -- ----        -----------         --------
    --
    -- 03/19/18    valimineti            BNG-277.  FactEmployeeCompliance
    --			 
    -- ================================================================================
         BEGIN
             SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
             DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
             DECLARE @DebugMsg NVARCHAR(500);
             DECLARE @SourceName VARCHAR(100)= 'FactEmployeeCompliance';
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
	    -- Extract FROM Source, Upserts contained in a single transaction.  
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
                 CREATE TABLE #FactEmployeeComplianceUpsert
				    ([EmployeeComplianceEffectiveDateKey]  INT NOT NULL,
					[EmployeeComplianceEndDateKey]        INT NOT NULL,
					[EmployeeComplianceCurrentRecordFlag] VARCHAR(1) NOT NULL,
					[PersonKey]                           INT NOT NULL,
					[ComplianceItemKey]                   INT NOT NULL,
					[ComplianceRatingKey]                 INT NOT NULL,
					[EmployeeComplianceID]                INT NOT NULL,
					[ComplianceValue1]                    VARCHAR(150) NULL,
					[ComplianceValue2]                    VARCHAR(150) NULL,
					[ComplianceValue3]                    VARCHAR(150) NULL,
					[ComplianceValue4]                    VARCHAR(150) NULL,
					[ComplianceValue5]                    VARCHAR(150) NULL,
					[ComplianceValue6]                    VARCHAR(150) NULL,
					[ComplianceValue7]                    VARCHAR(150) NULL,
					[ComplianceValue8]                    VARCHAR(150) NULL,
					[ComplianceValue9]                    VARCHAR(150) NULL,
					[ComplianceValue10]                   VARCHAR(150) NULL,
					[ComplianceValue11]                   VARCHAR(150) NULL,
					[ComplianceValue12]                   VARCHAR(150) NULL,
					[ComplianceValue13]                   VARCHAR(150) NULL,
					[ComplianceValue14]                   VARCHAR(150) NULL,
					[ComplianceValue15]                   VARCHAR(150) NULL,
					[ComplianceValue16]                   VARCHAR(150) NULL,
					[ComplianceValue17]                   VARCHAR(150) NULL,
					[ComplianceValue18]                   VARCHAR(150) NULL,
					[ComplianceValue19]                   VARCHAR(150) NULL,
					[ComplianceValue20]                   VARCHAR(150) NULL,
					[ComplianceCreatedDate]               DATETIME2 NOT NULL,
					[ComplianceCreatedUser]               INT NOT NULL,
					[ComplianceModifiedDate]              DATETIME2 NOT NULL,
					[ComplianceModifiedUser]              INT NOT NULL,
					[EDWCreatedDate]                      DATETIME2 NOT NULL,
					[EDWModifiedDate]                     DATETIME2 NOT NULL
				    );          

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table FROM Source, and create any helper indexes
		   --
		   -- ================================================================================
                 INSERT INTO #FactEmployeeComplianceUpsert
                 EXEC dbo.spHR_StagingTransform_FactEmployeeCompliance 
                      @EDWRunDateTime;

		   -- Get how many rows were extracted FROM source 

                 SELECT @SourceCount = COUNT(1)
                 FROM #FactEmployeeComplianceUpsert;
		   
		   -- Debug output progress
                 IF @DebugMode = 1
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows FROM Source.';
                 PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
                 CREATE UNIQUE NONCLUSTERED INDEX XAK1FactEmployeeComplianceUpsert ON #FactEmployeeComplianceUpsert
			 ([EmployeeComplianceID] ASC);

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
                 MERGE [BING_EDW].[dbo].[FactEmployeeCompliance] T
                 USING #FactEmployeeComplianceUpsert S
                 ON(S.EmployeeComplianceID = T.EmployeeComplianceID)
                     WHEN MATCHED AND(T.EmployeeComplianceEffectiveDateKey <> S.EmployeeComplianceEffectiveDateKey
                                      OR T.EmployeeComplianceEndDateKey <> S.EmployeeComplianceEndDateKey
							   OR T.EmployeeComplianceCurrentRecordFlag <> S.EmployeeComplianceCurrentRecordFlag
                                      OR T.PersonKey <> S.PersonKey
                                      OR T.ComplianceItemKey <> S.ComplianceItemKey
                                      OR T.ComplianceRatingKey <> S.ComplianceRatingKey
							   OR T.ComplianceValue1 <> S.ComplianceValue1
							   OR T.ComplianceValue2 <> S.ComplianceValue2
							   OR T.ComplianceValue3 <> S.ComplianceValue3
							   OR T.ComplianceValue4 <> S.ComplianceValue4
							   OR T.ComplianceValue5 <> S.ComplianceValue5
							   OR T.ComplianceValue6 <> S.ComplianceValue6
							   OR T.ComplianceValue7 <> S.ComplianceValue7
							   OR T.ComplianceValue8 <> S.ComplianceValue8
							   OR T.ComplianceValue9 <> S.ComplianceValue9
							   OR T.ComplianceValue10 <> S.ComplianceValue10
							   OR T.ComplianceValue11 <> S.ComplianceValue11
							   OR T.ComplianceValue12 <> S.ComplianceValue12
							   OR T.ComplianceValue13 <> S.ComplianceValue13
							   OR T.ComplianceValue14 <> S.ComplianceValue14
							   OR T.ComplianceValue15 <> S.ComplianceValue15
							   OR T.ComplianceValue16 <> S.ComplianceValue16
							   OR T.ComplianceValue17 <> S.ComplianceValue17
							   OR T.ComplianceValue18 <> S.ComplianceValue18
							   OR T.ComplianceValue19 <> S.ComplianceValue19
							   OR T.ComplianceValue20 <> S.ComplianceValue20
                                      OR T.ComplianceCreatedDate <> S.ComplianceCreatedDate
                                      OR T.ComplianceCreatedUser <> S.ComplianceCreatedUser
                                      OR T.ComplianceModifiedDate <> S.ComplianceModifiedDate
                                      OR T.ComplianceModifiedUser <> S.ComplianceModifiedUser)
                     THEN UPDATE SET
                                     T.EmployeeComplianceEffectiveDateKey = S.EmployeeComplianceEffectiveDateKey,
                                     T.EmployeeComplianceEndDateKey = S.EmployeeComplianceEndDateKey,
							  T.EmployeeComplianceCurrentRecordFlag = S.EmployeeComplianceCurrentRecordFlag,
                                     T.PersonKey = S.PersonKey,
                                     T.ComplianceItemKey = S.ComplianceItemKey,
                                     T.ComplianceRatingKey = S.ComplianceRatingKey,
							  T.ComplianceValue1 = S.ComplianceValue1,
							  T.ComplianceValue2 = S.ComplianceValue2,
							  T.ComplianceValue3 = S.ComplianceValue3,
							  T.ComplianceValue4 = S.ComplianceValue4,
							  T.ComplianceValue5 = S.ComplianceValue5,
							  T.ComplianceValue6 = S.ComplianceValue6,
							  T.ComplianceValue7 = S.ComplianceValue7,
							  T.ComplianceValue8 = S.ComplianceValue8,
							  T.ComplianceValue9 = S.ComplianceValue9,
							  T.ComplianceValue10 = S.ComplianceValue10,
							  T.ComplianceValue11 = S.ComplianceValue11,
							  T.ComplianceValue12 = S.ComplianceValue12,
							  T.ComplianceValue13 = S.ComplianceValue13,
							  T.ComplianceValue14 = S.ComplianceValue14,
							  T.ComplianceValue15 = S.ComplianceValue15,
							  T.ComplianceValue16 = S.ComplianceValue16,
							  T.ComplianceValue17 = S.ComplianceValue17,
							  T.ComplianceValue18 = S.ComplianceValue18,
							  T.ComplianceValue19 = S.ComplianceValue19,
							  T.ComplianceValue20 = S.ComplianceValue20,
                                     T.ComplianceCreatedDate = S.ComplianceCreatedDate,
                                     T.ComplianceCreatedUser = S.ComplianceCreatedUser,
                                     T.ComplianceModifiedDate = S.ComplianceModifiedDate,
                                     T.ComplianceModifiedUser = S.ComplianceModifiedUser,
                                     T.EDWModifiedDate = S.EDWModifiedDate
                     WHEN NOT MATCHED BY TARGET
                     THEN
                       INSERT	  (EmployeeComplianceEffectiveDateKey
						  ,EmployeeComplianceEndDateKey
						  ,EmployeeComplianceCurrentRecordFlag
						  ,PersonKey
						  ,ComplianceItemKey
						  ,ComplianceRatingKey
						  ,EmployeeComplianceID
						  ,ComplianceValue1
						  ,ComplianceValue2
						  ,ComplianceValue3
						  ,ComplianceValue4
						  ,ComplianceValue5
						  ,ComplianceValue6
						  ,ComplianceValue7
						  ,ComplianceValue8
						  ,ComplianceValue9
						  ,ComplianceValue10
						  ,ComplianceValue11
						  ,ComplianceValue12
						  ,ComplianceValue13
						  ,ComplianceValue14
						  ,ComplianceValue15
						  ,ComplianceValue16
						  ,ComplianceValue17
						  ,ComplianceValue18
						  ,ComplianceValue19
						  ,ComplianceValue20
						  ,ComplianceCreatedDate
						  ,ComplianceCreatedUser
						  ,ComplianceModifiedDate
						  ,ComplianceModifiedUser
						  ,EDWCreatedDate
						  ,EDWModifiedDate)
                       VALUES
					   (	   S.EmployeeComplianceEffectiveDateKey
						  ,S.EmployeeComplianceEndDateKey
						  ,S.EmployeeComplianceCurrentRecordFlag
						  ,S.PersonKey
						  ,S.ComplianceItemKey
						  ,S.ComplianceRatingKey
						  ,S.EmployeeComplianceID
						  ,S.ComplianceValue1
						  ,S.ComplianceValue2
						  ,S.ComplianceValue3
						  ,S.ComplianceValue4
						  ,S.ComplianceValue5
						  ,S.ComplianceValue6
						  ,S.ComplianceValue7
						  ,S.ComplianceValue8
						  ,S.ComplianceValue9
						  ,S.ComplianceValue10
						  ,S.ComplianceValue11
						  ,S.ComplianceValue12
						  ,S.ComplianceValue13
						  ,S.ComplianceValue14
						  ,S.ComplianceValue15
						  ,S.ComplianceValue16
						  ,S.ComplianceValue17
						  ,S.ComplianceValue18
						  ,S.ComplianceValue19
						  ,S.ComplianceValue20
						  ,S.ComplianceCreatedDate
						  ,S.ComplianceCreatedUser
						  ,S.ComplianceModifiedDate
						  ,S.ComplianceModifiedUser
						  ,S.EDWCreatedDate
						  ,S.EDWModifiedDate
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
             

		   -- Debug output progress
                 IF @DebugMode = 1
                     BEGIN
                         SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Soft Deleted '+CONVERT(NVARCHAR(20), @DeleteCount)+' FROM into Target.';
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
                 DROP TABLE #FactEmployeeComplianceUpsert;

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
                 EXEC dbo.spHR_StagingEDWETLBatchControl
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