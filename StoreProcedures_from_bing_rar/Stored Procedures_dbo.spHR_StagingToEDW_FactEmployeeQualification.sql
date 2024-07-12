/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spHR_StagingToEDW_FactEmployeeQualification'
)
    DROP PROCEDURE dbo.spHR_StagingToEDW_FactEmployeeQualification;
GO
*/
CREATE PROCEDURE [dbo].[spHR_StagingToEDW_FactEmployeeQualification]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingToEDW_FactEmployeeQualification
    --
    -- Purpose:            Performs the Insert / Update ETL process for
    --                         the FactEmployeeQualification table from Staging to BING_EDW.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                                 sub-procedure spHR_StagingTransform_FactEmployeeQualification, 
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
    --                       
    --
    -- Usage:              EXEC dbo.spHR_StagingToEDW_FactEmployeeQualification @DebugMode = 1	
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
    -- 03/20/18    ADevabhakthuni            BNG-959.  FactEmployeeQualification
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'FactEmployeeQualification';
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
             CREATE TABLE #FactEmployeeQualificationUpsert
             (
								 [DateKey]                           [INT] NOT NULL,
								 [QualificationAwardDateKey]         [INT] NOT NULL,
								 [PersonKey]                         [INT] NOT NULL,
								 [QualificationTypeKey]              [INT] NOT NULL,
								 [EmployeeQualificationID]           [INT] NOT NULL,
								 [EmployeeQualificationName]         [VARCHAR](150) NULL,
								 [EmployeeQualificationFlexValue1]   [VARCHAR](150) NULL,
								 [EmployeeQualificationFlexValue2]   [VARCHAR](150) NULL,
								 [EmployeeQualificationFlexValue3]   [VARCHAR](150) NULL,
								 [EmployeeQualificationFlexValue4]   [VARCHAR](150) NULL,
								 [EmployeeQualificationFlexValue5]   [VARCHAR](150) NULL,
								 [EmployeeQualificationCreatedDate]  [DATETIME2] NOT NULL,
								 [EmployeeQualificationCreatedUser]  [INT] NOT NULL,
								 [EmployeeQualificationModifiedDate] [DATETIME2] NOT NULL,
								 [EmployeeQualificationModifiedUser] [INT] NOT NULL,
								 [EDWCreatedDate]                    [DATETIME2] NOT NULL,
								 [EDWModifiedDate]                    [DATETIME2] NOT NULL
									 );          

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table FROM Source, and create any helper indexes
		   --
		   -- ================================================================================
             INSERT INTO #FactEmployeeQualificationUpsert
             EXEC dbo.spHR_StagingTransform_FactEmployeeQualification
                  @EDWRunDateTime;

		   -- Get how many rows were extracted FROM source 

             SELECT @SourceCount = COUNT(1)
             FROM #FactEmployeeQualificationUpsert;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows FROM Source.';
             PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
             CREATE NONCLUSTERED INDEX XAK1FactEmployeeQualificationUpsert ON #FactEmployeeQualificationUpsert
             ([EmployeeQualificationID] ASC
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
             MERGE [BING_EDW].[dbo].[FactEmployeeQualification] T
             USING #FactEmployeeQualificationUpsert S
             ON( S.EmployeeQualificationID = T.EmployeeQualificationID)
                 WHEN MATCHED AND (		   T.DateKey <> S.DateKey
										OR T.QualificationAwardDateKey <> S.QualificationAwardDateKey
										OR T.PersonKey <> S.PersonKey
										OR T.QualificationTypeKey <> S.QualificationTypeKey
										OR ISNULL(T.EmployeeQualificationName,'') <> S.EmployeeQualificationName
										OR ISNULL(T.EmployeeQualificationFlexValue1,'') <> S.EmployeeQualificationFlexValue1
										OR ISNULL(T.EmployeeQualificationFlexValue2,'') <> S.EmployeeQualificationFlexValue2
										OR ISNULL(T.EmployeeQualificationFlexValue3,'') <> S.EmployeeQualificationFlexValue3
										OR ISNULL(T.EmployeeQualificationFlexValue4,'') <> S.EmployeeQualificationFlexValue4
										OR ISNULL(T.EmployeeQualificationFlexValue5,'') <> S.EmployeeQualificationFlexValue5
										OR T.EmployeeQualificationCreatedDate <> S.EmployeeQualificationCreatedDate
										OR T.EmployeeQualificationCreatedUser <> S.EmployeeQualificationCreatedUser
										OR T.EmployeeQualificationModifiedDate <> S.EmployeeQualificationModifiedDate
										OR T.EmployeeQualificationModifiedUser <> S.EmployeeQualificationModifiedUser
											 )
                 THEN UPDATE SET
									
												T.DateKey =S.DateKey ,
												T.QualificationAwardDateKey =S.QualificationAwardDateKey ,
												T.PersonKey =S.PersonKey ,
												T.QualificationTypeKey =S.QualificationTypeKey ,
												T.EmployeeQualificationName =S.EmployeeQualificationName ,
												T.EmployeeQualificationFlexValue1 =S.EmployeeQualificationFlexValue1 ,
												T.EmployeeQualificationFlexValue2 =S.EmployeeQualificationFlexValue2 ,
												T.EmployeeQualificationFlexValue3 =S.EmployeeQualificationFlexValue3 ,
												T.EmployeeQualificationFlexValue4 =S.EmployeeQualificationFlexValue4 ,
												T.EmployeeQualificationFlexValue5 =S.EmployeeQualificationFlexValue5 ,
												T.EmployeeQualificationCreatedDate =S.EmployeeQualificationCreatedDate ,
												T.EmployeeQualificationCreatedUser =S.EmployeeQualificationCreatedUser ,
												T.EmployeeQualificationModifiedDate =S.EmployeeQualificationModifiedDate ,
												T.EmployeeQualificationModifiedUser =S.EmployeeQualificationModifiedUser, 
												T.EDWModifiedDate = S.EDWModifiedDate




                 WHEN NOT MATCHED BY TARGET
                 THEN
						  INSERT(					DateKey ,
													QualificationAwardDateKey ,
													PersonKey ,
													QualificationTypeKey ,
													EmployeeQualificationID ,
													EmployeeQualificationName ,
													EmployeeQualificationFlexValue1 ,
													EmployeeQualificationFlexValue2 ,
													EmployeeQualificationFlexValue3 ,
													EmployeeQualificationFlexValue4 ,
													EmployeeQualificationFlexValue5 ,
													EmployeeQualificationCreatedDate ,
													EmployeeQualificationCreatedUser ,
													EmployeeQualificationModifiedDate ,
													EmployeeQualificationModifiedUser ,
													EDWCreatedDate , 
													EDWModifiedDate
												 )

				   VALUES(							S.DateKey ,
													S.QualificationAwardDateKey ,
													S.PersonKey ,
													S.QualificationTypeKey ,
													S.EmployeeQualificationID ,
													S.EmployeeQualificationName ,
													S.EmployeeQualificationFlexValue1 ,
													S.EmployeeQualificationFlexValue2 ,
													S.EmployeeQualificationFlexValue3 ,
													S.EmployeeQualificationFlexValue4 ,
													S.EmployeeQualificationFlexValue5 ,
													S.EmployeeQualificationCreatedDate ,
													S.EmployeeQualificationCreatedUser ,
													S.EmployeeQualificationModifiedDate ,
													S.EmployeeQualificationModifiedUser ,
													S.EDWCreatedDate ,
													S.EDWModifiedDate
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
             DROP TABLE #FactEmployeeQualificationUpsert;

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
		   -- Raiserror
		   --
             DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
             SELECT @ErrMsg = ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;