/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spHR_StagingToEDW_DimQualificationType'
)
    DROP PROCEDURE dbo.spHR_StagingToEDW_DimQualificationType;
GO
*/
CREATE PROCEDURE [dbo].[spHR_StagingToEDW_DimQualificationType]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingToEDW_DimQualificationType
    --
    -- Purpose:            Performs the Insert / Update ETL process for
    --                         the DimQualificationType table from Staging to BING_EDW.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                                 sub-procedure spHR_StagingTransform_DimQualificationType, 
    --                                 and create any helper indexes
	--						   Step 3: Insert Sedd rows into Bing_Edw.dbo.DimQualificationType
    --                         Step 4: Perform the Insert / Update (Merge) required 
    --                                 for this EDW table load			 
    --                         Step 5: Execute any automated tests associated with this EDW table load
    --                         Step 6: Output Source / Insert / Update counts to caller, 
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
    -- Usage:              EXEC dbo.spHR_StagingToEDW_DimQualificationType @DebugMode = 1	
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
    -- 03/15/18    ADevabhakthuni            BNG-551.  DimQualificationType
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimQualificationType';
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
		 DECLARE @SeedRowCount INT= 0;

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
             CREATE TABLE #DimQualificationTypeUpsert
             (
								 [QualificationTypeID]                   [INT] NOT NULL,
								 [QualificationTypeName]                 [VARCHAR](150) NOT NULL,
								 [QualificationCategoryCode] [VARCHAR](50) NOT NULL,
								 [QualificationCategoryName] [VARCHAR](150) NOT NULL,
								 [QualificationTypeFlexAttribute1]       [VARCHAR](150) NULL,
								 [QualificationTypeFlexAttribute2]       [VARCHAR](150) NULL,
								 [QualificationTypeFlexAttribute3]       [VARCHAR](150) NULL,
								 [QualificationTypeFlexAttribute4]       [VARCHAR](150) NULL,
								 [QualificationTypeFlexAttribute5]       [VARCHAR](150) NULL,
								 [QualificationTypeCreatedDate]          [DATETIME2] NOT NULL,
								 [QualificationTypeCreatedUser]          [INT] NOT NULL,
								 [QualificationTypeModifiedDate]         [DATETIME2] NOT NULL,
								 [QualificationTypeModifiedUser]         [INT] NOT NULL,
								 [EDWCreatedDate]                     [DATETIME2] NOT NULL,
								 [EDWModifiedDate]                    [DATETIME2] NOT NULL
									 );       
									 
			--=================================================================================   

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table FROM Source, and create any helper indexes
		   --
		   -- ================================================================================
             INSERT INTO #DimQualificationTypeUpsert
             EXEC dbo.spHR_StagingTransform_DimQualificationType
                  @EDWRunDateTime;

		   -- Get how many rows were extracted FROM source 

             SELECT @SourceCount = COUNT(1)
             FROM #DimQualificationTypeUpsert;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows FROM Source.';
             PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
             CREATE NONCLUSTERED INDEX XAK1DimQualificationTypeUpsert ON #DimQualificationTypeUpsert
             ([QualificationTypeID] ASC
             );
		   -- ================================================================================
		   --
		   --S T E P   3.
		   --
		   --Insert Seed Rows into DIMQualificationType
		   --
		   -- ================================================================================
		     IF NOT EXISTS ( Select *  FROM [BING_EDW].[dbo].[DimQualificationType]
             WHERE [QualificationTypeKey] < 0)
			 BEGIN 
			 SET IDENTITY_INSERT [BING_EDW].[dbo].[DimQualificationType] ON;
			 INSERT Into BING_EDW.[dbo].[DimQualificationType] 
								(		[QualificationTypeKey],
										[QualificationTypeID] ,
										[QualificationTypeName] ,
										[QualificationCategoryCode] ,
										[QualificationCategoryName] ,
										[QualificationTypeFlexAttribute1] ,
										[QualificationTypeFlexAttribute2] ,
										[QualificationTypeFlexAttribute3] ,
										[QualificationTypeFlexAttribute4] ,
										[QualificationTypeFlexAttribute5] ,
										[QualificationTypeCreatedDate] ,
										[QualificationTypeCreatedUser] ,
										[QualificationTypeModifiedDate] ,
										[QualificationTypeModifiedUser] ,
										[EDWCreatedDate] ,
										[EDWModifiedDate]
												 )
									SELECT 
											-1,
											-1,
											'Unknown Qualification Type',
											-1,
											'Unknown Qualification Category',
											null,
											null,
											null,
											null,
											null,
											'1/1/1900',
											-1,
											'1/1/1900',
											-1,
											@EDWRunDateTime,
											@EDWRunDateTime
								UNION
								SELECT 
											-2,
											-2,
											'Not Applicable Qualification Type',
											-2,
											'Not Applicable Qualification Category',
											null,
											null,
											null,
											null,
											null,
											'1/1/1900',
											-1,
											'1/1/1900',
											-1,
											@EDWRunDateTime,
											@EDWRunDateTime
									SET @SeedRowCount=@@ROWCOUNT;
									SET IDENTITY_INSERT [BING_EDW].[dbo].[DimQualificationType] OFF;
									END;




		   -- ================================================================================
		   --
		   -- S T E P   4.
		   --
		   -- Perform the Insert / Update (Merge) / deletes required for this EDW table load
		   --
		   -- ================================================================================

		   --
		   -- Perform the Merge statement for insert / updates
		   --
             MERGE [BING_EDW].[dbo].[DimQualificationType] T
             USING #DimQualificationTypeUpsert S
             ON(S.QualificationTypeID = T.QualificationTypeID)
                 WHEN MATCHED AND (    T.QualificationTypeName <> S.QualificationTypeName
									OR T.QualificationCategoryCode <> S.QualificationCategoryCode
									OR T.QualificationCategoryName <> S.QualificationCategoryName
									OR T.QualificationTypeFlexAttribute1 <> S.QualificationTypeFlexAttribute1
									OR T.QualificationTypeFlexAttribute2 <> S.QualificationTypeFlexAttribute2
									OR T.QualificationTypeFlexAttribute3 <> S.QualificationTypeFlexAttribute3
									OR T.QualificationTypeFlexAttribute4 <> S.QualificationTypeFlexAttribute4
									OR T.QualificationTypeFlexAttribute5 <> S.QualificationTypeFlexAttribute5
									OR T.QualificationTypeCreatedDate <> S.QualificationTypeCreatedDate
									OR T.QualificationTypeCreatedUser <> S.QualificationTypeCreatedUser
									OR T.QualificationTypeModifiedDate <> S.QualificationTypeModifiedDate
									OR T.QualificationTypeModifiedUser <> S.QualificationTypeModifiedUser



											 )
                 THEN UPDATE SET
									
									T.QualificationTypeName =S.QualificationTypeName ,
									T.QualificationCategoryCode =S.QualificationCategoryCode ,
									T.QualificationCategoryName =S.QualificationCategoryName ,
									T.QualificationTypeFlexAttribute1 =S.QualificationTypeFlexAttribute1 ,
									T.QualificationTypeFlexAttribute2 =S.QualificationTypeFlexAttribute2 ,
									T.QualificationTypeFlexAttribute3 =S.QualificationTypeFlexAttribute3 ,
									T.QualificationTypeFlexAttribute4 =S.QualificationTypeFlexAttribute4 ,
									T.QualificationTypeFlexAttribute5 =S.QualificationTypeFlexAttribute5 ,
									T.QualificationTypeCreatedDate =S.QualificationTypeCreatedDate ,
									T.QualificationTypeCreatedUser =S.QualificationTypeCreatedUser ,
									T.QualificationTypeModifiedDate =S.QualificationTypeModifiedDate ,
									T.QualificationTypeModifiedUser =S.QualificationTypeModifiedUser ,
									T.EDWCreatedDate =S.EDWCreatedDate ,
									T.EDWModifiedDate =S.EDWModifiedDate



                 WHEN NOT MATCHED BY TARGET
                 THEN
						  INSERT(		QualificationTypeID ,
										QualificationTypeName ,
										QualificationCategoryCode ,
										QualificationCategoryName ,
										QualificationTypeFlexAttribute1 ,
										QualificationTypeFlexAttribute2 ,
										QualificationTypeFlexAttribute3 ,
										QualificationTypeFlexAttribute4 ,
										QualificationTypeFlexAttribute5 ,
										QualificationTypeCreatedDate ,
										QualificationTypeCreatedUser ,
										QualificationTypeModifiedDate ,
										QualificationTypeModifiedUser ,
										EDWCreatedDate ,
										EDWModifiedDate
												 )

				   VALUES(						S.QualificationTypeID ,
												S.QualificationTypeName ,
												S.QualificationCategoryCode ,
												S.QualificationCategoryName ,
												S.QualificationTypeFlexAttribute1 ,
												S.QualificationTypeFlexAttribute2 ,
												S.QualificationTypeFlexAttribute3 ,
												S.QualificationTypeFlexAttribute4 ,
												S.QualificationTypeFlexAttribute5 ,
												S.QualificationTypeCreatedDate ,
												S.QualificationTypeCreatedUser ,
												S.QualificationTypeModifiedDate ,
												S.QualificationTypeModifiedUser ,
												S.EDWCreatedDate ,
												S.EDWModifiedDate


													 )
             OUTPUT $action
                    INTO @tblMergeActions;
             SELECT @InsertCount = SUM(Inserted)+@SeedRowCount,
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
             DROP TABLE #DimQualificationTypeUpsert;

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