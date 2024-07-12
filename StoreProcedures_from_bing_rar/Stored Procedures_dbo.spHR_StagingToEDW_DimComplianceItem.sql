/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spHR_StagingToEDW_DimComplianceItem'
)
    DROP PROCEDURE dbo.spHR_StagingToEDW_DimComplianceItem;
GO
*/
CREATE PROCEDURE [dbo].[spHR_StagingToEDW_DimComplianceItem]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingToEDW_DimComplianceItem
    --
    -- Purpose:            Performs the Insert / Update ETL process for
    --                         the DimComplianceItem table from Staging to BING_EDW.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                                 sub-procedure spHR_StagingTransform_DimComplianceItem, 
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
    -- Usage:              EXEC dbo.spHR_StagingToEDW_DimComplianceItem @DebugMode = 1	
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
    -- 03/13/18    ADevabhakthuni            BNG-552.  DimComplianceItem
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimComplianceItem';
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
             CREATE TABLE #DimComplianceItemUpsert
             (
								 [ComplianceItemID]                   [INT] NOT NULL,
								 [ComplianceItemName]                 [VARCHAR](150) NOT NULL,
								 [ComplianceItemDescription]          [VARCHAR](250) NOT NULL,
								 [ComplianceItemEvaluationMethodCode] [VARCHAR](50) NOT NULL,
								 [ComplianceItemEvaluationMethodName] [VARCHAR](150) NOT NULL,
								 [ComplianceItemFlexAttribute1]       [VARCHAR](150) NULL,
								 [ComplianceItemFlexAttribute2]       [VARCHAR](150) NULL,
								 [ComplianceItemFlexAttribute3]       [VARCHAR](150) NULL,
								 [ComplianceItemFlexAttribute4]       [VARCHAR](150) NULL,
								 [ComplianceItemFlexAttribute5]       [VARCHAR](150) NULL,
								 [ComplianceItemCreatedDate]          [DATETIME2] NOT NULL,
								 [ComplianceItemCreatedUser]          [INT] NOT NULL,
								 [ComplianceItemModifiedDate]         [DATETIME2] NOT NULL,
								 [ComplianceItemModifiedUser]         [INT] NOT NULL,
								 [EDWCreatedDate]                     [DATETIME2] NOT NULL,
								 [EDWModifiedDate]                    [DATETIME2] NOT NULL
									 );          

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table FROM Source, and create any helper indexes
		   --
		   -- ================================================================================
             INSERT INTO #DimComplianceItemUpsert
             EXEC dbo.spHR_StagingTransform_DimComplianceItem
                  @EDWRunDateTime;

		   -- Get how many rows were extracted FROM source 

             SELECT @SourceCount = COUNT(1)
             FROM #DimComplianceItemUpsert;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows FROM Source.';
             PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
             CREATE NONCLUSTERED INDEX XAK1DimComplianceItemUpsert ON #DimComplianceItemUpsert
             ([ComplianceItemID] ASC
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
             MERGE [BING_EDW].[dbo].[DimComplianceItem] T
             USING #DimComplianceItemUpsert S
             ON(S.ComplianceItemID = T.ComplianceItemID)
                 WHEN MATCHED AND (    T.ComplianceItemName <> S.ComplianceItemName
									OR T.ComplianceItemDescription <> S.ComplianceItemDescription
									OR T.ComplianceItemEvaluationMethodCode <> S.ComplianceItemEvaluationMethodCode
									OR T.ComplianceItemEvaluationMethodName <> S.ComplianceItemEvaluationMethodName
									OR T.ComplianceItemFlexAttribute1 <> S.ComplianceItemFlexAttribute1
									OR T.ComplianceItemFlexAttribute2 <> S.ComplianceItemFlexAttribute2
									OR T.ComplianceItemFlexAttribute3 <> S.ComplianceItemFlexAttribute3
									OR T.ComplianceItemFlexAttribute4 <> S.ComplianceItemFlexAttribute4
									OR T.ComplianceItemFlexAttribute5 <> S.ComplianceItemFlexAttribute5
									OR T.ComplianceItemCreatedDate <> S.ComplianceItemCreatedDate
									OR T.ComplianceItemCreatedUser <> S.ComplianceItemCreatedUser
									OR T.ComplianceItemModifiedDate <> S.ComplianceItemModifiedDate
									OR T.ComplianceItemModifiedUser <> S.ComplianceItemModifiedUser



											 )
                 THEN UPDATE SET
									
									T.ComplianceItemName =S.ComplianceItemName ,
									T.ComplianceItemDescription =S.ComplianceItemDescription ,
									T.ComplianceItemEvaluationMethodCode =S.ComplianceItemEvaluationMethodCode ,
									T.ComplianceItemEvaluationMethodName =S.ComplianceItemEvaluationMethodName ,
									T.ComplianceItemFlexAttribute1 =S.ComplianceItemFlexAttribute1 ,
									T.ComplianceItemFlexAttribute2 =S.ComplianceItemFlexAttribute2 ,
									T.ComplianceItemFlexAttribute3 =S.ComplianceItemFlexAttribute3 ,
									T.ComplianceItemFlexAttribute4 =S.ComplianceItemFlexAttribute4 ,
									T.ComplianceItemFlexAttribute5 =S.ComplianceItemFlexAttribute5 ,
									T.ComplianceItemCreatedDate =S.ComplianceItemCreatedDate ,
									T.ComplianceItemCreatedUser =S.ComplianceItemCreatedUser ,
									T.ComplianceItemModifiedDate =S.ComplianceItemModifiedDate ,
									T.ComplianceItemModifiedUser =S.ComplianceItemModifiedUser ,
									T.EDWCreatedDate =S.EDWCreatedDate ,
									T.EDWModifiedDate =S.EDWModifiedDate



                 WHEN NOT MATCHED BY TARGET
                 THEN
						  INSERT(		ComplianceItemID ,
										ComplianceItemName ,
										ComplianceItemDescription ,
										ComplianceItemEvaluationMethodCode ,
										ComplianceItemEvaluationMethodName ,
										ComplianceItemFlexAttribute1 ,
										ComplianceItemFlexAttribute2 ,
										ComplianceItemFlexAttribute3 ,
										ComplianceItemFlexAttribute4 ,
										ComplianceItemFlexAttribute5 ,
										ComplianceItemCreatedDate ,
										ComplianceItemCreatedUser ,
										ComplianceItemModifiedDate ,
										ComplianceItemModifiedUser ,
										EDWCreatedDate ,
										EDWModifiedDate
												 )

				   VALUES(						S.ComplianceItemID ,
												S.ComplianceItemName ,
												S.ComplianceItemDescription ,
												S.ComplianceItemEvaluationMethodCode ,
												S.ComplianceItemEvaluationMethodName ,
												S.ComplianceItemFlexAttribute1 ,
												S.ComplianceItemFlexAttribute2 ,
												S.ComplianceItemFlexAttribute3 ,
												S.ComplianceItemFlexAttribute4 ,
												S.ComplianceItemFlexAttribute5 ,
												S.ComplianceItemCreatedDate ,
												S.ComplianceItemCreatedUser ,
												S.ComplianceItemModifiedDate ,
												S.ComplianceItemModifiedUser ,
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
             DROP TABLE #DimComplianceItemUpsert;

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