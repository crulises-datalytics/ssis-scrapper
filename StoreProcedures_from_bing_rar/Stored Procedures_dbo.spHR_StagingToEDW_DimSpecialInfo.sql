/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spHR_StagingToEDW_DimSpecialInfo'
)
    DROP PROCEDURE dbo.spHR_StagingToEDW_DimSpecialInfo;
GO
*/
CREATE PROCEDURE [dbo].[spHR_StagingToEDW_DimSpecialInfo]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingToEDW_DimSpecialInfo
    --
    -- Purpose:            Performs the Insert / Update ETL process for
    --                         the DimSpecialInfo table from Staging to BING_EDW.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                                 sub-procedure spHR_StagingTransform_DimSpecialInfo, 
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
    -- Usage:              EXEC dbo.spHR_StagingToEDW_DimSpecialInfo @DebugMode = 1	
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
    -- 03/01/18    ADevabhakthuni            BNG-262.  DimSpecialInfo
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimSpecialInfo';
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
             CREATE TABLE #DimSpecialInfoUpsert
             (
						 [SpecialInfoID]           [INT] NOT NULL,
						 [SpecialInfoTypeID]       [INT] NOT NULL,
						 [SpecialInfoTypeName]     [VARCHAR](150) NOT NULL,
						 [SpecialInfoAttribute1]   [VARCHAR](150) NULL,
						 [SpecialInfoAttribute2]   [VARCHAR](150) NULL,
						 [SpecialInfoAttribute3]   [VARCHAR](150) NULL,
						 [SpecialInfoAttribute4]   [VARCHAR](150) NULL,
						 [SpecialInfoAttribute5]   [VARCHAR](150) NULL,
						 [SpecialInfoAttribute6]   [VARCHAR](150) NULL,
						 [SpecialInfoAttribute7]   [VARCHAR](150) NULL,
						 [SpecialInfoAttribute8]   [VARCHAR](150) NULL,
						 [SpecialInfoAttribute9]   [VARCHAR](150) NULL,
						 [SpecialInfoAttribute10]  [VARCHAR](150) NULL,
						 [SpecialInfoAttribute11]  [VARCHAR](150) NULL,
						 [SpecialInfoAttribute12]  [VARCHAR](150) NULL,
						 [SpecialInfoAttribute13]  [VARCHAR](150) NULL,
						 [SpecialInfoAttribute14]  [VARCHAR](150) NULL,
						 [SpecialInfoAttribute15]  [VARCHAR](150) NULL,
						 [SpecialInfoAttribute16]  [VARCHAR](150) NULL,
						 [SpecialInfoAttribute17]  [VARCHAR](150) NULL,
						 [SpecialInfoAttribute18]  [VARCHAR](150) NULL,
						 [SpecialInfoAttribute19]  [VARCHAR](150) NULL,
						 [SpecialInfoAttribute20]  [VARCHAR](150) NULL,
						 [SpecialInfoAttribute21]  [VARCHAR](150) NULL,
						 [SpecialInfoAttribute22]  [VARCHAR](150) NULL,
						 [SpecialInfoAttribute23]  [VARCHAR](150) NULL,
						 [SpecialInfoAttribute24]  [VARCHAR](150) NULL,
						 [SpecialInfoAttribute25]  [VARCHAR](150) NULL,
						 [SpecialInfoAttribute26]  [VARCHAR](150) NULL,
						 [SpecialInfoAttribute27]  [VARCHAR](150) NULL,
						 [SpecialInfoAttribute28]  [VARCHAR](150) NULL,
						 [SpecialInfoAttribute29]  [VARCHAR](150) NULL,
						 [SpecialInfoAttribute30]  [VARCHAR](150) NULL,
						 [SpecialInfoSummaryFlag]  [VARCHAR](150) NOT NULL,
						 [SpecialInfoEnabledFlag]  [VARCHAR](150) NOT NULL,
						[SpecialInfoCreatedUser]  [INT] NOT NULL,
						[SpecialInfoCreatedDate]  [DATETIME2] NOT NULL,
						[SpecialInfoModifiedUser] [INT] NOT NULL,
						[SpecialInfoModifiedDate] [DATETIME2] NOT NULL,
						 [EDWCreatedDate]          [DATETIME2] NOT NULL,
						 [EDWModifiedDate]         [DATETIME2] NOT NULL,
									 );          

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table FROM Source, and create any helper indexes
		   --
		   -- ================================================================================
             INSERT INTO #DimSpecialInfoUpsert
             EXEC dbo.spHR_StagingTransform_DimSpecialInfo
                  @EDWRunDateTime;

		   -- Get how many rows were extracted FROM source 

             SELECT @SourceCount = COUNT(1)
             FROM #DimSpecialInfoUpsert;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows FROM Source.';
             PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
             CREATE NONCLUSTERED INDEX XAK1DimSpecialInfoUpsert ON #DimSpecialInfoUpsert
             ([SpecialInfoID] ASC
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
             MERGE [BING_EDW].[dbo].[DimSpecialInfo] T
             USING #DimSpecialInfoUpsert S
             ON(S.SpecialInfoID = T.SpecialInfoID)
                 WHEN MATCHED AND (			   T.SpecialInfoTypeID <> S.SpecialInfoTypeID
											OR T.SpecialInfoTypeName <> S.SpecialInfoTypeName
											OR T.SpecialInfoAttribute1 <> S.SpecialInfoAttribute1
											OR T.SpecialInfoAttribute2 <> S.SpecialInfoAttribute2
											OR T.SpecialInfoAttribute3 <> S.SpecialInfoAttribute3
											OR T.SpecialInfoAttribute4 <> S.SpecialInfoAttribute4
											OR T.SpecialInfoAttribute5 <> S.SpecialInfoAttribute5
											OR T.SpecialInfoAttribute6 <> S.SpecialInfoAttribute6
											OR T.SpecialInfoAttribute7 <> S.SpecialInfoAttribute7
											OR T.SpecialInfoAttribute8 <> S.SpecialInfoAttribute8
											OR T.SpecialInfoAttribute9 <> S.SpecialInfoAttribute9
											OR T.SpecialInfoAttribute10 <> S.SpecialInfoAttribute10
											OR T.SpecialInfoAttribute11 <> S.SpecialInfoAttribute11
											OR T.SpecialInfoAttribute12 <> S.SpecialInfoAttribute12
											OR T.SpecialInfoAttribute13 <> S.SpecialInfoAttribute13
											OR T.SpecialInfoAttribute14 <> S.SpecialInfoAttribute14
											OR T.SpecialInfoAttribute15 <> S.SpecialInfoAttribute15
											OR T.SpecialInfoAttribute16 <> S.SpecialInfoAttribute16
											OR T.SpecialInfoAttribute17 <> S.SpecialInfoAttribute17
											OR T.SpecialInfoAttribute18 <> S.SpecialInfoAttribute18
											OR T.SpecialInfoAttribute19 <> S.SpecialInfoAttribute19
											OR T.SpecialInfoAttribute20 <> S.SpecialInfoAttribute20
											OR T.SpecialInfoAttribute21 <> S.SpecialInfoAttribute21
											OR T.SpecialInfoAttribute22 <> S.SpecialInfoAttribute22
											OR T.SpecialInfoAttribute23 <> S.SpecialInfoAttribute23
											OR T.SpecialInfoAttribute24 <> S.SpecialInfoAttribute24
											OR T.SpecialInfoAttribute25 <> S.SpecialInfoAttribute25
											OR T.SpecialInfoAttribute26 <> S.SpecialInfoAttribute26
											OR T.SpecialInfoAttribute27 <> S.SpecialInfoAttribute27
											OR T.SpecialInfoAttribute28 <> S.SpecialInfoAttribute28
											OR T.SpecialInfoAttribute29 <> S.SpecialInfoAttribute29
											OR T.SpecialInfoAttribute30 <> S.SpecialInfoAttribute30
											OR T.SpecialInfoSummaryFlag <> S.SpecialInfoSummaryFlag
											OR T.SpecialInfoEnabledFlag <> S.SpecialInfoEnabledFlag
											OR T.SpecialInfoCreatedUser <> S.SpecialInfoCreatedUser
											OR T.SpecialInfoCreatedDate <> S.SpecialInfoCreatedDate
											OR T.SpecialInfoModifiedUser <> S.SpecialInfoModifiedUser
											OR T.SpecialInfoModifiedDate <> S.SpecialInfoModifiedDate

											 )
                 THEN UPDATE SET
													T.SpecialInfoTypeID = S.SpecialInfoTypeID ,
													T.SpecialInfoTypeName = S.SpecialInfoTypeName ,
													T.SpecialInfoAttribute1 = S.SpecialInfoAttribute1 ,
													T.SpecialInfoAttribute2 = S.SpecialInfoAttribute2 ,
													T.SpecialInfoAttribute3 = S.SpecialInfoAttribute3 ,
													T.SpecialInfoAttribute4 = S.SpecialInfoAttribute4 ,
													T.SpecialInfoAttribute5 = S.SpecialInfoAttribute5 ,
													T.SpecialInfoAttribute6 = S.SpecialInfoAttribute6 ,
													T.SpecialInfoAttribute7 = S.SpecialInfoAttribute7 ,
													T.SpecialInfoAttribute8 = S.SpecialInfoAttribute8 ,
													T.SpecialInfoAttribute9 = S.SpecialInfoAttribute9 ,
													T.SpecialInfoAttribute10 = S.SpecialInfoAttribute10 ,
													T.SpecialInfoAttribute11 = S.SpecialInfoAttribute11 ,
													T.SpecialInfoAttribute12 = S.SpecialInfoAttribute12 ,
													T.SpecialInfoAttribute13 = S.SpecialInfoAttribute13 ,
													T.SpecialInfoAttribute14 = S.SpecialInfoAttribute14 ,
													T.SpecialInfoAttribute15 = S.SpecialInfoAttribute15 ,
													T.SpecialInfoAttribute16 = S.SpecialInfoAttribute16 ,
													T.SpecialInfoAttribute17 = S.SpecialInfoAttribute17 ,
													T.SpecialInfoAttribute18 = S.SpecialInfoAttribute18 ,
													T.SpecialInfoAttribute19 = S.SpecialInfoAttribute19 ,
													T.SpecialInfoAttribute20 = S.SpecialInfoAttribute20 ,
													T.SpecialInfoAttribute21 = S.SpecialInfoAttribute21 ,
													T.SpecialInfoAttribute22 = S.SpecialInfoAttribute22 ,
													T.SpecialInfoAttribute23 = S.SpecialInfoAttribute23 ,
													T.SpecialInfoAttribute24 = S.SpecialInfoAttribute24 ,
													T.SpecialInfoAttribute25 = S.SpecialInfoAttribute25 ,
													T.SpecialInfoAttribute26 = S.SpecialInfoAttribute26 ,
													T.SpecialInfoAttribute27 = S.SpecialInfoAttribute27 ,
													T.SpecialInfoAttribute28 = S.SpecialInfoAttribute28 ,
													T.SpecialInfoAttribute29 = S.SpecialInfoAttribute29 ,
													T.SpecialInfoAttribute30 = S.SpecialInfoAttribute30 ,
													T.SpecialInfoSummaryFlag = S.SpecialInfoSummaryFlag ,
													T.SpecialInfoEnabledFlag = S.SpecialInfoEnabledFlag ,
													T.SpecialInfoCreatedUser = S.SpecialInfoCreatedUser ,
													T.SpecialInfoCreatedDate = S.SpecialInfoCreatedDate ,
													T.SpecialInfoModifiedUser = S.SpecialInfoModifiedUser ,
													T.SpecialInfoModifiedDate = S.SpecialInfoModifiedDate ,
													T.EDWCreatedDate = S.EDWCreatedDate ,
													T.EDWModifiedDate = S.EDWModifiedDate 

                 WHEN NOT MATCHED BY TARGET
                 THEN
						  INSERT(				SpecialInfoID ,
												SpecialInfoTypeID ,
												SpecialInfoTypeName ,
												SpecialInfoAttribute1 ,
												SpecialInfoAttribute2 ,
												SpecialInfoAttribute3 ,
												SpecialInfoAttribute4 ,
												SpecialInfoAttribute5 ,
												SpecialInfoAttribute6 ,
												SpecialInfoAttribute7 ,
												SpecialInfoAttribute8 ,
												SpecialInfoAttribute9 ,
												SpecialInfoAttribute10 ,
												SpecialInfoAttribute11 ,
												SpecialInfoAttribute12 ,
												SpecialInfoAttribute13 ,
												SpecialInfoAttribute14 ,
												SpecialInfoAttribute15 ,
												SpecialInfoAttribute16 ,
												SpecialInfoAttribute17 ,
												SpecialInfoAttribute18 ,
												SpecialInfoAttribute19 ,
												SpecialInfoAttribute20 ,
												SpecialInfoAttribute21 ,
												SpecialInfoAttribute22 ,
												SpecialInfoAttribute23 ,
												SpecialInfoAttribute24 ,
												SpecialInfoAttribute25 ,
												SpecialInfoAttribute26 ,
												SpecialInfoAttribute27 ,
												SpecialInfoAttribute28 ,
												SpecialInfoAttribute29 ,
												SpecialInfoAttribute30 ,
												SpecialInfoSummaryFlag ,
												SpecialInfoEnabledFlag ,
												SpecialInfoCreatedUser ,
												SpecialInfoCreatedDate ,
												SpecialInfoModifiedUser ,
												SpecialInfoModifiedDate ,
												EDWCreatedDate ,
												EDWModifiedDate 

												 )

				   VALUES(							S.SpecialInfoID ,
													S.SpecialInfoTypeID ,
													S.SpecialInfoTypeName ,
													S.SpecialInfoAttribute1 ,
													S.SpecialInfoAttribute2 ,
													S.SpecialInfoAttribute3 ,
													S.SpecialInfoAttribute4 ,
													S.SpecialInfoAttribute5 ,
													S.SpecialInfoAttribute6 ,
													S.SpecialInfoAttribute7 ,
													S.SpecialInfoAttribute8 ,
													S.SpecialInfoAttribute9 ,
													S.SpecialInfoAttribute10 ,
													S.SpecialInfoAttribute11 ,
													S.SpecialInfoAttribute12 ,
													S.SpecialInfoAttribute13 ,
													S.SpecialInfoAttribute14 ,
													S.SpecialInfoAttribute15 ,
													S.SpecialInfoAttribute16 ,
													S.SpecialInfoAttribute17 ,
													S.SpecialInfoAttribute18 ,
													S.SpecialInfoAttribute19 ,
													S.SpecialInfoAttribute20 ,
													S.SpecialInfoAttribute21 ,
													S.SpecialInfoAttribute22 ,
													S.SpecialInfoAttribute23 ,
													S.SpecialInfoAttribute24 ,
													S.SpecialInfoAttribute25 ,
													S.SpecialInfoAttribute26 ,
													S.SpecialInfoAttribute27 ,
													S.SpecialInfoAttribute28 ,
													S.SpecialInfoAttribute29 ,
													S.SpecialInfoAttribute30 ,
													S.SpecialInfoSummaryFlag ,
													S.SpecialInfoEnabledFlag ,
													S.SpecialInfoCreatedUser ,
													S.SpecialInfoCreatedDate ,
													S.SpecialInfoModifiedUser ,
													S.SpecialInfoModifiedDate ,
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
             DROP TABLE #DimSpecialInfoUpsert;

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