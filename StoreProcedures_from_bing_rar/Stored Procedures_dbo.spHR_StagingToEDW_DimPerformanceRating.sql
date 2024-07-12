/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spHR_StagingToEDW_DimPerformanceRating'
)
    DROP PROCEDURE dbo.spHR_StagingToEDW_DimPerformanceRating;
GO
*/
CREATE PROCEDURE [dbo].[spHR_StagingToEDW_DimPerformanceRating]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingToEDW_DimPerformanceRating
    --
    -- Purpose:            Performs the Insert / Update ETL process for
    --                         the DimPerformanceRating table from Staging to BING_EDW.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                                 sub-procedure spHR_StagingTransform_DimPerformanceRating, 
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
    -- Usage:              EXEC dbo.spHR_StagingToEDW_DimPerformanceRating @DebugMode = 1	
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
    -- 3/6/18    valimineti            BNG-553.  DimPerformanceRating
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimPerformanceRating';
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
		 DECLARE @SeedRowCount INT=0;

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
             CREATE TABLE #DimPerformanceRatingUpsert
             (
				 	[PerformanceRatingCode] [varchar](5) NOT NULL,
					[PerformanceRatingName] [varchar](150) NOT NULL,
					[PerformanceRatingFlexAttribute1] [varchar](150) NULL,
					[PerformanceRatingFlexAttribute2] [varchar](150) NULL,
					[PerformanceRatingFlexAttribute3] [varchar](150) NULL,
					[PerformanceRatingFlexAttribute4] [varchar](150) NULL,
					[PerformanceRatingFlexAttribute5] [varchar](150) NULL,
					[PerformanceRatingCreatedDate] [datetime2](7) NOT NULL,
					[PerformanceRatingCreatedUser] [int] NOT NULL,
					[PerformanceRatingModifiedDate] [datetime2](7) NOT NULL,
					[PerformanceRatingModifiedUser] [int] NOT NULL,
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
             INSERT INTO #DimPerformanceRatingUpsert
             EXEC dbo.spHR_StagingTransform_DimPerformanceRating
                  @EDWRunDateTime;

		   -- Get how many rows were extracted FROM source 

             SELECT @SourceCount = COUNT(1)
             FROM #DimPerformanceRatingUpsert;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows FROM Source.';
             PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
             CREATE NONCLUSTERED INDEX XAK1DimPerformanceRatingUpsert ON #DimPerformanceRatingUpsert
             ([PerformanceRatingCode] ASC
             );

			  -- --------------------------------------------------------------------------------
		   -- [Re]Insert Seed Rows into EDW DimPayRateChangeReason	   
		   -- --------------------------------------------------------------------------------
             
			 IF NOT EXISTS (SELECT top 1 1 FROM [BING_EDW].[dbo].[DimPerformanceRating])
				   BEGIN
				   SET IDENTITY_INSERT [BING_EDW].[dbo].[DimPerformanceRating] ON;
					   INSERT INTO [BING_EDW].[dbo].[DimPerformanceRating] 
							(
							 [PerformanceRatingKey]
							,[PerformanceRatingCode]
							,[PerformanceRatingName]
							,[PerformanceRatingFlexAttribute1]
							,[PerformanceRatingFlexAttribute2]
							,[PerformanceRatingFlexAttribute3]
							,[PerformanceRatingFlexAttribute4]
							,[PerformanceRatingFlexAttribute5]
							,[PerformanceRatingCreatedDate]
							,[PerformanceRatingCreatedUser]
							,[PerformanceRatingModifiedDate]
							,[PerformanceRatingModifiedUser]
							,[EDWCreatedDate]
							,[EDWModifiedDate]
							)
					   SELECT
							 -1, 
							'-1',
							'Unknown Performance Rating',
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
							'-2',
							'Not Applicable Performance Rating',
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
							@EDWRunDateTime;
					SET @SeedRowCount=@@ROWCOUNT;
					SET IDENTITY_INSERT [BING_EDW].[dbo].[DimPerformanceRating] OFF;					
				   END;
				   

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
             MERGE [BING_EDW].[dbo].[DimPerformanceRating] T
             USING #DimPerformanceRatingUpsert S
             ON(S.PerformanceRatingCode = T.PerformanceRatingCode)
                 WHEN MATCHED AND (		  S.PerformanceRatingName			 <> T.PerformanceRatingName
                                       OR S.PerformanceRatingFlexAttribute1  <> T.PerformanceRatingFlexAttribute1
									   OR S.PerformanceRatingFlexAttribute2  <> T.PerformanceRatingFlexAttribute2
									   OR S.PerformanceRatingFlexAttribute3  <> T.PerformanceRatingFlexAttribute3
									   OR S.PerformanceRatingFlexAttribute4  <> T.PerformanceRatingFlexAttribute4
									   OR S.PerformanceRatingFlexAttribute5  <> T.PerformanceRatingFlexAttribute5
									   OR S.PerformanceRatingCreatedDate     <> T.PerformanceRatingCreatedDate
									   OR S.PerformanceRatingCreatedUser	 <> T.PerformanceRatingCreatedUser
									   OR S.PerformanceRatingModifiedDate    <> T.PerformanceRatingModifiedDate
									   OR S.PerformanceRatingModifiedUser    <> T.PerformanceRatingModifiedUser

								  )
                 THEN UPDATE SET
                                 T.PerformanceRatingName		   = S.PerformanceRatingName,
                                 T.PerformanceRatingFlexAttribute1 = S.PerformanceRatingFlexAttribute1,
                                 T.PerformanceRatingFlexAttribute2 = S.PerformanceRatingFlexAttribute2,
                                 T.PerformanceRatingFlexAttribute3 = S.PerformanceRatingFlexAttribute3,
								 T.PerformanceRatingFlexAttribute4 = S.PerformanceRatingFlexAttribute4,
								 T.PerformanceRatingFlexAttribute5 = S.PerformanceRatingFlexAttribute5,
								 T.PerformanceRatingCreatedDate	   = S.PerformanceRatingCreatedDate,
								 T.PerformanceRatingCreatedUser    = S.PerformanceRatingCreatedUser,
								 T.PerformanceRatingModifiedDate   = S.PerformanceRatingModifiedDate,
								 T.PerformanceRatingModifiedUser   = S.PerformanceRatingModifiedUser,
                                 T.EDWModifiedDate				   = S.EDWModifiedDate
                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT(   
						 [PerformanceRatingCode]
						,[PerformanceRatingName]
						,[PerformanceRatingFlexAttribute1]
						,[PerformanceRatingFlexAttribute2]
						,[PerformanceRatingFlexAttribute3]
						,[PerformanceRatingFlexAttribute4]
						,[PerformanceRatingFlexAttribute5]
						,[PerformanceRatingCreatedDate]
						,[PerformanceRatingCreatedUser]
						,[PerformanceRatingModifiedDate]
						,[PerformanceRatingModifiedUser]
						,[EDWCreatedDate]
						,[EDWModifiedDate] )
				   VALUES(   
				     [PerformanceRatingCode]
					,[PerformanceRatingName]
					,[PerformanceRatingFlexAttribute1]
					,[PerformanceRatingFlexAttribute2]
					,[PerformanceRatingFlexAttribute3]
					,[PerformanceRatingFlexAttribute4]
					,[PerformanceRatingFlexAttribute5]
					,[PerformanceRatingCreatedDate]
					,[PerformanceRatingCreatedUser]
					,[PerformanceRatingModifiedDate]
					,[PerformanceRatingModifiedUser]
					,[EDWCreatedDate]
					,[EDWModifiedDate] )
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
             DROP TABLE #DimPerformanceRatingUpsert;

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