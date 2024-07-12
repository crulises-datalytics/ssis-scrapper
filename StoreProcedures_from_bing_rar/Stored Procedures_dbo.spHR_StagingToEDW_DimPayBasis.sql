/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spHR_StagingToEDW_DimPayBasis'
)
    DROP PROCEDURE dbo.spHR_StagingToEDW_DimPayBasis;
GO
*/
Create PROCEDURE [dbo].[spHR_StagingToEDW_DimPayBasis]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingToEDW_DimPayBasis
    --
    -- Purpose:            Performs the Insert / Update ETL process for
    --                         the DimPayBasis table from Staging to BING_EDW.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                                 sub-procedure spHR_StagingTransform_DimPayBasis, 
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
    -- Usage:              EXEC dbo.spHR_StagingToEDW_DimPayBasis @DebugMode = 1	
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
    -- 02/27/18    Adevabhakthuni            BNG-269  DimPayBasis
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimPayBasis';
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
             CREATE TABLE #DimPayBasisUpsert
             (
			 [PayBasisID]                  [INT] NOT NULL,
			 [PayBasisName]                [VARCHAR](150) NOT NULL,
			 [PayBasisAnnualizationFactor] [INT] NOT NULL,
			 [PayBasisFlexAttribute1]      [VARCHAR](150) NULL,
			 [PayBasisFlexAttribute2]      [VARCHAR](150) NULL,
			 [PayBasisFlexAttribute3]      [VARCHAR](150) NULL,
			 [PayBasisFlexAttribute4]      [VARCHAR](150) NULL,
			 [PayBasisFlexAttribute5]      [VARCHAR](150) NULL,
			 [PayBasisCreatedDate]         [DATETIME2] NOT NULL,
			 [PayBasisCreatedUser]         [INT] NOT NULL,
			 [PayBasisModifiedDate]        [DATETIME2] NOT NULL,
			 [PayBasisModifiedUser]        [INT] NOT NULL,
			 [EDWCreatedDate]              [DATETIME2] NOT NULL,
			 [EDWModifiedDate]             [DATETIME2] NOT NULL,
             );          

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table FROM Source, and create any helper indexes
		   --
		   -- ================================================================================
             INSERT INTO #DimPayBasisUpsert
             EXEC dbo.spHR_StagingTransform_DimPayBasis
                  @EDWRunDateTime;

		   -- Get how many rows were extracted FROM source 

             SELECT @SourceCount = COUNT(1)
             FROM #DimPayBasisUpsert;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows FROM Source.';
             PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
             CREATE NONCLUSTERED INDEX XAK1DimPayBasisUpsert ON #DimPayBasisUpsert
             ([PayBasisID] ASC
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
             MERGE [BING_EDW].[dbo].[DimPayBasis] T
             USING #DimPayBasisUpsert S
             ON(S.PayBasisID = T.PayBasisID)
                 WHEN MATCHED AND (    T.PayBasisName <> S.PayBasisName
									OR T.PayBasisAnnualizationFactor <> S.PayBasisAnnualizationFactor
									OR T.PayBasisFlexAttribute1 <> S.PayBasisFlexAttribute1
									OR T.PayBasisFlexAttribute2 <> S.PayBasisFlexAttribute2
									OR T.PayBasisFlexAttribute3 <> S.PayBasisFlexAttribute3
									OR T.PayBasisFlexAttribute4 <> S.PayBasisFlexAttribute4
									OR T.PayBasisFlexAttribute5 <> S.PayBasisFlexAttribute5
									OR T.PayBasisCreatedDate <> S.PayBasisCreatedDate
									OR T.PayBasisCreatedUser <> S.PayBasisCreatedUser
									OR T.PayBasisModifiedDate <> S.PayBasisModifiedDate
									OR T.PayBasisModifiedUser <> S.PayBasisModifiedUser
)
                 THEN UPDATE SET
								T.PayBasisName = S.PayBasisName ,
								T.PayBasisAnnualizationFactor = S.PayBasisAnnualizationFactor ,
								T.PayBasisFlexAttribute1 = S.PayBasisFlexAttribute1 ,
								T.PayBasisFlexAttribute2 = S.PayBasisFlexAttribute2 ,
								T.PayBasisFlexAttribute3 = S.PayBasisFlexAttribute3 ,
								T.PayBasisFlexAttribute4 = S.PayBasisFlexAttribute4 ,
								T.PayBasisFlexAttribute5 = S.PayBasisFlexAttribute5 ,
								T.PayBasisCreatedDate = S.PayBasisCreatedDate ,
								T.PayBasisCreatedUser = S.PayBasisCreatedUser ,
								T.PayBasisModifiedDate = S.PayBasisModifiedDate ,
								T.PayBasisModifiedUser = S.PayBasisModifiedUser ,
                                T.EDWCreatedDate = S.EDWCreatedDate,
                                T.EDWModifiedDate = S.EDWModifiedDate
                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT(  PayBasisID,
							PayBasisName,
							PayBasisAnnualizationFactor,
							PayBasisFlexAttribute1,
							PayBasisFlexAttribute2,
							PayBasisFlexAttribute3,
							PayBasisFlexAttribute4,
							PayBasisFlexAttribute5,
							PayBasisCreatedDate,
							PayBasisCreatedUser,
							PayBasisModifiedDate,
							PayBasisModifiedUser,
  							EDWCreatedDate,        
							EDWModifiedDate )
				   VALUES(		S.PayBasisID ,
								S.PayBasisName ,
								S.PayBasisAnnualizationFactor ,
								S.PayBasisFlexAttribute1 ,
								S.PayBasisFlexAttribute2 ,
								S.PayBasisFlexAttribute3 ,
								S.PayBasisFlexAttribute4 ,
								S.PayBasisFlexAttribute5 ,
								S.PayBasisCreatedDate ,
								S.PayBasisCreatedUser ,
								S.PayBasisModifiedDate ,
								S.PayBasisModifiedUser ,
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
             DROP TABLE #DimPayBasisUpsert;

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