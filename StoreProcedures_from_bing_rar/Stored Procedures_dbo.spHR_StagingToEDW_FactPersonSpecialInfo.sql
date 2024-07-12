﻿
/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spHR_StagingToEDW_FactPersonSpecialInfo'
)
    DROP PROCEDURE dbo.spHR_StagingToEDW_FactPersonSpecialInfo;
GO
*/

CREATE PROCEDURE [dbo].[spHR_StagingToEDW_FactPersonSpecialInfo]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingToEDW_FactPersonSpecialInfo
    --
    -- Purpose:            Performs the Insert / Update ETL process for
    --                         the FactPersonSpecialInfo table from Staging to BING_EDW.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                                 sub-procedure spHR_StagingTransform_FactPersonSpecialInfo, 
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
    -- Usage:              EXEC dbo.spHR_StagingToEDW_FactPersonSpecialInfo @DebugMode = 1	
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
    -- 03/16/18    valimineti            BNG-276.  FactPersonSpecialInfo
    --			 
    -- ================================================================================
         BEGIN
             SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
             DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
             DECLARE @DebugMsg NVARCHAR(500);
             DECLARE @SourceName VARCHAR(100)= 'FactPersonSpecialInfo';
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
                 CREATE TABLE #FactPersonSpecialInfoUpsert
				    (PersonSpecialInfoEffectiveDateKey  INT NOT NULL,
					PersonSpecialInfoEndDateKey        INT NOT NULL,
					PersonSpecialInfoCurrentRecordFlag VARCHAR(1) NOT NULL,
					PersonKey                          INT NOT NULL,
					SpecialInfoKey                     INT NOT NULL,
					PersonSpecialInfoID                INT NOT NULL,
					PersonSpecialInfoCreatedUser       INT NOT NULL,
					PersonSpecialInfoCreatedDate       DATETIME2 NOT NULL,
					PersonSpecialInfoModifiedUser      INT NOT NULL,
					PersonSpecialInfoModifiedDate      DATETIME2 NOT NULL,
					EDWCreatedDate                     DATETIME2 NOT NULL,
					EDWModifiedDate                    DATETIME2 NOT NULL
				    );          

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table FROM Source, and create any helper indexes
		   --
		   -- ================================================================================
                 INSERT INTO #FactPersonSpecialInfoUpsert
                 EXEC dbo.spHR_StagingTransform_FactPersonSpecialInfo
                      @EDWRunDateTime;

		   -- Get how many rows were extracted FROM source 

                 SELECT @SourceCount = COUNT(1)
                 FROM #FactPersonSpecialInfoUpsert;
		   
		   -- Debug output progress
                 IF @DebugMode = 1
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows FROM Source.';
                 PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
                 CREATE UNIQUE NONCLUSTERED INDEX XAK1FactPersonSpecialInfoUpsert ON #FactPersonSpecialInfoUpsert
			 ([PersonSpecialInfoID] ASC);

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
                 MERGE [BING_EDW].[dbo].[FactPersonSpecialInfo] T
                 USING #FactPersonSpecialInfoUpsert S
                 ON(S.PersonSpecialInfoID = T.PersonSpecialInfoID)
                     WHEN MATCHED AND(T.PersonSpecialInfoEffectiveDateKey <> S.PersonSpecialInfoEffectiveDateKey
                                      OR T.PersonSpecialInfoEndDateKey <> S.PersonSpecialInfoEndDateKey
                                      OR T.PersonSpecialInfoCurrentRecordFlag <> S.PersonSpecialInfoCurrentRecordFlag
                                      OR T.PersonKey <> S.PersonKey
                                      OR T.SpecialInfoKey <> S.SpecialInfoKey
                                      OR T.PersonSpecialInfoCreatedUser <> S.PersonSpecialInfoCreatedUser
                                      OR T.PersonSpecialInfoCreatedDate <> S.PersonSpecialInfoCreatedDate
                                      OR T.PersonSpecialInfoModifiedUser <> S.PersonSpecialInfoModifiedUser
                                      OR T.PersonSpecialInfoModifiedDate <> S.PersonSpecialInfoModifiedDate)
                     THEN UPDATE SET
                                     T.PersonSpecialInfoEffectiveDateKey = S.PersonSpecialInfoEffectiveDateKey,
                                     T.PersonSpecialInfoEndDateKey = S.PersonSpecialInfoEndDateKey,
                                     T.PersonSpecialInfoCurrentRecordFlag = S.PersonSpecialInfoCurrentRecordFlag,
                                     T.PersonKey = S.PersonKey,
                                     T.SpecialInfoKey = S.SpecialInfoKey,
                                     T.PersonSpecialInfoCreatedUser = S.PersonSpecialInfoCreatedUser,
                                     T.PersonSpecialInfoCreatedDate = S.PersonSpecialInfoCreatedDate,
                                     T.PersonSpecialInfoModifiedUser = S.PersonSpecialInfoModifiedUser,
                                     T.PersonSpecialInfoModifiedDate = S.PersonSpecialInfoModifiedDate,
                                     T.EDWModifiedDate = S.EDWModifiedDate
                     WHEN NOT MATCHED BY TARGET
                     THEN
                       INSERT(PersonSpecialInfoEffectiveDateKey,
                              PersonSpecialInfoEndDateKey,
                              PersonSpecialInfoCurrentRecordFlag,
                              PersonKey,
                              SpecialInfoKey,
                              PersonSpecialInfoID,
                              PersonSpecialInfoCreatedUser,
                              PersonSpecialInfoCreatedDate,
                              PersonSpecialInfoModifiedUser,
                              PersonSpecialInfoModifiedDate,
                              EDWCreatedDate,
                              EDWModifiedDate)
                       VALUES
					   (S.PersonSpecialInfoEffectiveDateKey,
					    S.PersonSpecialInfoEndDateKey,
					    S.PersonSpecialInfoCurrentRecordFlag,
					    S.PersonKey,
					    S.SpecialInfoKey,
					    S.PersonSpecialInfoID,
					    S.PersonSpecialInfoCreatedUser,
					    S.PersonSpecialInfoCreatedDate,
					    S.PersonSpecialInfoModifiedUser,
					    S.PersonSpecialInfoModifiedDate,
					    S.EDWCreatedDate,
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
                 DROP TABLE #FactPersonSpecialInfoUpsert;

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