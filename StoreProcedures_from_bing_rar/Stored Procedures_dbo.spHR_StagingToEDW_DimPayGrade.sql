/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spHR_StagingToEDW_DimPayGrade'
)
    DROP PROCEDURE dbo.spHR_StagingToEDW_DimPayGrade;
GO
*/
CREATE PROCEDURE [dbo].[spHR_StagingToEDW_DimPayGrade]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingToEDW_DimPayGrade
    --
    -- Purpose:            Performs the Insert / Update ETL process for
    --                         the DimPayGrade table from Staging to BING_EDW.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                                 sub-procedure spHR_StagingTransform_DimPayGrade, 
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
    -- Usage:              EXEC dbo.spHR_StagingToEDW_DimPayGrade @DebugMode = 1	
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
    -- 12/20/17    hhebbalu            BNG-268.  DimPayGrade
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimPayGrade';
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
             CREATE TABLE #DimPayGradeUpsert
             (
				PayGradeKey            INT IDENTITY NOT NULL,
				PayGradeID             INT NOT NULL,
				PayGradeName           VARCHAR(250) NOT NULL,
				PayGradeSort           INT NOT NULL,
				PayGradeJobTypeCode    VARCHAR(5) NOT NULL,
				PayGradeJobTypeName    VARCHAR(250) NOT NULL,
				PayGradeJobCode        VARCHAR(5) NOT NULL,
				PayGradeJobName        VARCHAR(250) NOT NULL,
				PayGradeGeoCode        VARCHAR(5) NOT NULL,
				PayGradeGeoName        VARCHAR(250) NOT NULL,
				PayGradeRuleID         INT NOT NULL,
				PayGradeRuleMinValue   VARCHAR(25) NOT NULL,
				PayGradeRuleMidValue   VARCHAR(25) NOT NULL,
				PayGradeRuleMaxValue   VARCHAR(25) NOT NULL,
				PayGradeFlexAttribute1 VARCHAR(150) NULL,
				PayGradeFlexAttribute2 VARCHAR(150) NULL,
				PayGradeFlexAttribute3 VARCHAR(150) NULL,
				PayGradeFlexAttribute4 VARCHAR(150) NULL,
				PayGradeFlexAttribute5 VARCHAR(150) NULL,
				PayGradeCreatedDate    DATETIME2 NOT NULL,
				PayGradeCreatedUser    INT NOT NULL,
				PayGradeModifiedDate   DATETIME2 NOT NULL,
				PayGradeModifiedUser   INT NOT NULL,
				EDWCreatedDate         DATETIME2 NOT NULL,
				EDWModifiedDate        DATETIME2 NOT NULL
             );          

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table FROM Source, and create any helper indexes
		   --
		   -- ================================================================================
             INSERT INTO #DimPayGradeUpsert
             EXEC dbo.spHR_StagingTransform_DimPayGrade
                  @EDWRunDateTime;

		   -- Get how many rows were extracted FROM source 

             SELECT @SourceCount = COUNT(1)
             FROM #DimPayGradeUpsert;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows FROM Source.';
             PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
             CREATE NONCLUSTERED INDEX XAK1DimPayGradeUpsert ON #DimPayGradeUpsert
             ([PayGradeID] ASC
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
             MERGE [BING_EDW].[dbo].[DimPayGrade] T
             USING #DimPayGradeUpsert S
             ON(S.PayGradeID = T.PayGradeID)
                 WHEN MATCHED AND (S.PayGradeName  <> T.PayGradeName 
                                       OR S.PayGradeSort  <> T.PayGradeSort
									   OR S.PayGradeJobTypeCode  <> T.PayGradeJobTypeCode
									   OR S.PayGradeJobTypeName  <> T.PayGradeJobTypeName
									   OR S.PayGradeJobCode  <> T.PayGradeJobCode
									   OR S.PayGradeJobName  <> T.PayGradeJobName
									   OR S.PayGradeGeoCode  <> T.PayGradeGeoCode
									   OR S.PayGradeGeoName  <> T.PayGradeGeoName
									   OR S.PayGradeRuleID  <> T.PayGradeRuleID
									   OR S.PayGradeRuleMinValue  <> T.PayGradeRuleMinValue
									   OR S.PayGradeRuleMidValue  <> T.PayGradeRuleMidValue
									   OR S.PayGradeRuleMaxValue  <> T.PayGradeRuleMaxValue
									   OR S.PayGradeFlexAttribute1  <> T.PayGradeFlexAttribute1
									   OR S.PayGradeFlexAttribute2  <> T.PayGradeFlexAttribute2
									   OR S.PayGradeFlexAttribute3  <> T.PayGradeFlexAttribute3
									   OR S.PayGradeFlexAttribute4  <> T.PayGradeFlexAttribute4
									   OR S.PayGradeFlexAttribute5  <> T.PayGradeFlexAttribute5
									   OR S.PayGradeCreatedDate  <> T.PayGradeCreatedDate
									   OR S.PayGradeCreatedUser  <> T.PayGradeCreatedUser
									   OR S.PayGradeModifiedDate  <> T.PayGradeModifiedDate
									   OR S.PayGradeModifiedUser  <> T.PayGradeModifiedUser )
                 THEN UPDATE SET
                                 T.PayGradeName = S.PayGradeName,
                                 T.PayGradeSort = S.PayGradeSort,
                                 T.PayGradeJobTypeCode = S.PayGradeJobTypeCode,
                                 T.PayGradeJobTypeName = S.PayGradeJobTypeName,
								 T.PayGradeJobCode = S.PayGradeJobCode,
								 T.PayGradeJobName = S.PayGradeJobName,
								 T.PayGradeGeoCode = S.PayGradeGeoCode,
								 T.PayGradeGeoName = S.PayGradeGeoName,
								 T.PayGradeRuleID = S.PayGradeRuleID,
								 T.PayGradeRuleMinValue = S.PayGradeRuleMinValue,
								 T.PayGradeRuleMidValue = S.PayGradeRuleMidValue,
								 T.PayGradeRuleMaxValue = S.PayGradeRuleMaxValue,
								 T.PayGradeFlexAttribute1 = S.PayGradeFlexAttribute1,
								 T.PayGradeFlexAttribute2 = S.PayGradeFlexAttribute2,
								 T.PayGradeFlexAttribute3 = S.PayGradeFlexAttribute3,
								 T.PayGradeFlexAttribute4 = S.PayGradeFlexAttribute4,
								 T.PayGradeFlexAttribute5 = S.PayGradeFlexAttribute5,
								 T.PayGradeCreatedDate = S.PayGradeCreatedDate,
								 T.PayGradeCreatedUser = S.PayGradeCreatedUser,
								 T.PayGradeModifiedDate = S.PayGradeModifiedDate,
								 T.PayGradeModifiedUser = S.PayGradeModifiedUser,
                                 T.EDWCreatedDate = S.EDWCreatedDate,
                                 T.EDWModifiedDate = S.EDWModifiedDate
                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT(PayGradeID,
						  PayGradeName,          
						  PayGradeSort,          
						  PayGradeJobTypeCode,   
						  PayGradeJobTypeName,   
						  PayGradeJobCode,       
						  PayGradeJobName,       
						  PayGradeGeoCode,       
						  PayGradeGeoName,       
						  PayGradeRuleID,        
						  PayGradeRuleMinValue,  
						  PayGradeRuleMidValue,  
						  PayGradeRuleMaxValue,  
						  PayGradeFlexAttribute1,
						  PayGradeFlexAttribute2,
						  PayGradeFlexAttribute3,
						  PayGradeFlexAttribute4,
						  PayGradeFlexAttribute5,
						  PayGradeCreatedDate,   
						  PayGradeCreatedUser,   
						  PayGradeModifiedDate,  
						  PayGradeModifiedUser, 
						  EDWCreatedDate,        
						  EDWModifiedDate )
				   VALUES(PayGradeID,
						  PayGradeName,          
						  PayGradeSort,          
						  PayGradeJobTypeCode,
						  PayGradeJobTypeName,   
						  PayGradeJobCode,  
						  PayGradeJobName,       
						  PayGradeGeoCode,       
						  PayGradeGeoName,       
						  PayGradeRuleID,       
						  PayGradeRuleMinValue,
						  PayGradeRuleMidValue, 
						  PayGradeRuleMaxValue, 
						  PayGradeFlexAttribute1,
						  PayGradeFlexAttribute2,
						  PayGradeFlexAttribute3,
						  PayGradeFlexAttribute4,
						  PayGradeFlexAttribute5,
						  PayGradeCreatedDate,
						  PayGradeCreatedUser,  
						  PayGradeModifiedDate,  
						  PayGradeModifiedUser, 
						  EDWCreatedDate, 
						  EDWModifiedDate )
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
             DROP TABLE #DimPayGradeUpsert;

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