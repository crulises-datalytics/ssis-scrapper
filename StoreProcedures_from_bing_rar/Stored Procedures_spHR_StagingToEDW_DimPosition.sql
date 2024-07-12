CREATE PROCEDURE dbo.spHR_StagingToEDW_DimPosition
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingToEDW_DimPosition
    --
    -- Purpose:            Performs the Insert / Update / Delete ETL process for
    --                         the DimCreditMemoType table from Staging to BING_EDW.
    --
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
    --                         DeleteCount - Number or rows deleted in target table
    --
    -- Usage:              EXEC dbo.spHR_StagingToEDW_DimPosition @DebugMode = 1	
    -- 
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By         Comments
    -- ----         -----------         --------
    --
    -- 12/19/17     sburke              BNG-264 - Initial version
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimPosition';
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
           -- For the DimPosition load we deviate slightly from theb usual pattern of using
           --     a MERGE statement.  This is because the Source deals with all the history
           --     and SCD for us, and to do a MERGE again here is quite costly for no real
           --     benefit (we would have to compare on the entire record).
           -- Therefore, we just do a kill & fill as it is easier logically (and on the 
           --     optimizer)
           -- ================================================================================

		   -- --------------------------------------------------------------------------------
		   -- Get @SourceCount & @DeleteCount (which is the EDW DimPosition rowcount pre-truncate)		   
		   -- --------------------------------------------------------------------------------

             SELECT @SourceCount = COUNT(1)
             FROM dbo.vPositions;
             SELECT @DeleteCount = COUNT(1)
             FROM [BING_EDW].[dbo].[DimPosition];

		   -- --------------------------------------------------------------------------------
		   -- Clear-down EDW DimPosition		   
		   -- --------------------------------------------------------------------------------

             TRUNCATE TABLE [BING_EDW].[dbo].[DimPosition];
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Deleted '+CONVERT(NVARCHAR(20), @DeleteCount)+' rows from Target.';
             PRINT @DebugMsg;

		   -- --------------------------------------------------------------------------------
		   -- [Re]Insert Seed Rows into EDW DimPosition	   
		   -- --------------------------------------------------------------------------------
             SET IDENTITY_INSERT [BING_EDW].[dbo].[DimPosition] ON;
             INSERT INTO [BING_EDW].[dbo].[DimPosition]
             (PositionKey,
              PositionEffectiveDate,
              PositionEndDate,
              PositionCurrentRecordFlag,
              PositionID,
              PositionCode,
              PositionName,
              RollupPositionName,
              PositionStatusCode,
              PositionStatusName,
              PositionFunctionalAreaName,
              PositionSubfunctionalAreaName,
              PositionCorporateOverheadValue,
              PositionBonusEligibleFlag,
              PositionBonusPlanName,
              PositionBonusTargetPercent,
              PositionLTIPEligibleFlag,
              PositionPayBasisID,
              PositionEntryPayGradeID,
              JobID,
              JobCode,
              JobName,
              JobGroupCode,
              JobGroupName,
              JobCategoryID,
              JobCategoryName,
              JobCCDGroupName,
              JobEEOCategoryID,
              JobEEOCategoryName,
              JobFLSACode,
              JobFLSAName,
              JobWorkersCompTypeCode,
              JobWorkersCompTypeName,
              JobAATypeCode,
              JobAATypeName,
              JobAACategoryName,
              JobLevelCode,
              JobLevelName,
              JobPeopleGroupName,
              JobTypeName,
              JobFamilyName,
              JobManagerFlag,
              JobPurchaseApprovalLevelName,
              JobPcardApproverFlag,
              PositionFlexValue1,
              PositionFlexValue2,
              PositionFlexValue3,
              PositionFlexValue4,
              PositionFlexValue5,
              JobFlexValue1,
              JobFlexValue2,
              JobFlexValue3,
              JobFlexValue4,
              JobFlexValue5,
              PositionCreatedDate,
              PositionCreatedUser,
              PositionModifiedDate,
              PositionModifiedUser,
              JobCreatedDate,
              JobCreatedUser,
              JobModifiedDate,
              JobModifiedUser,
              EDWCreatedDate,
              EDWModifiedDate
             )
             VALUES
             (-1,
              '19000101',
              '99991231',
              'Y',
              -1,
              '-1',
              'Unknown Position',
              'Unknown Rollup Position',
              'Unknown Position Status',
              'Unknown Position Status',
              'Unknown Functional Area',
              'Unknown Subfunctional Area',
              'Unknown Corporate Overhead',
              'Unknown Bonus Eligible',
              'Unknown Bonus Plan',
              '0',
              'Unknown LTIP Eligible',
              -1,
              -1,
              -1,
              '-1',
              'Unknown Job',
              '-1',
              'Unknown Job Group',
              -1,
              'Unknown Job Category',
              'Unknown Job CCD Group',
              -1,
              'Unknown EEO Category',
              '-1',
              'Unknown FLSA',
              '-1',
              'Unknown Workers Comp Type',
              '-1',
              'Unknown AA Type',
              'Unknown AA Category',
              '-1',
              'Unknown Job Level',
              'Unknown People Group',
              'Unknown Job Type',
              'Unknown Job Family',
              'Unknown Manager',
              'Unknown Purchase Approval Level',
              'Unknown Pcard Approver',
              NULL,
              NULL,
              NULL,
              NULL,
              NULL,
              NULL,
              NULL,
              NULL,
              NULL,
              NULL,
              '19000101',
              -1,
              '19000101',
              -1,
              '19000101',
              -1,
              '19000101',
              -1,
              GETDATE(),
              GETDATE()
             );
             SET IDENTITY_INSERT [BING_EDW].[dbo].[DimPosition] OFF;
		   -- --------------------------------------------------------------------------------
		   -- Insert Rows into EDW DimPosition	   
		   -- --------------------------------------------------------------------------------
             INSERT INTO [BING_EDW].[dbo].[DimPosition]
             EXEC dbo.spHR_StagingTransform_DimPosition
                  @EDWRunDateTime;

		   -- Get how many rows were extracted from source 

             SELECT @InsertCount = COUNT(1)
             FROM [BING_EDW].[dbo].[DimPosition];
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Source.';
                     PRINT @DebugMsg;
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(NVARCHAR(20), @InsertCount)+' into Target.';
                     PRINT @DebugMsg;
             END;

		  
		  -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Committing transaction.';
             PRINT @DebugMsg;
		   
		   --
		   -- Commit the successful transaction 
		   --
             COMMIT TRANSACTION;
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
GO