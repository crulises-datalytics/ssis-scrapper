
CREATE PROCEDURE [dbo].[spHR_StagingToEDW_DimAssessmentType]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingToEDW_DimAssessmentType
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
    -- Usage:              EXEC dbo.spHR_StagingToEDW_DimAssessmentType @DebugMode = 1	
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
    -- 3/12/18      Banandesi              BNG-271 - Initial version
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimAssessmentType';
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
		   -- For the DimAssessmentType load we deviate slightly from theb usual pattern of using
		   --     a MERGE statement.  This is because the Source deals with all the history
		   --     and SCD for us, and to do a MERGE again here is quite costly for no real
		   --     benefit.
		   -- Therefore, we just do a kill & fill as it is easier logically (and on the 
		   --     optimizer
             -- ================================================================================

		   -- --------------------------------------------------------------------------------
		   -- Get @SourceCount & @DeleteCount (which is the EDW DimAssessmentType rowcount pre-truncate)		   
		   -- --------------------------------------------------------------------------------

             SELECT @SourceCount = COUNT(1)
             FROM dbo.vAssessmentTypes;
             SELECT @DeleteCount = COUNT(1)
             FROM [BING_EDW].[dbo].[DimAssessmentType];

		   -- --------------------------------------------------------------------------------
		   -- Clear-down EDW DimAssessmentType		   
		   -- --------------------------------------------------------------------------------

             TRUNCATE TABLE [BING_EDW].[dbo].[DimAssessmentType];
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Deleted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Target.';
             PRINT @DebugMsg;

		   -- --------------------------------------------------------------------------------
		   -- [Re]Insert Seed Rows into EDW DimAssessmentType	   
		   -- --------------------------------------------------------------------------------
             SET IDENTITY_INSERT [BING_EDW].[dbo].[DimAssessmentType] ON;
             INSERT INTO [BING_EDW].[dbo].[DimAssessmentType]
             (
		     [AssessmentTypeKey] ,
	          [AssessmentJobName] ,
	          [AssessmentRatingCode] ,
	          [AssessmentRatingName] ,
	          [EDWCreatedDate] 
             )
             SELECT
				 -1
				,'Unknown Assessment Job'
				,-1
				,'Unknown Assessment Rating'
				,@EDWRunDateTime

			 UNION
             SELECT
				-2
				,'Not Applicable Assessment Job'
				,-2
				,'Not Applicable Assessment Rating'
				,@EDWRunDateTime;

             SET IDENTITY_INSERT [BING_EDW].[dbo].[DimAssessmentType] OFF;
		   -- --------------------------------------------------------------------------------
		   -- Insert Rows into EDW DimAssessmentType	   
		   -- --------------------------------------------------------------------------------
             INSERT INTO [BING_EDW].[dbo].[DimAssessmentType]
			 (
			 
	           [AssessmentJobName],
	           [AssessmentRatingCode],
	           [AssessmentRatingName],
	           [EDWCreatedDate] 
			 )
             EXEC dbo.spHR_StagingTransform_DimAssessmentType
                  @EDWRunDateTime;

		   -- Get how many rows were extracted from source 

             SELECT @InsertCount = COUNT(1)
             FROM [BING_EDW].[dbo].[DimAssessmentType];
		   
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