

/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spHR_StagingToEDW_DimPayRateChangeReason'
)
    DROP PROCEDURE dbo.spHR_StagingToEDW_DimPayRateChangeReason;
GO
*/
CREATE PROCEDURE [dbo].[spHR_StagingToEDW_DimPayRateChangeReason]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingToEDW_DimPayRateChangeReason
    --
    -- Purpose:            Performs the Insert / Update ETL process for
    --                         the DimPayRateChangeReason table from Staging to BING_EDW.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                                 sub-procedure spHR_StagingTransform_DimPayRateChangeReason, 
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
    -- Usage:              EXEC dbo.spHR_StagingToEDW_DimPayRateChangeReason @DebugMode = 1	
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
    -- 2/27/18    valimineti            BNG-270  DimPayRateChangeReason staging to EDW load.
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimPayRateChangeReason';
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
             CREATE TABLE #DimPayRateChangeReasonUpsert
             (
				 [PayRateChangeReasonCode]           [VARCHAR](50) NOT NULL,
				 [PayRateChangeReasonName]           [VARCHAR](150) NOT NULL,
				 [PayRateChangeReasonFlexAttribute1] [VARCHAR](150) NULL,
				 [PayRateChangeReasonFlexAttribute2] [VARCHAR](150) NULL,
				 [PayRateChangeReasonFlexAttribute3] [VARCHAR](150) NULL,
				 [PayRateChangeReasonFlexAttribute4] [VARCHAR](150) NULL,
				 [PayRateChangeReasonFlexAttribute5] [VARCHAR](150) NULL,
				 [PayRateChangeReasonCreatedDate]    [DATETIME2] NOT NULL,
				 [PayRateChangeReasonCreatedUser]    [INT] NOT NULL,
				 [PayRateChangeReasonModifiedDate]   [DATETIME2] NOT NULL,
				 [PayRateChangeReasonModifiedUser]   [INT] NOT NULL,
				 [EDWCreatedDate]                    [DATETIME2] NOT NULL,
				 [EDWModifiedDate]                   [DATETIME2] NOT NULL
             );          

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table FROM Source, and create any helper indexes
		   --
		   -- ================================================================================
             INSERT INTO #DimPayRateChangeReasonUpsert
             EXEC dbo.spHR_StagingTransform_DimPayRateChangeReason
                  @EDWRunDateTime;

		   -- Get how many rows were extracted FROM source 

             SELECT @SourceCount = COUNT(1)
             FROM #DimPayRateChangeReasonUpsert;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows FROM Source.';
             PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
             CREATE NONCLUSTERED INDEX XAK1DimPayRateChangeReasonUpsert ON #DimPayRateChangeReasonUpsert
             ([PayRateChangeReasonCode] ASC
             );
 -- --------------------------------------------------------------------------------
		   -- [Re]Insert Seed Rows into EDW DimPayRateChangeReason	   
		   -- --------------------------------------------------------------------------------
             
			 IF NOT EXISTS (SELECT top 1 1 FROM [BING_EDW].[dbo].[DimPayRateChangeReason])
				   BEGIN
				   SET IDENTITY_INSERT [BING_EDW].[dbo].[DimPayRateChangeReason] ON;
					   INSERT INTO [BING_EDW].[dbo].[DimPayRateChangeReason] 
							(
							[PayRateChangeReasonKey],
							[PayRateChangeReasonCode],
							[PayRateChangeReasonName],
							[PayRateChangeReasonFlexAttribute1],
							[PayRateChangeReasonFlexAttribute2],
							[PayRateChangeReasonFlexAttribute3],
							[PayRateChangeReasonFlexAttribute4],
							[PayRateChangeReasonFlexAttribute5],
							[PayRateChangeReasonCreatedDate],
							[PayRateChangeReasonCreatedUser],
							[PayRateChangeReasonModifiedDate],
							[PayRateChangeReasonModifiedUser],
							[EDWCreatedDate],
							[EDWModifiedDate]
							)
					   SELECT 
							-1,
							'-1',
							'Unknown Pay Rate Change Reason',
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
							'Not Applicable Pay Rate Change Reason',
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
					SET IDENTITY_INSERT [BING_EDW].[dbo].[DimPayRateChangeReason] OFF;
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
             MERGE [BING_EDW].[dbo].[DimPayRateChangeReason] T
             USING #DimPayRateChangeReasonUpsert S
             ON(S.PayRateChangeReasonCode = T.PayRateChangeReasonCode)
                 WHEN MATCHED AND 
				 (     
					   S.[PayRateChangeReasonName]  <>  T.[PayRateChangeReasonName]
					OR S.[PayRateChangeReasonFlexAttribute1]  <>  T.[PayRateChangeReasonFlexAttribute1]
					OR S.[PayRateChangeReasonFlexAttribute2]  <>  T.[PayRateChangeReasonFlexAttribute2]
					OR S.[PayRateChangeReasonFlexAttribute3]  <>  T.[PayRateChangeReasonFlexAttribute3]
					OR S.[PayRateChangeReasonFlexAttribute4]  <>  T.[PayRateChangeReasonFlexAttribute4]
					OR S.[PayRateChangeReasonFlexAttribute5]  <>  T.[PayRateChangeReasonFlexAttribute5]
					OR S.[PayRateChangeReasonCreatedDate]  <>  T.[PayRateChangeReasonCreatedDate]
					OR S.[PayRateChangeReasonCreatedUser]  <>  T.[PayRateChangeReasonCreatedUser]
					OR S.[PayRateChangeReasonModifiedDate]  <>  T.[PayRateChangeReasonModifiedDate]
					OR S.[PayRateChangeReasonModifiedUser]  <>  T.[PayRateChangeReasonModifiedUser]

				)
                 THEN UPDATE SET
                                T.[PayRateChangeReasonName] = S.[PayRateChangeReasonName],
								T.[PayRateChangeReasonFlexAttribute1] = S.[PayRateChangeReasonFlexAttribute1],
								T.[PayRateChangeReasonFlexAttribute2] = S.[PayRateChangeReasonFlexAttribute2],
								T.[PayRateChangeReasonFlexAttribute3] = S.[PayRateChangeReasonFlexAttribute3],
								T.[PayRateChangeReasonFlexAttribute4] = S.[PayRateChangeReasonFlexAttribute4],
								T.[PayRateChangeReasonFlexAttribute5] = S.[PayRateChangeReasonFlexAttribute5],
								T.[PayRateChangeReasonCreatedDate] = S.[PayRateChangeReasonCreatedDate],
								T.[PayRateChangeReasonCreatedUser] = S.[PayRateChangeReasonCreatedUser],
								T.[PayRateChangeReasonModifiedDate] = S.[PayRateChangeReasonModifiedDate],
								T.[PayRateChangeReasonModifiedUser] = S.[PayRateChangeReasonModifiedUser],
								T.EDWCreatedDate = S.EDWCreatedDate,
								T.EDWModifiedDate = S.EDWModifiedDate

                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT
				   (  
					 [PayRateChangeReasonCode],
					 [PayRateChangeReasonName],
					 [PayRateChangeReasonFlexAttribute1],
					 [PayRateChangeReasonFlexAttribute2],
					 [PayRateChangeReasonFlexAttribute3],
					 [PayRateChangeReasonFlexAttribute4],
					 [PayRateChangeReasonFlexAttribute5],
					 [PayRateChangeReasonCreatedDate],
					 [PayRateChangeReasonCreatedUser],
					 [PayRateChangeReasonModifiedDate],
					 [PayRateChangeReasonModifiedUser],
					 [EDWCreatedDate],
					 [EDWModifiedDate]
					)
				   VALUES
				   (
					 [PayRateChangeReasonCode],
					 [PayRateChangeReasonName],
					 [PayRateChangeReasonFlexAttribute1],
					 [PayRateChangeReasonFlexAttribute2],
					 [PayRateChangeReasonFlexAttribute3],
					 [PayRateChangeReasonFlexAttribute4],
					 [PayRateChangeReasonFlexAttribute5],
					 [PayRateChangeReasonCreatedDate],
					 [PayRateChangeReasonCreatedUser],
					 [PayRateChangeReasonModifiedDate],
					 [PayRateChangeReasonModifiedUser],
					 [EDWCreatedDate],
					 [EDWModifiedDate]
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
             DROP TABLE #DimPayRateChangeReasonUpsert;

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