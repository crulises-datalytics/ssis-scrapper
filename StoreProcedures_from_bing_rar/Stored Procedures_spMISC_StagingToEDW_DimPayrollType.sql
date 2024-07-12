/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spMISC_StagingToEDW_DimPayrollType'
)
    DROP PROCEDURE dbo.spMISC_StagingToEDW_DimPayrollType;
GO
*/
CREATE PROCEDURE [dbo].[spMISC_StagingToEDW_DimPayrollType]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spMISC_StagingToEDW_DimPayrollType
    --
    -- Purpose:            Performs the Insert / Update ETL process for
    --                         the DimPayrollType table from Staging to BING_EDW.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                                 sub-procedure spMISC_StagingTransform_DimPayrollType, 
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
    -- Usage:              EXEC dbo.spMISC_StagingToEDW_DimPayrollType @DebugMode = 1	
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
    -- 7/30/18    Banandesi            BNG-3487.  DimPayrollType
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimPayrollType';
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
             CREATE TABLE #DimPayrollTypeUpsert
             (
				[PayrollCode] [varchar](50) NULL,
	               [PayrollElement] [varchar](50) NULL,
	               [PayrollDescriptionShortName] [varchar](50) NULL,
	               [PayrollDescriptionLongName] [varchar](50) NULL,
	               [EDWCreatedDate] [datetime] NOT NULL
             );          

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table FROM Source, and create any helper indexes
		   --
		   -- ================================================================================
             INSERT INTO #DimPayrollTypeUpsert
             EXEC dbo.spMISC_StagingTransform_DimPayrollType
                  @EDWRunDateTime;

		   -- Get how many rows were extracted FROM source 

             SELECT @SourceCount = COUNT(1)
             FROM #DimPayrollTypeUpsert;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows FROM Source.';
             PRINT @DebugMsg;
		   -- --------------------------------------------------------------------------------
		   -- [Re]Insert Seed Rows into EDW DimPayrollType	   
		   -- --------------------------------------------------------------------------------
             
			 IF NOT EXISTS (SELECT top 1 1 FROM [BING_EDW].[dbo].[DimPayrollType])
				   BEGIN
				   SET IDENTITY_INSERT [BING_EDW].[dbo].[DimPayrollType] ON;
					   INSERT INTO [BING_EDW].[dbo].[DimPayrollType] 
							(
							[PayrollTypeKey],
							[PayrollCode],
						     [PayrollElement],          
						     [PayrollDescriptionShortName],          
						     [PayrollDescriptionLongName],
						     [EDWCreatedDate]
							)
					   SELECT 
							-1,
							'Unknown Payroll code',
							'Unknown Pay Element',
							'Unknown Journal Desc',
							'Unknown Desc',
							@EDWRunDateTime
							

					UNION
						SELECT
							-2,
							'Not Applicable Payroll code',
							'Not Applicable Pay Element',
							'Not Applicable Journal Desc',
							'Not Applicable Desc',
							@EDWRunDateTime
					SET IDENTITY_INSERT [BING_EDW].[dbo].[DimPayrollType] OFF;
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
             MERGE [BING_EDW].[dbo].[DimPayrollType] T
             USING #DimPayrollTypeUpsert S
             ON(S.PayrollCode = T.PayrollCode)
                 WHEN MATCHED AND (S.PayrollElement  <> T.PayrollElement 
                                       OR S.PayrollDescriptionShortName  <> T.PayrollDescriptionShortName
									   OR S.PayrollDescriptionLongName  <> T.PayrollDescriptionLongName
									    )
                 THEN UPDATE SET
                                 T.PayrollElement = S.PayrollElement,
                                 T.PayrollDescriptionShortName = S.PayrollDescriptionShortName,
                                 T.PayrollDescriptionLongName = S.PayrollDescriptionLongName,
                                 T.EDWCreatedDate = S.EDWCreatedDate
                                 
                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT(PayrollCode,
						  PayrollElement,          
						  PayrollDescriptionShortName,          
						  PayrollDescriptionLongName,
						  EDWCreatedDate)
				   VALUES(PayrollCode,
						  PayrollElement,          
						  PayrollDescriptionShortName,          
						  PayrollDescriptionLongName,
						  EDWCreatedDate )
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
             DROP TABLE #DimPayrollTypeUpsert;

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
             EXEC dbo.spMISC_StagingEDWETLBatchControl
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