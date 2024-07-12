/* 
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spGL_StagingToEDW_DimCostCenterType'
)
    DROP PROCEDURE dbo.spGL_StagingToEDW_DimCostCenterType;
GO
*/
CREATE PROCEDURE dbo.spGL_StagingToEDW_DimCostCenterType
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spGL_StagingToEDW_DimCostCenterType
    --
    --	Purpose:           Performs the Insert / Update / Delete ETL process for
    --                         the DimCostCenterType table from Staging to BING_EDW.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                                 sub-procedure spHR_StagingTransform_DimOrganization, 
    --                                 and create any helper indexes
    --                         Step 3: Perform the Insert / Update (Merge) / deletes required 
    --                                 for this EDW table load			 
    --                         Step 4: Execute any automated tests associated with this EDW table load
    --                         Step 5: Output Source / Insert / Update / Delete counts to caller, 
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
    -- Usage:              EXEC dbo.spGL_StagingToEDW_DimCostCenterType @DebugMode = 1
    --                  
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By     Comments
    -- ----         -----------     --------
    --
    -- 10/17/17     sburke          BNG-673 - Fix duplicate Business Unit records in DimCostCenterType
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimCostCenterType';
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
         DECLARE @tblDeleteActions TABLE(MergeAction VARCHAR(20));
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
		   --
		   -- S T E P   1.
		   --
		   -- Create temporary landing #table
		   --
		   -- ================================================================================
             CREATE TABLE #DimCostCenterTypeUpsert
             ([CostCenterTypeID]                 [VARCHAR](4) NOT NULL,
              [CostCenterTypeName]               [VARCHAR](250) NOT NULL,
              [CCTBusinessUnitCode]              [VARCHAR](250) NOT NULL,
              [CCTBusinessUnitName]              [VARCHAR](250) NOT NULL,
              [CCTLineOfBusinessCode]            [VARCHAR](250) NOT NULL,
              [CCTLineOfBusinessName]            [VARCHAR](250) NOT NULL,
              [CCTLineOfBusinessSubcategoryCode] [VARCHAR](250) NOT NULL,
              [CCTLineOfBusinessSubcategoryName] [VARCHAR](250) NOT NULL,
              [CCTLineOfBusinessCategoryCode]    [VARCHAR](250) NOT NULL,
              [CCTLineOfBusinessCategoryName]    [VARCHAR](250) NOT NULL,
              [CCTOrganizationLevelCode]         [VARCHAR](250) NOT NULL,
              [CCTOrganizationLevelName]         [VARCHAR](250) NOT NULL,
              [CCTFunctionCode]                  [VARCHAR](250) NOT NULL,
              [CCTFunctionName]                  [VARCHAR](250) NOT NULL,
              [EDWCreatedDate]                   [DATETIME2](7) NOT NULL,
              [EDWCreatedBy]                     [VARCHAR](50) NOT NULL,
              [EDWModifiedDate]                  [DATETIME2](7) NOT NULL,
              [EDWModifiedBy]                    [VARCHAR](50) NOT NULL,
              [Deleted]                          [DATETIME2](7) NULL
             );
          

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table from Source, and create any helper indexes
		   --
		   -- ================================================================================
             INSERT INTO #DimCostCenterTypeUpsert
             EXEC dbo.spGL_StagingTransform_DimCostCenterType
                  @EDWRunDateTime;


		   -- Get how many rows were extracted from source 

             SELECT @SourceCount = COUNT(1)
             FROM #DimCostCenterTypeUpsert;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Source.';
             PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
             CREATE NONCLUSTERED INDEX XAK1DimCostCenterTypeUpsert ON #DimCostCenterTypeUpsert
             ([CostCenterTypeID] ASC
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
             MERGE [BING_EDW].[dbo].[DimCostCenterType] T
             USING #DimCostCenterTypeUpsert S
             ON(S.CostCenterTypeID = T.CostCenterTypeID)
                 WHEN MATCHED AND S.Deleted IS NULL
                                  AND (S.CostCenterTypeName <> T.CostCenterTypeName
                                       OR S.CCTBusinessUnitCode <> T.CCTBusinessUnitCode
                                       OR S.CCTBusinessUnitName <> T.CCTBusinessUnitName
                                       OR S.CCTLineOfBusinessCode <> T.CCTLineOfBusinessCode
                                       OR S.CCTLineOfBusinessName <> T.CCTLineOfBusinessName
                                       OR S.CCTLineOfBusinessSubcategoryCode <> T.CCTLineOfBusinessSubcategoryCode
                                       OR S.CCTLineOfBusinessSubcategoryName <> T.CCTLineOfBusinessSubcategoryName
                                       OR S.CCTLineOfBusinessCategoryCode <> T.CCTLineOfBusinessCategoryCode
                                       OR S.CCTLineOfBusinessCategoryName <> T.CCTLineOfBusinessCategoryName
                                       OR S.CCTOrganizationLevelCode <> T.CCTOrganizationLevelCode
                                       OR S.CCTOrganizationLevelName <> T.CCTOrganizationLevelName
                                       OR S.CCTFunctionCode <> T.CCTFunctionCode
                                       OR S.CCTFunctionName <> T.CCTFunctionName
                                       OR T.Deleted IS NOT NULL)
                 THEN UPDATE SET
                                 T.CostCenterTypeName = S.CostCenterTypeName,
                                 T.CCTBusinessUnitCode = S.CCTBusinessUnitCode,
                                 T.CCTBusinessUnitName = S.CCTBusinessUnitName,
                                 T.CCTLineOfBusinessCode = S.CCTLineOfBusinessCode,
                                 T.CCTLineOfBusinessName = S.CCTLineOfBusinessName,
                                 T.CCTLineOfBusinessSubcategoryCode = S.CCTLineOfBusinessSubcategoryCode,
                                 T.CCTLineOfBusinessSubcategoryName = S.CCTLineOfBusinessSubcategoryName,
                                 T.CCTLineOfBusinessCategoryCode = S.CCTLineOfBusinessCategoryCode,
                                 T.CCTLineOfBusinessCategoryName = S.CCTLineOfBusinessCategoryName,
                                 T.CCTOrganizationLevelCode = S.CCTOrganizationLevelCode,
                                 T.CCTOrganizationLevelName = S.CCTOrganizationLevelName,
                                 T.CCTFunctionCode = S.CCTFunctionCode,
                                 T.CCTFunctionName = S.CCTFunctionName,
                                 T.EDWCreatedDate = S.EDWCreatedDate,
                                 T.EDWCreatedBy = S.EDWCreatedBy,
                                 T.EDWModifiedDate = S.EDWModifiedDate,
                                 T.EDWModifiedBy = S.EDWModifiedBy,
                                 T.Deleted = S.Deleted
                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT(CostCenterTypeID,
                          CostCenterTypeName,
                          CCTBusinessUnitCode,
                          CCTBusinessUnitName,
                          CCTLineOfBusinessCode,
                          CCTLineOfBusinessName,
                          CCTLineOfBusinessSubcategoryCode,
                          CCTLineOfBusinessSubcategoryName,
                          CCTLineOfBusinessCategoryCode,
                          CCTLineOfBusinessCategoryName,
                          CCTOrganizationLevelCode,
                          CCTOrganizationLevelName,
                          CCTFunctionCode,
                          CCTFunctionName,
                          EDWCreatedDate,
                          EDWCreatedBy,
                          EDWModifiedDate,
                          EDWModifiedBy,
                          Deleted)
                   VALUES
             (CostCenterTypeID,
              CostCenterTypeName,
              CCTBusinessUnitCode,
              CCTBusinessUnitName,
              CCTLineOfBusinessCode,
              CCTLineOfBusinessName,
              CCTLineOfBusinessSubcategoryCode,
              CCTLineOfBusinessSubcategoryName,
              CCTLineOfBusinessCategoryCode,
              CCTLineOfBusinessCategoryName,
              CCTOrganizationLevelCode,
              CCTOrganizationLevelName,
              CCTFunctionCode,
              CCTFunctionName,
              EDWCreatedDate,
              EDWCreatedBy,
              EDWModifiedDate,
              EDWModifiedBy,
              Deleted
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
		   -- Perform the Merge statement for soft deletes
		   --
             MERGE [BING_EDW].[dbo].[DimCostCenterType] T
             USING #DimCostCenterTypeUpsert S
             ON(S.CostCenterTypeID = T.CostCenterTypeID)
                 WHEN MATCHED AND(S.Deleted IS NOT NULL
                                  AND t.Deleted IS NULL)
                 THEN UPDATE SET
                                 T.CostCenterTypeName = S.CostCenterTypeName,
                                 T.CCTBusinessUnitCode = S.CCTBusinessUnitCode,
                                 T.CCTBusinessUnitName = S.CCTBusinessUnitName,
                                 T.CCTLineOfBusinessCode = S.CCTLineOfBusinessCode,
                                 T.CCTLineOfBusinessName = S.CCTLineOfBusinessName,
                                 T.CCTLineOfBusinessSubcategoryCode = S.CCTLineOfBusinessSubcategoryCode,
                                 T.CCTLineOfBusinessSubcategoryName = S.CCTLineOfBusinessSubcategoryName,
                                 T.CCTLineOfBusinessCategoryCode = S.CCTLineOfBusinessCategoryCode,
                                 T.CCTLineOfBusinessCategoryName = S.CCTLineOfBusinessCategoryName,
                                 T.CCTOrganizationLevelCode = S.CCTOrganizationLevelCode,
                                 T.CCTOrganizationLevelName = S.CCTOrganizationLevelName,
                                 T.CCTFunctionCode = S.CCTFunctionCode,
                                 T.CCTFunctionName = S.CCTFunctionName,
                                 T.EDWCreatedDate = S.EDWCreatedDate,
                                 T.EDWCreatedBy = S.EDWCreatedBy,
                                 T.EDWModifiedDate = S.EDWModifiedDate,
                                 T.EDWModifiedBy = S.EDWModifiedBy,
                                 T.Deleted = S.Deleted
             OUTPUT $action
                    INTO @tblDeleteActions;
             SELECT @DeleteCount = SUM(Updated)
             FROM
             (
                 SELECT 0 AS Inserted,
                        COUNT(*) AS Updated
                 FROM @tblDeleteActions
                 WHERE MergeAction = 'UPDATE' -- Soft Deletes show up as Updates in $action
             ) merge_actions;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Soft Deleted '+CONVERT(NVARCHAR(20), @DeleteCount)+' from into Target.';
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
             DROP TABLE #DimCostCenterTypeUpsert;

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
             DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
             SELECT @ErrMsg = ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;
GO