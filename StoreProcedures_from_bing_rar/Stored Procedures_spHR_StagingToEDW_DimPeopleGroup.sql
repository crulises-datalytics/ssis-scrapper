
CREATE PROCEDURE [dbo].[spHR_StagingToEDW_DimPeopleGroup]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingToEDW_DimPeopleGroup
    --
    -- Purpose:            Performs the Insert / Update / Delete ETL process for
    --                         the DimPeopleGroup table from Staging to BING_EDW.
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
    -- Usage:              EXEC dbo.spHR_StagingToEDW_DimPeopleGroup @DebugMode = 1	
    -- 
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date        Modified By         Comments
    -- ----        -----------         --------
    --
    -- 12/20/17    Banandesi              BNG-267 - Created EDW DimPeopleGroup ETL load to use stored proc over Data Flow in SSIS
  		 
    -- ================================================================================    
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimPeopleGroup';
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

             CREATE TABLE #DimPeopleGroupUpsert
             (
              PeopleGroupID                 INT NOT NULL,
              PeopleGroupName               VARCHAR(150) NOT NULL,
              PeopleGroupAssignmentName     VARCHAR(150) NOT NULL,
              PeopleGroupLineOfBusinessName VARCHAR(150) NOT NULL,
              PeopleGroupFlexAttribute1     VARCHAR(150) NULL,
              PeopleGroupFlexAttribute2     VARCHAR(150) NULL,
              PeopleGroupFlexAttribute3     VARCHAR(150) NULL,
              PeopleGroupFlexAttribute4     VARCHAR(150) NULL,
              PeopleGroupFlexAttribute5     VARCHAR(150) NULL,
              PeopleGroupCreatedDate        DATETIME2 NOT NULL,
              PeopleGroupCreatedUser        INT NOT NULL,
              PeopleGroupModifiedDate       DATETIME2 NOT NULL,
              PeopleGroupModifiedUser       INT NOT NULL,
              EDWCreatedDate                DATETIME2 NOT NULL,
              EDWModifiedDate               DATETIME2 NOT NULL
		    )
		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table from Source, and create any helper indexes
		   --
		   -- ================================================================================
             INSERT INTO #DimPeopleGroupUpsert
             EXEC dbo.spHR_StagingTransform_DimPeopleGroup
                  @EDWRunDateTime;
		   
		   -- Get how many rows were extracted from source 

             SELECT @SourceCount = COUNT(1)
             FROM #DimPeopleGroupUpsert;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Source.';
             PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
             CREATE NONCLUSTERED INDEX XPKDimPeopleGroupUpsert ON #DimPeopleGroupUpsert
             ([PeopleGroupID] ASC
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
             MERGE [BING_EDW].[dbo].[DimPeopleGroup] T
             USING #DimPeopleGroupUpsert S
             ON(S.PeopleGroupID = T.PeopleGroupID)
                 WHEN MATCHED AND S.PeopleGroupName <> T.PeopleGroupName
                                  OR S.PeopleGroupAssignmentName <> T.PeopleGroupAssignmentName
                                  OR S.PeopleGroupLineOfBusinessName <> T.PeopleGroupLineOfBusinessName
                                  OR S.PeopleGroupFlexAttribute1 <> T.PeopleGroupFlexAttribute1
                                  OR S.PeopleGroupFlexAttribute2 <> T.PeopleGroupFlexAttribute2
                                  OR S.PeopleGroupFlexAttribute3 <> T.PeopleGroupFlexAttribute3
                                  OR S.PeopleGroupFlexAttribute4 <> T.PeopleGroupFlexAttribute4
                                  OR S.PeopleGroupFlexAttribute5 <> T.PeopleGroupFlexAttribute5
                                  OR S.PeopleGroupCreatedDate <> T.PeopleGroupCreatedDate
                                  OR S.PeopleGroupCreatedUser <> T.PeopleGroupCreatedUser
                                  OR S.PeopleGroupModifiedDate <> T.PeopleGroupModifiedDate
                                  OR S.PeopleGroupModifiedUser <> T.PeopleGroupModifiedUser
                 THEN UPDATE SET
                                 T.PeopleGroupName = S.PeopleGroupName,
                                 T.PeopleGroupAssignmentName = S.PeopleGroupAssignmentName,
                                 T.PeopleGroupLineOfBusinessName = S.PeopleGroupLineOfBusinessName,
                                 T.PeopleGroupFlexAttribute1 = S.PeopleGroupFlexAttribute1,
                                 T.PeopleGroupFlexAttribute2 = S.PeopleGroupFlexAttribute2,
                                 T.PeopleGroupFlexAttribute3 = S.PeopleGroupFlexAttribute3,
                                 T.PeopleGroupFlexAttribute4 = S.PeopleGroupFlexAttribute4,
                                 T.PeopleGroupFlexAttribute5 = S.PeopleGroupFlexAttribute5,
                                 T.PeopleGroupCreatedDate = S.PeopleGroupCreatedDate,
                                 T.PeopleGroupCreatedUser = S.PeopleGroupCreatedUser,
                                 T.PeopleGroupModifiedDate = S.PeopleGroupModifiedDate,
                                 T.PeopleGroupModifiedUser = S.PeopleGroupModifiedUser,
						   T.EDWCreatedDate = S.EDWCreatedDate,
                                 T.EDWModifiedDate = S.EDWModifiedDate
                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT(PeopleGroupID,
			           PeopleGroupName,
                          PeopleGroupAssignmentName,
                          PeopleGroupLineOfBusinessName,
                          PeopleGroupFlexAttribute1,
                          PeopleGroupFlexAttribute2,
                          PeopleGroupFlexAttribute3,
                          PeopleGroupFlexAttribute4,
                          PeopleGroupFlexAttribute5,
                          PeopleGroupCreatedDate,
                          PeopleGroupCreatedUser,
                          PeopleGroupModifiedDate,
                          PeopleGroupModifiedUser,
                          EDWCreatedDate,
                          EDWModifiedDate)
                   VALUES
                         (PeopleGroupID,
			           PeopleGroupName,
                          PeopleGroupAssignmentName,
                          PeopleGroupLineOfBusinessName,
                          PeopleGroupFlexAttribute1,
                          PeopleGroupFlexAttribute2,
                          PeopleGroupFlexAttribute3,
                          PeopleGroupFlexAttribute4,
                          PeopleGroupFlexAttribute5,
                          PeopleGroupCreatedDate,
                          PeopleGroupCreatedUser,
                          PeopleGroupModifiedDate,
                          PeopleGroupModifiedUser,
                          EDWCreatedDate,
                          EDWModifiedDate
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
             DROP TABLE #DimPeopleGroupUpsert;

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