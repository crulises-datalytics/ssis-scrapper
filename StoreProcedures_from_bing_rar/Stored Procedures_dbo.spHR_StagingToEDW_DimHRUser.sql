

/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spHR_StagingToEDW_DimHRUser'
)
    DROP PROCEDURE dbo.spHR_StagingToEDW_DimHRUser;
GO
*/
CREATE PROCEDURE [dbo].[spHR_StagingToEDW_DimHRUser]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingToEDW_DimHRUser
    --
    -- Purpose:            Performs the Insert / Update ETL process for
    --                         the DimHRUser table from Staging to BING_EDW.
    --
    --                         Step 1: Create temporary landing #table
    --                         Step 2: Populate the Landing table from Source by calling
    --                                 sub-procedure spHR_StagingTransform_DimHRUser, 
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
    -- Usage:              EXEC dbo.spHR_StagingToEDW_DimHRUser @DebugMode = 1	
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
    -- 12/20/17    ADevabhakthuni            BNG-265  DimHRUSer staging to EDW load.
    -- 06/11/21    Adevabhakthuni            BI-4820 Updated the HRUserCode field 		 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimHRUser';
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
             CREATE TABLE #DimHRUserUpsert
             (
	[HRUserEffectiveDate] [date] NOT NULL,
	[HRUserEndDate] [date] NOT NULL,
	[HRUserID] [int] NOT NULL,
	[HRUserCode] [varchar](100) NOT NULL,
	[HRUserName] [varchar](250) NOT NULL,
	[HRUserEmployeeNumber] [varchar](6) NULL,
	[HRUserEmployeeName] [varchar](250) NULL,
	[HRUserCreatedDate] [datetime2](7) NOT NULL,
	[HRUserCreatedUser] [int] NOT NULL,
	[HRUserModifiedDate] [datetime2](7) NOT NULL,
	[HRUserModifiedUser] [int] NOT NULL,
	[EDWCreatedDate] [datetime2](7) NOT NULL,
	[EDWModifiedDate] [datetime2](7) NOT NULL,
             );          

		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Populate the Landing table FROM Source, and create any helper indexes
		   --
		   -- ================================================================================
             INSERT INTO #DimHRUserUpsert
             EXEC dbo.spHR_StagingTransform_DimHRUser
                  @EDWRunDateTime;

		   -- Get how many rows were extracted FROM source 

             SELECT @SourceCount = COUNT(1)
             FROM #DimHRUserUpsert;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows FROM Source.';
             PRINT @DebugMsg;
		   --
		   -- Create helper index
		   --
             CREATE NONCLUSTERED INDEX XAK1DimHRUserUpsert ON #DimHRUserUpsert
             ([HRUserID] ASC
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
             MERGE [BING_EDW].[dbo].[DimHRUser] T
             USING #DimHRUserUpsert S
             ON(S.HRUserID = T.HRUserID)
                 WHEN MATCHED AND (S.HRUserEffectiveDate  <>  T.HRUserEffectiveDate
OR S.HRUserEndDate  <>  T.HRUserEndDate
OR S.HRUserCode  <>  T.HRUserCode
OR S.HRUserName  <>  T.HRUserName
OR S.HRUserEmployeeNumber  <>  T.HRUserEmployeeNumber
OR S.HRUserEmployeeName  <>  T.HRUserEmployeeName
OR S.HRUserCreatedDate  <>  T.HRUserCreatedDate
OR S.HRUserCreatedUser  <>  T.HRUserCreatedUser
OR S.HRUserModifiedDate  <>  T.HRUserModifiedDate
OR S.HRUserModifiedUser  <>  T.HRUserModifiedUser

 )
                 THEN UPDATE SET
                                T.HRUserEffectiveDate = S.HRUserEffectiveDate,
T.HRUserEndDate = S.HRUserEndDate,
T.HRUserCode = S.HRUserCode,
T.HRUserName = S.HRUserName,
T.HRUserEmployeeNumber = S.HRUserEmployeeNumber,
T.HRUserEmployeeName = S.HRUserEmployeeName,
T.HRUserCreatedDate = S.HRUserCreatedDate,
T.HRUserCreatedUser = S.HRUserCreatedUser,
T.HRUserModifiedDate = S.HRUserModifiedDate,
T.HRUserModifiedUser = S.HRUserModifiedUser,
T.EDWCreatedDate = S.EDWCreatedDate,
T.EDWModifiedDate = S.EDWModifiedDate

                 WHEN NOT MATCHED BY TARGET
                 THEN
                   INSERT(HRUserEffectiveDate,
HRUserEndDate,
HRUserID,
HRUserCode,
HRUserName,
HRUserEmployeeNumber,
HRUserEmployeeName,
HRUserCreatedDate,
HRUserCreatedUser,
HRUserModifiedDate,
HRUserModifiedUser,
EDWCreatedDate,
EDWModifiedDate

 )
				   VALUES(HRUserEffectiveDate,
HRUserEndDate,
HRUserID,
HRUserCode,
HRUserName,
HRUserEmployeeNumber,
HRUserEmployeeName,
HRUserCreatedDate,
HRUserCreatedUser,
HRUserModifiedDate,
HRUserModifiedUser,
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
             DROP TABLE #DimHRUserUpsert;

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