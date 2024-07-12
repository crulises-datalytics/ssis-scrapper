Create PROCEDURE [dbo].[spHR_StagingToEDW_FactEmployeeAssignment]
(@EDWRunDateTime    DATETIME2 = NULL, 
 @DebugMode         INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingToEDW_FactEmployeeAssignment
    --
    -- Purpose:            Performs the Insert / Update / Delete ETL process for
    --					  the FactEmployeeAssignment table from Staging to BING_EDW.
    --
    --                     Step 1: Truncate the Fact table
    --                     Step 2: Perform the Insert required 
    --                          for this EDW table load
    --                     Step 3: Execute any automated tests associated with this EDW table load
    --                     Step 4: Output Source / Insert / Update / Delete counts to caller, 
    --                         commit the transaction, and tidy-up
    --
    -- Parameters:		   @EDWRunDateTime
    --                     @AuditId - This is the Audit tracking ID used both in the EDWAuditLog and EDWBatchLoadLog tables 
    --                     @DebugMode - Used just for development & debug purposes,
    --                         outputting helpful info back to the caller.  Not
    --                         required for Production, and does not affect any
    --                         core logic.
    --
    --
    -- Usage:              EXEC dbo.spHR_StagingToEDW_FactEmployeeAssignment @EDWRunDateTime = @EDWRunDateTime, @DebugMode = 1
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date		Modified By		Comments
    -- ----		-----------		--------
    --
    -- 3/13/18       Banandesi		    BNG-273 - Intial version of EDW FactEmployeeAssignment load
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'FactEmployeeAssignment';
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
		   -- Truncate the Fact Table
		   --
		   -- ================================================================================

		   -- Get how many rows were extracted from source 

             SELECT @SourceCount = COUNT(1)
             FROM vAssignments;
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Source.';
             PRINT @DebugMsg;
		   
		   --
		   -- Delete whole data
		   --
			
			 SELECT @DeleteCount = COUNT(1)
             FROM BING_EDW.dbo.FactEmployeeAssignment;
             
			 Truncate table BING_EDW.dbo.FactEmployeeAssignment

		   -- Debug output progress
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Deleted '+CONVERT(NVARCHAR(20), @DeleteCount)+' rows from Target.';
                     PRINT @DebugMsg;
             END;
			 
		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Perform the Insert required for this EDW table load
		   --
		   -- For this Fact load, we do not update or merge - instead we delete and reload
		   --     the data 
		   --
		   -- ================================================================================
		   --
		   -- Insert the whole dataset
		   --
             INSERT INTO BING_EDW.dbo.FactEmployeeAssignment
             ( [AssignmentStartDateKey] 
	         ,[AssignmentEndDateKey] 
	         ,[AssignmentCurrentRecordFlag] 
	         ,[AssignmentPositionStartDateKey] 
	         ,[AssignmentProjectedEndDateKey] 
	         ,[EmploymentStartDateKey] 
	         ,[EmploymentEndDateKey] 
	         ,[EmploymentAdjustedServiceDateKey]
	         ,[EmploymentLastWorkedDateKey]  
	         ,[EmploymentTerminationNotifiedDateKey] 
		    ,[EmploymentTerminationAcceptedDateKey]
	         ,[EmploymentTerminationProjectedDateKey] 
              ,[EmploymentTerminationActualDateKey] 
	         ,[EmploymentLastPayrollProcessDateKey]
	         ,[PersonKey] 
	         ,[ExecutiveAssistantPersonKey]
	         ,[SupervisorPersonKey] 
	         ,[LocationKey] 
	         ,[OrgKey] 
	         ,[CompanyKey] 
	         ,[CostCenterTypeKey] 
	         ,[CostCenterKey] 
	         ,[PositionKey]
	         ,[PayGradeKey] 
	         ,[PeopleGroupKey] 
	         ,[EmployeeAssignmentTypeKey]
	         ,[AssignmentID] 
	         ,[EmploymentID] 
	         ,[AssignmentNumber] 
	         ,[AssignmentSequence] 
	         ,[AssignmentPositionSequence] 
	         ,[EmploymentTerminationComments] 
	         ,[EmploymentCreatedDate]
	         ,[EmploymentCreatedUser] 
	         ,[EmploymentModifiedDate] 
	         ,[EmploymentModifiedUser] 
	         ,[AssignmentCreatedDate] 
	         ,[AssignmentCreatedUser] 
	         ,[AssignmentModifiedDate] 
	         ,[AssignmentModifiedUser] 
	         ,[EDWCreatedDate] 
	         ,[EDWModifiedDate] 
             )

              EXEC [dbo].[spHR_StagingTransform_FactEmployeeAssignment];
             SELECT @InsertCount = @@ROWCOUNT;
		   
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
		   -- S T E P   3.
		   --
		   -- Execute any automated tests associated with this EDW table load
		   --
		   -- ================================================================================


		   -- ================================================================================
		   --
		   -- S T E P   4.
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