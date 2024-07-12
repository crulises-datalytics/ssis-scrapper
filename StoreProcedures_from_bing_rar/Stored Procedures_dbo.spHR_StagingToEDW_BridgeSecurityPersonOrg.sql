CREATE PROCEDURE [dbo].[spHR_StagingToEDW_BridgeSecurityPersonOrg]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS

-- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingToEDW_BridgeSecurityPersonOrg
    --
    -- Purpose:            Performs the truncate and reload ETL process for
    --                         the BridgeSecurityPersonOrg table from Staging to BING_EDW.
    --
    --                         Step 1: Truncate BridgeSecurityPersonOrg Table
    --                         Step 2: Insert for this EDW table load
    --                         Step 3: Output Source / Insert, 
    --                             commit the transaction, and tidy-up
    --
    -- Parameters:             @EDWRunDateTime - Used to populate any 'current date' fields, rather than
    --                             making numerous GETDATE() calls  
    --                         @DebugMode - Used just for development & debug purposes,
    --                             outputting helpful info back to the caller.  Not
    --                             required for Production, and does not affect any
    --                             core logic.
    --
    -- Usage:			   EXEC dbo.spHR_StagingToEDW_BridgeSecurityPersonOrg @EDWRunDateTime = @EDWRunDateTime, @DebugMode = 1
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By     Comments
    -- ----         -----------     --------
    --
    -- 12/04/17    Valimineti           BNG-564 - Create BridgeSecurityPersonOrg StagingToEDW load. (Initial version of proc, 
	--                                  converted from SSIS logic)
    -- 11/19/18    anmorales            BNG-4438 - Fix issue in the code which caused Deleted entries to be entered.
    --			 
    -- ================================================================================
	BEGIN
     SET NOCOUNT ON;
	 	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'BridgeSecurityPersonOrg';
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
		   -- Truncate BridgeSecurityPersonOrg Table
		   --
		   -- ================================================================================
				TRUNCATE TABLE BING_EDW.dbo.BridgeSecurityPersonOrg
			-- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Insert Into BING_EDW Table 
		   --
		   -- ================================================================================

		Insert into BING_EDW.dbo.BridgeSecurityPersonOrg
			(
				PersonKey,
				OrgKey,
				OrgSelfFlag,
				OrgSelfDescendentsFlag,
				OrgLevelUpDescendentsFlag,
				EDWCreatedDate
			)
		Select	PersonKey,
				OrgKey,
				case when OrgSelfFlag = 'Y' then 1 else 0 end as OrgSelfFlag,
				case when OrgSelfDescendantsFlag = 'Y' then 1 else 0 end as OrgSelfDescendantsFlag,
				case when OrgLevelUpDescendantsFlag = 'Y' then 1 else 0 end as OrgLevelUpDescendantsFlag,
				@EDWRunDateTime
		from dbo.OrgLeaderAccess OLA
		 INNER JOIN BING_EDW.dbo.DimPerson DP on OLA.EmployeeNumber=DP.EmployeeNumber
		 INNER JOIN BING_EDW.dbo.DimOrganization DO on OLA.CanAccessOrgID=DO.OrgID and DO.EDWEndDate is null
		 WHERE OLA.Deleted IS NULL
		  -- Get how many rows were extracted from source 
             SELECT @SourceCount = count(1) from dbo.OrgLeaderAccess OLA
									  INNER JOIN BING_EDW.dbo.DimPerson DP on OLA.EmployeeNumber=DP.EmployeeNumber
									  INNER JOIN BING_EDW.dbo.DimOrganization DO on OLA.CanAccessOrgID=DO.OrgID and DO.EDWEndDate is null
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Source.';
             PRINT @DebugMsg;
          -- Count the number of inserts 
		  SELECT @InsertCount=COUNT(1) FROM [BING_EDW].[dbo].[BridgeSecurityPersonOrg] ;	
		  
		   -- Debug output progress
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(NVARCHAR(20), @InsertCount)+' rows into Target.';
                     PRINT @DebugMsg;
					 END ;

		   -- ================================================================================
		   --
		   -- S T E P   3.
		   --
		   -- Output Source / Insert / commit the transaction,
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
		   -- Write our successful run to the EDW AuditLog 
		   --
             EXEC [BING_EDW].[dbo].[spEDWEndAuditLog]
                  @InsertCount = @InsertCount,
				  @SourceCount = @SourceCount,				  
				  @UpdateCount = @UpdateCount,
				  @DeleteCount = @DeleteCount,
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
		   -- Raise error
		   --			 
             DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
             SELECT @ErrMsg = ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;