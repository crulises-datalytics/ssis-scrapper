

/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spGL_StagingToEDW_BridgeCompanyRollup'
)
    DROP PROCEDURE dbo.spGL_StagingToEDW_BridgeCompanyRollup;
GO
*/
CREATE PROCEDURE [dbo].[spGL_StagingToEDW_BridgeCompanyRollup]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spGL_StagingToEDW_BridgeCompanyRollup
    --
    -- Purpose:            Performs the Insert / Update / Delete ETL process for
    --                         the DimCompany table from Staging to BING_EDW.
    --
    --                         Step 1: Truncate BridgeCompanyRollup Table
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
    -- Usage:			   EXEC dbo.spGL_StagingToEDW_BridgeCompanyRollup @EDWRunDateTime = @EDWRunDateTime, @DebugMode = 1
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By     Comments
    -- ----         -----------     --------
    --
    -- 12/04/17    ADevabhakthuni           BNG-910 - Refactor BridgeCompanyRollup StagingToEDW load. (Initial version of proc, 
	--                                  converted from SSIS logic)
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;

	    --
	    -- Housekeeping Variables
	    --
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'BridgeCompanyRollup';
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
	    -- Extract from Source, Inserts in a single transaction.  
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
		   -- Truncate BridgeCompnayRollup Table
		   --
		   -- ================================================================================
		     TRUNCATE TABLE  [BING_EDW].[dbo].[BridgeCompanyRollup]
		   -- ================================================================================
		   --
		   -- S T E P   2.
		   --
		   -- Insert Into BING_EDW Table 
		   --
		   -- ================================================================================
		
         
		     INSERT INTO  [BING_EDW].[dbo].[BridgeCompanyRollup]
             EXEC dbo.spGL_StagingTransform_BridgeCompanyRollup
                  @EDWRunDateTime;
			   -- Get how many rows were extracted from source 
             SELECT @SourceCount = count(1) from dbo.tfnCompanyRollupBridge()
		   
		   -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Extracted '+CONVERT(NVARCHAR(20), @SourceCount)+' rows from Source.';
             PRINT @DebugMsg;
          -- Count the number of inserts 
		  SELECT @InsertCount=COUNT(1) FROM [BING_EDW].[dbo].[BridgeCompanyRollup] ;	
		  
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

                  @SourceCount = @SourceCount,				  @UpdateCount = @UpdateCount,
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