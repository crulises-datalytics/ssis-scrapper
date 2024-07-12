CREATE PROCEDURE [dbo].[spBING_EDW_Generate_DimLeadEventType]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
     BEGIN
         -- ================================================================================
         -- 
         -- Stored Procedure:   spBING_EDW_Generate_DimLeadEventType
         --
         -- Purpose:            Populates the DimLeadEventType table in BING_EDW.
         --                     The table in question is almost static - that is, we don't
         --                         expect the data to change often.  However, we have
         --                         the population process encapsulated in a proc if we  
         --                         need to update or [re]deploy the entire database solution
         --                         from scratch.	  
         --
         --                     The logic for this was in an SSIS Project called PostDeploymentExecution,
         --                         which was lost and forgotten in our Source Repository.  Putting it here
         --                         makes it easier to locate what's actually populating the table	   	    	    	      	    
         --
         --
         -- Populates:          Truncates and [re]loads BING_EDW..DimLeadEventType
         --
         -- Usage:              EXEC dbo.spBING_EDW_Generate_DimLeadEventType @DebugMode = 1
         --
         -- --------------------------------------------------------------------------------
         --
         -- Change Log:		   
         -- ----------
         --
         -- Date         Modified By     Comments
         -- ----         -----------     --------
         --
         --  2/13/18     sburke          BNG-259 - Initial version of proc
         --			 
         -- ================================================================================
         SET NOCOUNT ON;
         --
         -- Housekeeping Variables
         -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimLeadEventType';
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
         EXEC [dbo].[spEDWBeginAuditLog]
              @SourceName = @SourceName,
              @AuditId = @AuditId OUTPUT; 		 	 
         --
         BEGIN TRY
             DELETE FROM dbo.DimLeadEventType
             WHERE LeadEventTypeKey > 0;
             SELECT @DeleteCount = @@ROWCOUNT;
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Deleted '+CONVERT(NVARCHAR(20), @DeleteCount)+' from Target.';
                     PRINT @DebugMsg;
             END;

             -- ================================================================================
             -- Unknown and Not Applicable Seed rows already loaded via stored proc
             --     spBING_EDW_Build_DimensionSeedRows.
             -- ================================================================================

             -- ================================================================================
             -- Insert into dbo.DimLeadEventType
             -- ================================================================================
             INSERT INTO dbo.DimLeadEventType
             (LeadEventTypeKey,
              LeadEventTypeName,
              EDWCreatedDate
             )
                    SELECT 1,
                           'Inquiry',
                           @EDWRunDateTime
                    UNION
                    SELECT 2,
                           'Interaction',
                           @EDWRunDateTime
                    UNION
                    SELECT 3,
                           'Scheduled Tour',
                           @EDWRunDateTime
                    UNION
                    SELECT 4,
                           'Completed Tour',
                           @EDWRunDateTime
                    UNION
                    SELECT 5,
                           'Conversion',
                           @EDWRunDateTime;
             SELECT @SourceCount = @@ROWCOUNT;
             SELECT @InsertCount = @SourceCount;
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(NVARCHAR(20), @InsertCount)+' into Target.';
                     PRINT @DebugMsg;
             END;

             --
             -- Write our successful run to the EDW AuditLog 
             --
             EXEC [dbo].[spEDWEndAuditLog]
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
	    --
	    -- Catch, and throw the error back to the calling procedure or client
	    --
         BEGIN CATCH
		   --
		   -- Write our failed run to the EDW AuditLog 
		   --
             EXEC [dbo].[spEDWErrorAuditLog]
                  @AuditId = @AuditId;
             DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
             SELECT @ErrMsg = 'Sub-procedure '+@ProcName+' - '+ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;
GO


