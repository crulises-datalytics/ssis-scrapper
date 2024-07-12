CREATE PROCEDURE [rep].[spBING_EDW_Generate_repReportFilter]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
     BEGIN
         -- ================================================================================
         -- 
         -- Stored Procedure:   spBING_EDW_Generate_ReportFilter
         --
         -- Purpose:            Populates the rep.ReportFilter table in BING_EDW.
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
         -- Populates:          Truncates and [re]loads BING_EDW.rep.ReportFilter
         --
         -- Usage:              EXEC rep.spBING_EDW_Generate_ReportFilter @DebugMode = 1
         --
         -- --------------------------------------------------------------------------------
         --
         -- Change Log:		   
         -- ----------
         --
         -- Date         Modified By     Comments
         -- ----         -----------     --------
         --
         --  2/13/19     aquitta         BNG-4522 - Data Driven subscriptions do not need to be sent to Center Leaders
         --			 
         -- ================================================================================
         SET NOCOUNT ON;
         --
         -- Housekeeping Variables
         -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'rep.ReportFilter';
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
             SELECT @DeleteCount = COUNT(1)
             FROM rep.ReportFilter;
             TRUNCATE TABLE rep.ReportFilter;
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Deleted '+CONVERT(NVARCHAR(20), @DeleteCount)+' from Target.';
                     PRINT @DebugMsg;
             END;
             --
             -- Add Seed row
             --
             DBCC CHECKIDENT('rep.[ReportFilter]', RESEED, 1);
             SET IDENTITY_INSERT rep.ReportFilter ON;
             INSERT INTO [rep].[ReportFilter]
             (	[ReportFilterID],
				   [ReportFilterName],
				   [FilterDescription],
				   [EDWCreatedDate],
				   [EDWCreatedBy],
				   [EDWModifiedDate],
				   [EDWModifiedBy],
				   [Deleted]
             )
                    SELECT-1,
                          'Unknown Filter',				-- ReportFilterName
                          'Unknown Description',		-- FilterDescription
                          GETDATE(),
                          SYSTEM_USER,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL
                    UNION
                    SELECT-2,
                          'Not Applicable Filter',			-- ReportFilterName
                          'Not Applicable Description',		-- FilterDescription
                          GETDATE(),
                          SYSTEM_USER,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL;
             SET IDENTITY_INSERT rep.ReportFilter OFF;

             -- ================================================================================
             -- Insert into dbo.DimARAgingBucket
             -- ================================================================================
             INSERT INTO [rep].[ReportFilter]
             (	[ReportFilterName],
				[FilterDescription],
				[EDWCreatedDate],
				[EDWCreatedBy],
				[EDWModifiedDate],
				[EDWModifiedBy],
				[Deleted]
             )
                    SELECT
						        'Center Only' AS ReportFilterName,				-- ReportFilterName
                          'OrgTypeName = ''Center'' ',						-- FilterDescription
                          GETDATE(),
                          SYSTEM_USER,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL
                    UNION
                    SELECT
						        'District Region' AS ReportFilterName,			-- ReportFilterName
                          'OrgTypeName in (''District'',''Region'') ',		-- FilterDescription
                          GETDATE(),
                          SYSTEM_USER,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL
                    UNION
                    SELECT
						        'Region 09' AS ReportFilterName,			-- ReportFilterName
                          'CHARINDEX(''[R09 Region 09]'', OrganizationOrgHierarchy) > 0 ',		-- FilterDescription
                          GETDATE(),
                          SYSTEM_USER,
                          GETDATE(),
                          SYSTEM_USER,
                          NULL
                    ORDER BY ReportFilterName;
             SELECT @SourceCount = @@ROWCOUNT + 1; -- The Seed Row is the +1

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
