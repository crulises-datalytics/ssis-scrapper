CREATE PROCEDURE [dbo].[spBING_EDW_Generate_DimLifecycleStatus]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
     BEGIN
         -- ================================================================================
         -- 
         -- Stored Procedure:   spBING_EDW_Generate_DimLifecycleStatus
         --
         -- Purpose:            Populates the DimLifecycleStatus table in BING_EDW.
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
         -- Populates:          Truncates and [re]loads BING_EDW..DimLifecycleStatus
         --
         -- Usage:              EXEC dbo.spBING_EDW_Generate_DimLifecycleStatus @DebugMode = 1
         --
         -- --------------------------------------------------------------------------------
         --
         -- Change Log:		   
         -- ----------
         --
         -- Date         Modified By     Comments
         -- ----         -----------     --------
         --
         -- 12/01/17     sburke          Initial version   
         --  1/09/17     sburke          BNG-998 - Add -2 'Not Applicable' records for Dimension
         --                                 tables	            
         --			 
         -- ================================================================================
         SET NOCOUNT ON;
         --
         -- Housekeeping Variables
         -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimLifecycleStatus';
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
             FROM dbo.DimLifecycleStatus;
             TRUNCATE TABLE dbo.DimLifecycleStatus;
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Deleted '+CONVERT(NVARCHAR(20), @DeleteCount)+' from Target.';
                     PRINT @DebugMsg;
             END;             

             -- ================================================================================
             -- Insert into dbo.DimLifecycleStatus
             -- ================================================================================
             WITH LifecycleStatus
                  AS (
                  SELECT-1 AS LifecycleStatusKey,
                        'Unknown Lifecycle Status' AS LifecycleStatusName,
                        99999 AS LifecycleStatusImportance,
                        99999 AS LifecycleStatusSort,
                        'Unknown Lifecycle Category' AS LifecycleCategory,
                        99999 AS LifecycleCategorySort,
                        'Unknown Lifecycle Subcategory' AS LifecycleSubcategory,
                        99999 AS LifecycleSubcategorySort
                  UNION
                  SELECT-2 AS LifecycleStatusKey,
                        'Not Applicable Lifecycle Status' AS LifecycleStatusName,
                        99999 AS LifecycleStatusImportance,
                        99999 AS LifecycleStatusSort,
                        'Not Applicable Lifecycle Category' AS LifecycleCategory,
                        99999 AS LifecycleCategorySort,
                        'Not Applicable Lifecycle Subcategory' AS LifecycleSubcategory,
                        99999 AS LifecycleSubcategorySort
                  UNION
                  SELECT 1 AS LifecycleStatusKey,
                         'Pre-Enrolled (New)' AS LifecycleStatusName,
                         11 AS LifecycleStatusImportance,
                         1 AS LifecycleStatusSort,
                         'Inactive' AS LifecycleCategory,
                         2 AS LifecycleCategorySort,
                         'Pre-Enrolled' AS LifecycleSubcategory,
                         1 AS LifecycleSubcategorySort
                  UNION
                  SELECT 2 AS LifecycleStatusKey,
                         'Pre-Enrolled (Existing)' AS LifecycleStatusName,
                         10 AS LifecycleStatusImportance,
                         2 AS LifecycleStatusSort,
                         'Inactive' AS LifecycleCategory,
                         2 AS LifecycleCategorySort,
                         'Pre-Enrolled' AS LifecycleSubcategory,
                         1 AS LifecycleSubcategorySort
                  UNION
                  SELECT 3 AS LifecycleStatusKey,
                         'New' AS LifecycleStatusName,
                         1 AS LifecycleStatusImportance,
                         3 AS LifecycleStatusSort,
                         'Active' AS LifecycleCategory,
                         1 AS LifecycleCategorySort,
                         'New' AS LifecycleSubcategory,
                         2 AS LifecycleSubcategorySort
                  UNION
                  SELECT 4 AS LifecycleStatusKey,
                         'Transferred' AS LifecycleStatusName,
                         3 AS LifecycleStatusImportance,
                         4 AS LifecycleStatusSort,
                         'Active' AS LifecycleCategory,
                         1 AS LifecycleCategorySort,
                         'New' AS LifecycleSubcategory,
                         2 AS LifecycleSubcategorySort
                  UNION
                  SELECT 5 AS LifecycleStatusKey,
                         'Enrolled' AS LifecycleStatusName,
                         8 AS LifecycleStatusImportance,
                         5 AS LifecycleStatusSort,
                         'Active' AS LifecycleCategory,
                         1 AS LifecycleCategorySort,
                         'Enrolled' AS LifecycleSubcategory,
                         3 AS LifecycleSubcategorySort
                  UNION
                  SELECT 6 AS LifecycleStatusKey,
                         'Reserved' AS LifecycleStatusName,
                         9 AS LifecycleStatusImportance,
                         6 AS LifecycleStatusSort,
                         'Active' AS LifecycleCategory,
                         1 AS LifecycleCategorySort,
                         'Enrolled' AS LifecycleSubcategory,
                         3 AS LifecycleSubcategorySort
                  UNION
                  SELECT 7 AS LifecycleStatusKey,
                         'At-Risk (Disenrollment)' AS LifecycleStatusName,
                         6 AS LifecycleStatusImportance,
                         7 AS LifecycleStatusSort,
                         'Active' AS LifecycleCategory,
                         1 AS LifecycleCategorySort,
                         'At-Risk' AS LifecycleSubcategory,
                         4 AS LifecycleSubcategorySort
                  UNION
                  SELECT 8 AS LifecycleStatusKey,
                         'At-Risk (A/R)' AS LifecycleStatusName,
                         7 AS LifecycleStatusImportance,
                         8 AS LifecycleStatusSort,
                         'Active' AS LifecycleCategory,
                         1 AS LifecycleCategorySort,
                         'At-Risk' AS LifecycleSubcategory,
                         4 AS LifecycleSubcategorySort
                  UNION
                  SELECT 9 AS LifecycleStatusKey,
                         'Re-Enrolled (Short Term)' AS LifecycleStatusName,
                         5 AS LifecycleStatusImportance,
                         9 AS LifecycleStatusSort,
                         'Active' AS LifecycleCategory,
                         1 AS LifecycleCategorySort,
                         'Re-Enrolled' AS LifecycleSubcategory,
                         5 AS LifecycleSubcategorySort
                  UNION
                  SELECT 10 AS LifecycleStatusKey,
                         'Re-Enrolled (Long Term)' AS LifecycleStatusName,
                         4 AS LifecycleStatusImportance,
                         10 AS LifecycleStatusSort,
                         'Active' AS LifecycleCategory,
                         1 AS LifecycleCategorySort,
                         'Re-Enrolled' AS LifecycleSubcategory,
                         5 AS LifecycleSubcategorySort
                  UNION
                  SELECT 11 AS LifecycleStatusKey,
                         'Withdrawing' AS LifecycleStatusName,
                         2 AS LifecycleStatusImportance,
                         11 AS LifecycleStatusSort,
                         'Active' AS LifecycleCategory,
                         2 AS LifecycleCategorySort,
                         'Withdrawn' AS LifecycleSubcategory,
                         6 AS LifecycleSubcategorySort
                  UNION
                  SELECT 12 AS LifecycleStatusKey,
                         'Withdrawn' AS LifecycleStatusName,
                         12 AS LifecycleStatusImportance,
                         12 AS LifecycleStatusSort,
                         'Inactive' AS LifecycleCategory,
                         2 AS LifecycleCategorySort,
                         'Withdrawn' AS LifecycleSubcategory,
                         6 AS LifecycleSubcategorySort)
                  INSERT INTO dbo.DimLifecycleStatus
                  ([LifecycleStatusKey],
                   [LifecycleStatusName],
                   [LifecycleStatusImportance],
                   [LifecycleStatusSort],
                   [LifecycleCategory],
                   [LifecycleCategorySort],
                   [LifecycleSubcategory],
                   [LifecycleSubcategorySort],
                   [EDWCreatedDate],
                   [EDWCreatedBy],
                   [EDWModifiedDate],
                   [EDWModifiedBy]
                  )
                         SELECT LifecycleStatus.LifecycleStatusKey,
                                LifecycleStatus.LifecycleStatusName,
                                LifecycleStatus.LifecycleStatusImportance,
                                LifecycleStatus.LifecycleStatusSort,
                                LifecycleStatus.LifecycleCategory,
                                LifecycleStatus.LifecycleCategorySort,
                                LifecycleStatus.LifecycleSubcategory,
                                LifecycleStatus.LifecycleSubcategorySort,
                                GETDATE() AS EDWCreatedDate,
                                CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWCreatedBy,
                                GETDATE() AS EDWModifiedDate,
                                CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWModifiedBy
                         FROM LifecycleStatus;
             SELECT @SourceCount = @SourceCount + @@ROWCOUNT + 1; -- Seed Row is the +1

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


