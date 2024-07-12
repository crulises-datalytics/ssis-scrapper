CREATE PROCEDURE [dbo].[spGL_StagingTransform_DimCostCenterType] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingTransform_DimCostCenterType
    --
    -- Purpose:            Performs the transformation logic with the source database
    --                         for a given Fact or Dimension table, and returns the
    --                         results set to the caller (usually for populating a
    --                         temporary table).
    --
    -- Parameters:         @EDWRunDateTime - Used to populate any 'current date' fields, rather than
    --                         making numerous GETDATE() calls  
    --				   
    -- Returns:            Results set containing the transformed data ready for
    --                         consumption by the ETL process for load into BING_EDW
    --
    -- Usage:              INSERT #TemplateUpsert -- (Temporary table)
    --                     EXEC dbo.spGL_StagingTransform_DimCostCenterType 
    -- 
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
    -- 12/04/17     sburke          BNG-757 - Replacing the old tfnCostCenterTypes UDF as
    --                                  part of the Peiordic GL Refactoring work	
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;
	    --
	    -- Housekeeping Variables
	    -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
	    --
	    -- If we do not get an @EDWRunDateTime input, set to current date
	    --
         IF @EDWRunDateTime IS NULL
             SET @EDWRunDateTime = GETDATE();
	    --
	    -- Execute the extract / transform from the Staging database source
	    --
         BEGIN TRY
             SELECT COALESCE(NULLIF(CostCenterTypeID, ''), '-1') AS CostCenterTypeID,
                    COALESCE(NULLIF(CostCenterTypeName, ''), 'Unknown Cost Center Type') AS CostCenterTypeName,
                    COALESCE(NULLIF(BusinessUnitCode, ''), 'Unknown Business Unit') AS CCTBusinessUnitCode,
                    COALESCE(NULLIF(BusinessUnitName, ''), 'Unknown Business Unit') AS CCTBusinessUnitName,
                    COALESCE(NULLIF(LineOfBusinessCode, ''), 'Unknown Line of Business') AS CCTLineOfBusinessCode,
                    COALESCE(NULLIF(LineOfBusinessName, ''), 'Unknown Line of Business') AS CCTLineOfBusinessName,
                    COALESCE(NULLIF(LineOfBusinessSubcategoryCode, ''), 'Unknown Line of Business Subcategory') AS CCTLineOfBusinessSubcategoryCode,
                    COALESCE(NULLIF(LineOfBusinessSubcategoryName, ''), 'Unknown Line of Business Subcategory') AS CCTLineOfBusinessSubcategoryName,
                    COALESCE(NULLIF(LineOfBusinessCategoryCode, ''), 'Unknown Line of Business Category') AS CCTLineOfBusinessCategoryCode,
                    COALESCE(NULLIF(LineOfBusinessCategoryName, ''), 'Unknown Line of Business Category') AS CCTLineOfBusinessCategoryName,
                    COALESCE(NULLIF(OrganizationLevelCode, ''), 'Unknown Organizational Level') AS CCTOrganizationLevelCode,
                    COALESCE(NULLIF(OrganizationLevelName, ''), 'Unknown Organizational Level') AS CCTOrganizationLevelName,
                    COALESCE(NULLIF(FunctionCode, ''), 'Unknown Function') AS CCTFunctionCode,
                    COALESCE(NULLIF(FunctionName, ''), 'Unknown Function') AS CCTFunctionName,
                    @EDWRunDateTime AS EDWCreatedDate,
                    CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWCreatedBy,
                    @EDWRunDateTime AS EDWModifiedDate,
                    CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWModifiedBy,
				NULL AS Deleted
             FROM dbo.tfnGL_StagingGenerate_CostCenterTypes();
         END TRY
	    --
	    -- Catch, and throw the error back to the calling procedure or client
	    --
         BEGIN CATCH
             DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
             SELECT @ErrMsg = 'Sub-procedure '+@ProcName+' - '+ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;
GO
