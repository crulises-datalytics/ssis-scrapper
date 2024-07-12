
/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spGL_StagingTransform_DimCompany'
)
    DROP PROCEDURE dbo.spGL_StagingTransform_DimCompany;
GO
*/
CREATE PROCEDURE [dbo].[spGL_StagingTransform_DimCompany] @EDWRunDateTime DATETIME2
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spGL_StagingTransform_DimCompany
    --
    -- Purpose:            Performs the transformation logic with the source database
    --                         for a given Fact or Dimension table, and returns the
    --                         results set to the caller (usually for populating a
    --                         temporary table).
    --
    -- Parameters:         @EDWRunDateTime - Not required, and defaults to GETDATE()
    --
    -- Returns:            Results set containing the transformed data ready for
    --                         consumption by the ETL process for load into BING_EDW
    --
    -- Usage:              INSERT INTO #DimCompanyUpsert -- (Temporary table)
    --                     EXEC dbo.spGL_StagingTransform_DimCompany
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date        Modified By      Comments
    -- ----        -----------      --------
    --
    -- 10/10/17    sburke           BNG-707 - Adding new CompanyTaxNumber column for 
	--                                  Cost Center Master. (Initial version of proc, 
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

	    --
	    -- If we do not get an @EDWRunDateTime input, set to current date
	    --
         IF @EDWRunDateTime IS NULL
             SET @EDWRunDateTime = GETDATE();
	    --
	    -- Execute the extract / transform from the Staging database source
	    --
         BEGIN TRY
             SELECT COALESCE(NULLIF(CompanyID, ''), '-1') CompanyID,
                    COALESCE(NULLIF(CompanyName, ''), 'Unknown Company') CompanyName,
                    COALESCE(NULLIF(CompanyTaxNumber, ''), 'Unknown Tax Number') CompanyTaxNumber,
                    @EDWRunDateTime AS EDWCreatedDate,
                    CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWCreatedBy,
                    @EDWRunDateTime AS EDWModifiedDate,
                    CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWModifiedBy,
                    NULL AS Deleted
             FROM dbo.tfnGL_StagingGenerate_Companies_DimCompany();
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


