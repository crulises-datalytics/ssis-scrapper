



/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spGL_StagingTransform_BridgeCompanyRollup'
)
    DROP PROCEDURE dbo.spGL_StagingTransform_BridgeCompanyRollup;
GO
*/
CREATE PROCEDURE [dbo].[spGL_StagingTransform_BridgeCompanyRollup] @EDWRunDateTime DATETIME2
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spGL_StagingTransform_BridgeCompanyRollup
    --
    -- Purpose:            Performs the transformation logic with the source database
    --                         for a given  table, and returns the
    --                         results set to the caller (usually for populating a
    --                         temporary table).
    --
    -- Parameters:         @EDWRunDateTime - Not required, and defaults to GETDATE()
    --
    -- Returns:            Results set containing the transformed data ready for
    --                         consumption by the ETL process for load into BING_EDW
    --
    -- Usage:              INSERT INTO #BridgeCompanyUpsert -- (Temporary table)
    --                     EXEC dbo.spGL_StagingTransform_BridgeCompanyRollup
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date        Modified By      Comments
    -- ----        -----------      --------
    --
    -- 12/04/17    ADevabhakthuni           BNG-910 - Refactor BridgeCompanyRollup StagingToEDW load (Initial version of proc, 
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
           		    SELECT 
				dcr.CompanyRollupKey AS CompanyRollupKey,
				dc.CompanyKey AS CompanyKey,
				@EDWRunDateTime AS EDWCreatedDate,
				CAST (SYSTEM_USER AS VARCHAR(50)) AS EDWCreatedBy,
				@EDWRunDateTime AS EDWModifiedDate,
				CAST (SYSTEM_USER AS VARCHAR(50)) AS EDWModifiedBy,
				Null as Deleted 
				from dbo.tfnCompanyRollupBridge() f
			inner join BING_EDW.dbo.DimCompanyRollup dcr on f.CompanyRollupID = dcr.CompanyRollupID
				inner join BING_EDW.dbo.DimCompany dc on f.CompanyID = dc.CompanyID
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