


CREATE PROCEDURE [dbo].[spGL_StagingTransfrom_DimCompanyRollup] @EDWRunDateTime DATETIME2
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spGL_StagingTransfrom_DimCompanyRollup
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
    -- Usage:              INSERT INTO #DimCompanyRollupUpsert -- (Temporary table)
    --                     EXEC dbo.spGL_StagingTransfrom_DimCompanyRollup
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date        Modified By      Comments
    -- ----        -----------      --------
    --
    -- 12/06/17    ADevabhakthuni          BNG-909 - Refactor DimCOmpanyRollup staging to EDW load. 
	--                                  
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
			COALESCE(NULLIF(CompanyRollupID,''),'-1') CompanyRollupID,
			COALESCE(NULLIF(CompanyRollupName,''),'Unknown Company') CompanyRollupName,
			GETDATE() AS EDWCreatedDate,
			CAST (SYSTEM_USER AS VARCHAR(50)) AS EDWCreatedBy,
			GETDATE() AS EDWModifiedDate,
			CAST (SYSTEM_USER AS VARCHAR(50)) AS EDWModifiedBy,
			NULL as Deleted
			from dbo.tfnCompanyRollup()
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