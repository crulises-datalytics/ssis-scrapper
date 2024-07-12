CREATE PROCEDURE dbo.spCSS_StagingTransform_DimTier @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCSS_StagingTransform_DimTier
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
    --                     EXEC dbo.spCSS_StagingTransform_DimTier 
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
    --  2/16/18     sburke          BNG-1248 - Convert from SSIS DFT to stored proc
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
             WITH CTE_DistinctTier
                  AS (
                  SELECT DISTINCT
                         tier_level
                  FROM CSS_Staging.dbo.Csxstudr
                  WHERE tier_level IS NOT NULL
                        AND tier_level <> '')
                  SELECT-2 AS TierID,
                        'T'+tier_level+'CSS' AS TierName,
                        'Unknown Tier Friendly Name' AS TierFriendlyName,
                        'Tier '+tier_level AS TierAssignment,
                        'Unknown Billing Frequency' AS TierBillingFrequency,
                        'Unknown Tier Label' AS TierLabel,
                        'Unknown Show To Sponsor' AS TierShowToSponsor,
                        COALESCE(tier_level, -1) AS CSSTierName,
                        'CSS' AS SourceSystem,
                        @EDWRunDateTime AS EDWCreatedDate,
                        CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWCreatedBy,
                        @EDWRunDateTime AS EDWModifiedDate,
                        CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWModifiedBy
                  FROM CTE_DistinctTier;
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