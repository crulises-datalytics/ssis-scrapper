CREATE PROCEDURE [dbo].[spSalesForce_StagingTransform_FactLeadPipeline](@EDWRunDateTime DATETIME2 = NULL)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spSalesForce_StagingTransform_FactLeadPipeline
    --
    -- Purpose:            Performs the transformation logic with the source database
    --                         for a given Fact or Dimension table, and returns the
    --                         results set to the caller (usually for populating a
    --                         temporary table).
    --
    -- Parameters:         @EDWRunDateTime
    --
    -- Returns:            Results set containing the transformed data ready for
    --                         consumption by the ETL process for load into BING_EDW
    --
    -- Usage:              INSERT #FactLeadPipelineUpsert -- (Temporary table)
    --                     EXEC dbo.spSalesForce_StagingTransform_FactLeadPipeline @EDWRunDateTime
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By        Comments
    -- ----         -----------        --------
    --
    --  2/13/18     sburke             BNG-252 - Initial version of proc
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
	    -- Get the last processed date, and if there is none, set to beginning of time (01-01-1900)
	    --
         DECLARE @LastProcessedDate DATETIMEOFFSET;
         SELECT @LastProcessedDate = LastProcessedDate
         FROM dbo.EDWETLBatchControl
         WHERE EventName = 'FactLeadPipeline';
         --
         IF @LastProcessedDate IS NULL
             SET @LastProcessedDate = '1900-01-01 12:00:00.0000 +0:0';

	    --
	    -- Execute the extract / transform from the Staging database source
	    --
         BEGIN TRY
             SELECT dm_ld.LeadKey AS LeadKey, -- We have to be able to link to Lead in the DimLead table (so inner join)
                    COALESCE(dm_org.OrgKey, -1) AS OrgKey,
                    COALESCE(dm_loc.LocationKey, -1) AS LocationKey,
                    COALESCE(dm_cmp.CompanyKey, -1) AS CompanyKey,
                    COALESCE(dm_cctyp.CostCenterTypeKey, -1) AS CostCenterTypeKey,
                    COALESCE(dm_cctr.CostCenterKey, -1) AS CostCenterKey,
                    COALESCE(dm_wb_cmp.WebCampaignKey, -1) AS WebCampaignKey,
                    COALESCE(dm_dt.DateKey, -1) AS InquiryDateKey,
                    fct_ld_pln.InquiryDate AS InquiryDate,
                    fct_ld_pln.FirstInteractionDate AS FirstInteractionDate,
                    fct_ld_pln.FirstTourCreatedDate AS FirstTourCreatedDate,
                    fct_ld_pln.FirstTourScheduledDate AS FirstTourScheduledDate,
                    fct_ld_pln.FirstTourCompletedDate AS FirstTourCompletedDate,
                    fct_ld_pln.ConversionDate AS ConversionDate,
                    NULL AS FirstEnrollmentDate, -- For now we hard-code this as NULL.  It will eventually hold the first date the Lead actually enrolled a Student at the Center
                    fct_ld_pln.LeadPipelineLastModifiedDate AS LeadPipelineLastModifiedDate,
                    fct_ld_pln.InteractionCount AS InteractionCount,
                    fct_ld_pln.TourScheduledCount AS TourScheduledCount,
                    fct_ld_pln.TourCompletedCount AS TourCompletedCount,
                    @EDWRunDateTime AS EDWCreatedDate,
                    @EDWRunDateTime AS EDWModifiedDate
             FROM dbo.vLeadPipeline fct_ld_pln
                  INNER JOIN BING_EDW.dbo.DimLead dm_ld ON fct_ld_pln.LeadID = dm_ld.LeadID
                  LEFT JOIN BING_EDW.dbo.DimDate dm_dt ON CAST(fct_ld_pln.InquiryDate AS DATE) = dm_dt.FullDate
                  LEFT JOIN BING_EDW.dbo.DimCostCenter dm_cctr ON dm_cctr.CostCenterNumber = fct_ld_pln.LocationNumber
                                                                  AND dm_cctr.EDWEndDate IS NULL -- DimCostCenter is SCD2, so get the latest version
                  LEFT JOIN BING_EDW.dbo.DimCostCenterType dm_cctyp ON dm_cctyp.CostCenterTypeID = dm_cctr.CostCenterTypeID
                  LEFT JOIN BING_EDW.dbo.DimCompany dm_cmp ON dm_cmp.CompanyID = dm_cctr.CompanyID
                  LEFT JOIN BING_EDW.dbo.DimOrganization dm_org ON dm_cctr.CostCenterNumber = dm_org.CostCenterNumber
                                                                   AND dm_org.EDWEndDate IS NULL -- DimOrganization is SCD2, so get the latest version
                  LEFT JOIN BING_EDW.dbo.DimLocation dm_loc ON dm_org.DefaultLocationID = dm_loc.LocationID
                                                               AND dm_loc.EDWEndDate IS NULL -- DimLocation is SCD2, so get the latest version
                  LEFT JOIN BING_EDW.dbo.DimWebCampaign dm_wb_cmp ON fct_ld_pln.WebCampaignID = dm_wb_cmp.WebCampaignID
             WHERE fct_ld_pln.LeadPipelineLastModifiedDate >= @LastProcessedDate;
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