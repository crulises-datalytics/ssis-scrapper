/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spSalesforce_StagingTransform_FactLeadEvent'
)
    DROP PROCEDURE dbo.spSalesforce_StagingTransform_FactLeadEvent;
GO
*/
CREATE PROCEDURE [dbo].[spSalesforce_StagingTransform_FactLeadEvent] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spSalesforce_StagingTransform_FactLeadEvent
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
    -- Usage:              INSERT #FactLeadEventUpsert -- (Temporary table)
    --                     EXEC dbo.spSalesforce_StagingTransform_FactLeadEvent @EDWRunDateTime
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By        Comments
    -- ----         -----------        --------
    --
    -- 01/13/18     anmorales          BNG-253 - EDW FactLeadEvent: Created procedure
    -- 10/04/18     valimineti	       BNG-3803 - Adding 2 new columns; LeadCreatedByName and LeadCreatedById 
	-- 12/11/18     hhebbalu		   BNG-4503-Fix FactLeadEvent table logic in the BING EDW 
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
                 SELECT distinct evnt.LeadEventTypeKey,
                        evnt.LeadEventID AS LeadEventID,
                        COALESCE(LEAD.LeadKey, -1) AS LeadKey,
                        COALESCE(org.OrgKey, -1) AS OrgKey,
                        COALESCE(loc.LocationKey, -1) AS LocationKey,
                        COALESCE(com.CompanyKey, -1) AS CompanyKey,
                        COALESCE(cctype.CostCenterTypeKey, -1) AS CostCenterTypeKey,
                        COALESCE(cc.CostCenterKey, -1) AS CostCenterKey,
                        COALESCE(wcamp.WebCampaignKey, -1) AS WebCampaignKey,
                        COALESCE(dt.DateKey, -1) AS DateKey,
                        COALESCE(evnt.LeadEventDate, '1900-01-01') AS EventDate,
						COALESCE(evnt.LeadCreatedbyName,'Unknown') AS LeadCreatedbyName,
						COALESCE(evnt.LeadCreatedbyId,'-1') AS LeadCreatedbyId,
                        @EDWRunDateTime AS EDWCreatedDate
                 FROM Salesforce_Staging.dbo.vLeadEvent AS evnt
                      LEFT JOIN Salesforce_Staging.dbo.vLeadPipeline AS leadpipe ON evnt.LeadID = leadpipe.LeadID
                      LEFT JOIN BING_EDW.dbo.DimLead AS lead ON evnt.LeadID = lead.LeadID		--BNG-4503-Fix FactLeadEvent table logic in the BING EDW/ Before it was joined on leadpipe.LeadId
                      LEFT JOIN BING_EDW.dbo.DimWebCampaign AS wcamp ON leadpipe.WebCampaignID = wcamp.WebCampaignID
                      LEFT JOIN BING_EDW.dbo.DimDate AS dt ON CAST(evnt.LeadEventDate AS DATE) = dt.FullDate		--BNG-4503-Fix FactLeadEvent table logic in the BING EDW/ Before it was joined on leadpipe.InquiryDate
                      LEFT JOIN BING_EDW.dbo.DimCostCenter AS cc ON evnt.LocationNumber = cc.CostCenterNumber	--BNG-4503-Fix FactLeadEvent table logic in the BING EDW/ Before it was joined on leadpipe.LocationNumber
                                                                    AND cc.EDWEndDate IS NULL -- DimCostCenter is SCD2, so get the latest version
                      LEFT JOIN BING_EDW.dbo.DimCompany AS com ON cc.CompanyID = com.CompanyID
                      LEFT JOIN BING_EDW.dbo.DimCostCenterType AS cctype ON cc.CostCenterTypeID = cctype.CostCenterTypeID
                      LEFT JOIN BING_EDW.dbo.DimOrganization AS org ON cc.CostCenterNumber = org.CostCenterNumber
                                                                       AND org.EDWEndDate IS NULL -- DimOrganization is SCD2, so get the latest version
                      LEFT JOIN BING_EDW.dbo.DimLocation AS loc ON org.DefaultLocationID = loc.LocationID
                                                                   AND loc.EDWEndDate IS NULL; -- DimLocation is SCD2, so get the latest version
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