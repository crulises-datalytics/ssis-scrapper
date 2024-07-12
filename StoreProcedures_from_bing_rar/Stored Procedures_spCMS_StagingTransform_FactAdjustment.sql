/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spCMS_StagingTransform_FactAdjustment'
)
    DROP PROCEDURE dbo.spCMS_StagingTransform_FactAdjustment;
GO
*/
CREATE PROCEDURE [dbo].[spCMS_StagingTransform_FactAdjustment] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingTransform_FactAdjustment
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
    -- Usage:              INSERT #FactAdjustmentUpsert -- (Temporary table)
    --                     EXEC dbo.spCMS_StagingTransform_FactAdjustment @EDWRunDateTime
    -- 
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date        Modified By         Comments
    -- ----        -----------         --------
    --
    -- 11/01/17    sburke              BNG-640.  Refactoring FactAdjustment post-Center Master changes
    --  5/25/18    sburke              BNG-1759 - Add SCD2 into join for DimOrganization
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
             DECLARE @LastProcessedDate DATETIME;
             SET @LastProcessedDate =
             (
                 SELECT LastProcessedDate
                 FROM EDWETLBatchControl(NOLOCK)
                 WHERE EventName = 'FactAdjustment'
             );
             IF @LastProcessedDate IS NULL
                 SET @LastProcessedDate = '19000101';   -- If no previous load logged in EDWETLBatchControl, assume we bring in everything

             SELECT AccountAdjustmentNumber AS AdjustmentID,
                    COALESCE(dm_org.OrgKey, -1) AS OrgKey,
                    COALESCE(dm_loc.LocationKey, -1) AS LocationKey,
                    COALESCE(dm_cmp.CompanyKey, -1) AS CompanyKey,
                    COALESCE(dm_cctyp.CostCenterTypeKey, -1) AS CostCenterTypeKey,
                    COALESCE(dm_cctr.CostCenterKey, -1) AS CostCenterKey,
                    COALESCE(SponsorKey, -1) AS SponsorKey,
                    COALESCE(TuitionAssistanceProviderKey, -1) AS TuitionAssistanceProviderKey,
                    COALESCE(AdjustmentReasonKey, -1) AS AdjustmentReasonKey,
                    COALESCE(CreditMemoTypeKey, -1) AS CreditMemoTypeKey,
                    COALESCE(dd.DateKey, -1) AS AdjustmentDateKey,
                    COALESCE(AdjustmentAmount, 0) AS AdjustmentAmount,
                    COALESCE(BalanceAmount, 0) AS AdjustmentUnappliedAmount,
                    @EDWRunDateTime AS EDWCreatedDate,
                    @EDWRunDateTime AS EDWModifiedDate,
                    f.Deleted
             FROM dbo.finAccountAdjustmentQueue f
                  LEFT JOIN BING_EDW.dbo.DimCostCenter dm_cctr ON f.idSite = dm_cctr.CenterCMSID
                                                                  AND dm_cctr.EDWEndDate IS NULL -- DimCostCenter is SDC2, so get latest version
                  LEFT JOIN BING_EDW.dbo.DimCostCenterType dm_cctyp ON dm_cctr.CostCenterTypeID = dm_cctyp.CostCenterTypeID
                  LEFT JOIN BING_EDW.dbo.DimOrganization dm_org ON dm_cctr.CostCenterNumber = dm_org.CostCenterNumber
                                                                   AND dm_org.EDWEndDate IS NULL -- DimOrganization is SDC2, so get latest version
                  LEFT JOIN BING_EDW.dbo.DimLocation dm_loc ON dm_org.DefaultLocationID = dm_loc.LocationID
                                                               AND dm_loc.EDWEndDate IS NULL -- DimLocation is SDC2, so get latest version
                  LEFT JOIN BING_EDW.dbo.DimCompany dm_cmp ON dm_cctr.CompanyID = dm_cmp.CompanyID
                  LEFT JOIN BING_EDW.dbo.DimSponsor dsp ON f.idSponsor = dsp.SponsorID
                                                           AND dsp.EDWEndDate IS NULL
														   AND dsp.SourceSystem = 'CMS'
                  LEFT JOIN BING_EDW.dbo.DimTuitionAssistanceProvider tap ON f.idSubsidyAgency = tap.TuitionAssistanceProviderID
                  LEFT JOIN BING_EDW.dbo.DimAdjustmentReason dar ON dar.AdjustmentReasonID = f.idAccountAdjustmentReason
                  LEFT JOIN BING_EDW.dbo.DimCreditMemoType dcm ON dcm.CreditMemoTypeID = f.idCreditMemoType
                  LEFT JOIN BING_EDW.dbo.DimDate dd ON CAST(f.CreatedDate AS DATE) = dd.FullDate
             WHERE(f.StgModifiedDate >= @LastProcessedDate);
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

