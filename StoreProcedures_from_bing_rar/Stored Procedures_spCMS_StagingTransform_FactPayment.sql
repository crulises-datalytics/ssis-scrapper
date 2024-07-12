/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spCMS_StagingTransform_FactPayment'
)
    DROP PROCEDURE dbo.spCMS_StagingTransform_FactPayment;
GO
*/
CREATE PROCEDURE [dbo].[spCMS_StagingTransform_FactPayment]
(@EDWRunDateTime   DATETIME2 = NULL,
 @FiscalYearNumber INT
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingTransform_FactPayment
    --
    -- Purpose:            Performs the transformation logic with the source database
    --                         for a given Fact or Dimension table, and returns the
    --                         results set to the caller (usually for populating a
    --                         temporary table).
    --
    -- Parameters:         @EDWRunDateTime
    --                     @FiscalYearNumber - Stored proc runs for just a single FiscalYear 
    --                          (so we run in batches for multiple years)
    --
    -- Returns:            Results set containing the transformed data ready for
    --                         consumption by the ETL process for load into BING_EDW
    --
    -- Usage:              INSERT #FactPaymentUpsert -- (Temporary table)
    --                     EXEC dbo.spCMS_StagingTransform_FactPayment @EDWRunDateTime
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date          Modified By        Comments
    -- ----          -----------        --------
    --
    -- 10/03/17      sburke             Initial version of proc, converted from SSIS logic
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
                 FROM CMS_Staging..EDWETLBatchControl(NOLOCK)
                 WHERE EventName = 'FactPayment'
             );
             IF @LastProcessedDate IS NULL
                 SET @LastProcessedDate = '19000101';  -- If no previous load logged in EDWETLBatchControl, assume we bring in everything

             SELECT std_pmt.PaymentNumber AS PaymentID,
                    COALESCE(dm_org.OrgKey, -1) AS OrgKey,
                    COALESCE(LocationKey, -1) AS LocationKey,
                    COALESCE(dm_cmp.CompanyKey, -1) AS CompanyKey,
                    COALESCE(dm_cctyp.CostCenterTypeKey, -1) AS CostCenterTypeKey,
                    COALESCE(dm_cctr.CostCenterKey, -1) AS CostCenterKey,
                    COALESCE(StudentKey, -1) AS StudentKey,
                    COALESCE(SponsorKey, -1) AS SponsorKey,
                    COALESCE(TuitionAssistanceProviderKey, -1) AS TuitionAssistanceProviderKey,
                    COALESCE(PaymentTypeKey, -1) AS PaymentTypeKey,
                    COALESCE(dm_dt_pmt.DateKey, -1) AS PaymentDateKey,
                    COALESCE(dm_dt_vd.DateKey, -1) AS PaymentVoidDateKey,
                    COALESCE(std_pmt.PaidAmount, 0) AS PaymentAmount,
                    COALESCE(ovr_pmt.BalanceAmount, 0) AS PaymentUnappliedAmount,
                    @EDWRunDateTime AS EDWCreatedDate,
                    @EDWRunDateTime AS EDWModifiedDate,
                    NULL AS Deleted
             FROM dbo.finStudentPayment std_pmt
                  FULL OUTER JOIN dbo.finOverPayment ovr_pmt ON std_pmt.idStudentPayment = ovr_pmt.idStudentPayment
                                                                AND ovr_pmt.idOverPayment NOT IN(61350, 61252) -- SB 11/02/17 - Why are these excluded?
                  LEFT JOIN BING_EDW.dbo.DimCostCenter dm_cctr ON std_pmt.idSite = dm_cctr.CenterCMSID
                                                                  AND dm_cctr.EDWEndDate IS NULL -- DimCostCenter is SCD2, so get the latest version
                  LEFT JOIN BING_EDW.dbo.DimCostCenterType dm_cctyp ON dm_cctr.CostCenterTypeID = dm_cctyp.CostCenterTypeID
                  LEFT JOIN BING_EDW.dbo.DimOrganization dm_org ON dm_cctr.CostCenterNumber = dm_org.CostCenterNumber
                                                                   AND dm_org.EDWEndDate IS NULL -- DimOrganization is SCD2, so get the latest version
                  LEFT JOIN BING_EDW.dbo.DimCompany dm_cmp ON dm_cctr.CompanyID = dm_cmp.CompanyID
                  LEFT JOIN BING_EDW.dbo.DimLocation dm_loc ON dm_org.DefaultLocationID = dm_loc.LocationID
                                                               AND dm_loc.EDWEndDate IS NULL -- DimLocation is SCD2, so get the latest version
                  LEFT JOIN BING_EDW.dbo.DimStudent dm_std ON std_pmt.idStudent = dm_std.StudentID
                                                              AND dm_std.SourceSystem = 'CMS'
															  AND dm_std.EDWEndDate IS NULL -- DimStudent is SCD2, so get the latest version
                  LEFT JOIN BING_EDW.dbo.DimSponsor dm_spr ON std_pmt.idSponsor = dm_spr.SponsorID
															  AND dm_spr.SourceSystem = 'CMS'
                                                              AND dm_spr.EDWEndDate IS NULL -- DimSponsor is SCD2, so get the latest version
                  LEFT JOIN BING_EDW.dbo.DimTuitionAssistanceProvider dm_tap ON std_pmt.idSubsidyAgency = dm_tap.TuitionAssistanceProviderID
                  LEFT JOIN BING_EDW.dbo.DimPaymentType dm_pmtyp ON std_pmt.idPaymentType = dm_pmtyp.PaymentTypeID
                  LEFT JOIN BING_EDW.dbo.DimDate dm_dt_pmt ON CAST(std_pmt.PaymentDate AS DATE) = dm_dt_pmt.FullDate
                  LEFT JOIN BING_EDW.dbo.DimDate dm_dt_vd ON CAST(std_pmt.VoidDate AS DATE) = dm_dt_vd.FullDate
             WHERE(std_pmt.StgModifiedDate >= @LastProcessedDate
                   OR ovr_pmt.StgModifiedDate >= @LastProcessedDate)
                  AND std_pmt.PaymentNumber IS NOT NULL
                  AND YEAR(std_pmt.PaymentDate) = @FiscalYearNumber;
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


