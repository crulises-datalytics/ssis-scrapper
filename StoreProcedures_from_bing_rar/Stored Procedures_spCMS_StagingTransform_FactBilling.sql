/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spCMS_StagingTransform_FactBilling'
)
    DROP PROCEDURE dbo.spCMS_StagingTransform_FactBilling;
GO
*/
CREATE PROCEDURE [dbo].[spCMS_StagingTransform_FactBilling]
(@EDWRunDateTime   DATETIME2 = NULL,
 @FiscalYearNumber INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingTransform_FactBilling
    --
    -- Purpose:            Performs the transformation logic with the source database
    --                         for a given Fact or Dimension table, and returns the
    --                         results set to the caller (usually for populating a
    --                         temporary table).
    --
    -- Parameters:         @EDWRunDateTime 
    --                     @FiscalYearNumber - Fiscal Year.  This proc has the option
    --                         of only returning data for a given Fiscal Year, which can 
    --                         be leveraged as part of a batched process (so we don't
    --                         extract and load a huge dataset in one go).
    --
    -- Returns:            Results set containing the transformed data ready for
    --					  consumption by the ETL process for load into BING_EDW
    --
    -- Usage:               INSERT #FactBillingUpsert -- (Temporary table)
    --                      EXEC dbo.spCMS_StagingTransform_FactBilling
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By        Comments
    -- ----         -----------        --------
    --
    -- 11/01/17    sburke              BNG-640.  Refactoring Fact table for Center Master
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
	    -- If we do not get an @FiscalYearNumber input, set @ReturnAll to true, 
	    --     and bring back all data
	    --
         DECLARE @ReturnAll INT;
         IF @FiscalYearNumber IS NULL
             SET @ReturnAll = 1;  
	    --
	    -- Execute the extract / transform from the Staging database source
	    --
         BEGIN TRY
             DECLARE @LastProcessedDate DATETIME;
             SET @LastProcessedDate =
             (
                 SELECT LastProcessedDate
                 FROM EDWETLBatchControl(NOLOCK)
                 WHERE EventName = 'FactBilling'
             );
             IF @LastProcessedDate IS NULL
                 SET @LastProcessedDate = '19000101';   -- If no previous load logged in EDWETLBatchControl, assume we bring in everything


		   --
		   -- Get all Invoices
		   -- 
             WITH Invoice
                  AS (
                  SELECT COALESCE(NULLIF(ReffInvoiceNumber, ''), '-1') AS ReferenceInvoiceID,
                         COALESCE(NULLIF(InvoiceNumber, ''), '-1') AS InvoiceID,
                         COALESCE(NULLIF(InvoiceNumber, ''), '-1') AS BillingID,
                         COALESCE(NULLIF(CONVERT(VARCHAR(8), CAST(InvoiceDate AS DATE), 112), ''), '-1') AS BillingDateKey,
                         COALESCE(NULLIF(bsd.DateKey, ''), -1) AS BillingStartDateKey,
                         COALESCE(NULLIF(bed.DateKey, ''), -1) AS BillingEndDateKey,
                         COALESCE(NULLIF(CONVERT(VARCHAR(8), CAST(PaymentDueDate AS DATE), 112), ''), '-1') AS PaymentDueDateKey,
                         COALESCE(NULLIF(CONVERT(VARCHAR(8), CAST(VoidDate AS DATE), 112), ''), '-1') AS InvoiceVoidDateKey,
                         COALESCE(NULLIF(dorg.OrgKey, ''), -1) AS OrgKey,
                         COALESCE(NULLIF(LocationKey, ''), -1) AS LocationKey,
                         COALESCE(NULLIF(dcmp.CompanyKey, ''), -1) AS CompanyKey,
                         COALESCE(NULLIF(dcctyp.CostCenterTypeKey, ''), -1) AS CostCenterTypeKey,
                         COALESCE(NULLIF(dcc.CostCenterKey, ''), -1) AS CostCenterKey,
                         COALESCE(NULLIF(SponsorKey, ''), -1) AS SponsorKey,
                         COALESCE(NULLIF(StudentKey, ''), -1) AS StudentKey,
                         COALESCE(NULLIF(TuitionAssistanceProviderKey, ''), -1) AS TuitionAssistanceProviderKey,
                         COALESCE(NULLIF(ProgramKey, ''), -1) AS ProgramKey,
                         COALESCE(NULLIF(SessionKey, ''), -1) AS SessionKey,
                         COALESCE(NULLIF(ScheduleTypeKey, ''), -1) AS ScheduleTypeKey,
                         COALESCE(NULLIF(tr.TierKey, ''), -1) AS TierKey,
                         COALESCE(NULLIF(InvoiceTypeKey, ''), -1) AS InvoiceTypeKey,
                         -1 AS CreditMemoTypeKey,
                         COALESCE(NULLIF(FeeTypeKey, ''), -1) AS FeeTypeKey,
                         COALESCE(NULLIF(DiscountTypeKey, ''), -1) AS DiscountTypeKey,
                         CASE
                             WHEN si.idProgram IS NOT NULL
                                  AND si.idDiscount IS NULL
                             THEN COALESCE(NULLIF(si.TotalInvoiceAmount, 0), 0)
                             WHEN si.idFees IS NOT NULL
                             THEN COALESCE(NULLIF(si.TotalInvoiceAmount, 0), 0)
                             WHEN si.idDiscount IS NOT NULL
                             THEN COALESCE(NULLIF(si.InvoiceAmount, 0), 0)
                             ELSE 0
                         END AS BillingAmount,
                         @EDWRunDateTime AS EDWCreatedDate,
                         si.Deleted AS Deleted
                  FROM dbo.finStudentInvoice si(NOLOCK)
                       LEFT OUTER JOIN BING_EDW.dbo.DimDate dd(NOLOCK) ON CAST(si.InvoiceDate AS DATE) = dd.FullDate
                       LEFT OUTER JOIN BING_EDW.dbo.DimDate bsd(NOLOCK) ON CAST(si.BillingStartDate AS DATE) = bsd.FullDate
                       LEFT OUTER JOIN BING_EDW.dbo.DimDate bed(NOLOCK) ON CAST(si.BillingEndDate AS DATE) = bed.FullDate
                       LEFT OUTER JOIN BING_EDW.dbo.DimCostCenter(NOLOCK) dcc ON si.idSite = dcc.CenterCMSID
                                                                                 AND dcc.EDWEndDate IS NULL -- DimCostCenter is SCD2, so get latest version
                       LEFT OUTER JOIN BING_EDW.dbo.DimCostCenterType dcctyp ON dcc.CostCenterTypeID = dcctyp.CostCenterTypeID
                       LEFT OUTER JOIN BING_EDW.dbo.DimOrganization dorg ON dcc.CostCenterNumber = dorg.CostCenterNumber
                       LEFT OUTER JOIN BING_EDW.dbo.DimLocation dc(NOLOCK) ON dorg.DefaultLocationID = dc.LocationID
                                                                              AND dc.EDWEndDate IS NULL
                       LEFT OUTER JOIN BING_EDW.dbo.DimCompany dcmp ON dcc.CompanyID = dcmp.CompanyID
                       LEFT OUTER JOIN BING_EDW.dbo.DimProgram dp(NOLOCK) ON si.idProgram = dp.ProgramID
                       LEFT OUTER JOIN BING_EDW.dbo.DimSponsor dspon(NOLOCK) ON si.idSponsor = dspon.SponsorID
                                                                                AND dspon.EDWEndDate IS NULL
                       LEFT OUTER JOIN BING_EDW.dbo.DimStudent dstu(NOLOCK) ON si.idStudent = dstu.StudentID
                                                                               AND dstu.EDWEndDate IS NULL
                       LEFT OUTER JOIN BING_EDW.dbo.DimTuitionAssistanceProvider dtap(NOLOCK) ON si.idSubsidyAgency = dtap.TuitionAssistanceProviderID
                       LEFT OUTER JOIN BING_EDW.dbo.DimScheduleType dsch(NOLOCK) ON si.idScheduleType = dsch.ScheduleTypeID
                       LEFT OUTER JOIN BING_EDW.dbo.DimSession dse(NOLOCK) ON si.idSessionType = dse.SessionID
                                                                              AND dse.EDWEndDate IS NULL
                       LEFT OUTER JOIN BING_EDW.dbo.DimFeeType dft(NOLOCK) ON si.idFees = dft.FeeTypeID
                       LEFT OUTER JOIN BING_EDW.dbo.DimDiscountType ddt(NOLOCK) ON si.idDiscount = ddt.DiscountTypeID
                       LEFT OUTER JOIN BING_EDW.dbo.DimInvoiceType dit(NOLOCK) ON si.idInvoiceType = dit.InvoiceTypeID
                       LEFT OUTER JOIN vEnrollmentDates ed(NOLOCK) ON Si.idStudent = ed.idStudent
                                                                                   AND si.idSite = ed.idSite
                                                                                   AND si.idProgram = ed.idProgram
                                                                                   AND si.idSessionType = ed.idSessionType
                                                                                   AND CAST(si.BillingStartDate AS DATE) BETWEEN ed.EnrollmentStartDate AND ed.EnrollmentEndDate
                       LEFT OUTER JOIN vTierAssignment vta(NOLOCK) ON ed.idEnrollment = vta.idEnrollment
                                                                                   AND CAST(si.BillingStartDate AS DATE) BETWEEN vta.TierStartDate AND vta.TierEndDate
                       LEFT OUTER JOIN BING_EDW.dbo.DimTier(NOLOCK) tr ON vta.idSiteTier = tr.TierID
                  WHERE si.StgModifiedDate >= @LastProcessedDate),

		   --
		   -- Get all Credit Memos that are applied to Invoices.
		   -- 
                  CreditMemo
                  AS (
                  SELECT COALESCE(NULLIF(si.ReffInvoiceNumber, ''), '-1') AS ReferenceInvoiceID,
                         COALESCE(NULLIF(si.InvoiceNumber, ''), '-1') AS InvoiceReferenceNumber,
                         COALESCE(NULLIF(COALESCE(NULLIF(cm.CreditMemoNumber, ''), si.InvoiceNumber), ''), '-1') AS BillingID,
                         COALESCE(NULLIF(CONVERT(VARCHAR(8), CAST(si.InvoiceDate AS DATE), 112), ''), '-1') AS BillingDateKey,
                         COALESCE(NULLIF(bsd.DateKey, ''), -1) AS BillingStartDateKey,
                         COALESCE(NULLIF(bed.DateKey, ''), -1) AS BillingEndDateKey,
                         COALESCE(NULLIF(CONVERT(VARCHAR(8), CAST(si.PaymentDueDate AS DATE), 112), ''), '-1') AS PaymentDueDateKey,
                         COALESCE(NULLIF(CONVERT(VARCHAR(8), CAST(si.VoidDate AS DATE), 112), ''), '-1') AS InvoiceVoidDateKey,
                         COALESCE(NULLIF(dorg.OrgKey, ''), -1) AS OrgKey,
                         COALESCE(NULLIF(LocationKey, ''), -1) AS LocationKey,
                         COALESCE(NULLIF(dcmp.CompanyKey, ''), -1) AS CompanyKey,
                         COALESCE(NULLIF(dcctyp.CostCenterTypeKey, ''), -1) AS CostCenterTypeKey,
                         COALESCE(NULLIF(dcc.CostCenterKey, ''), -1) AS CostCenterKey,
                         COALESCE(NULLIF(dspon.SponsorKey, ''), -1) AS SponsorKey,
                         COALESCE(NULLIF(dstu.StudentKey, ''), -1) AS StudentKey,
                         COALESCE(NULLIF(dtap.TuitionAssistanceProviderKey, ''), -1) AS TuitionAssistanceProviderKey,
                         COALESCE(NULLIF(dp.ProgramKey, ''), -1) AS ProgramKey,
                         COALESCE(NULLIF(dse.SessionKey, ''), -1) AS SessionKey,
                         COALESCE(NULLIF(dsch.ScheduleTypeKey, ''), -1) AS ScheduleTypeKey,
                         COALESCE(NULLIF(tr.TierKey, ''), -1) AS TierKey,
                         CASE
                             WHEN cm.idCreditMemo IS NULL
                             THEN COALESCE(NULLIF(dit.InvoiceTypeKey, ''), -1)
                             ELSE-1
                         END AS InvoiceTypeKey,
                         COALESCE(NULLIF(dcmt.CreditMemoTypeKey, ''), -1) AS CreditMemoTypeKey,
                         -1 AS FeeTypeKey,
                         -1 AS DiscountTypeKey,
                         CASE
                             WHEN cm.idAccountAdjustmentQueue IS NULL
                             THEN COALESCE(cm.TotalCreditMemoAmount, 0)
                             ELSE COALESCE(cm.Amount, 0)
                         END AS BillingMemoAmount,
                         @EDWRunDateTime AS EDWCreatedDate,
                         cm.Deleted AS Deleted
                  FROM dbo.finStudentInvoice si(NOLOCK)
                       INNER JOIN dbo.finCreditMemo cm(NOLOCK) ON si.idStudentInvoice = cm.idStudentInvoice
                       LEFT OUTER JOIN BING_EDW.dbo.DimDate dd(NOLOCK) ON CAST(si.InvoiceDate AS DATE) = dd.FullDate
                       LEFT OUTER JOIN BING_EDW.dbo.DimDate bsd(NOLOCK) ON CAST(si.BillingStartDate AS DATE) = bsd.FullDate
                       LEFT OUTER JOIN BING_EDW.dbo.DimDate bed(NOLOCK) ON CAST(si.BillingEndDate AS DATE) = bed.FullDate
                       LEFT OUTER JOIN BING_EDW.dbo.DimCostCenter(NOLOCK) dcc ON si.idSite = dcc.CenterCMSID
                                                                                 AND dcc.EDWEndDate IS NULL -- DimCostCenter is SCD2, so get latest version
                       LEFT OUTER JOIN BING_EDW.dbo.DimCostCenterType dcctyp ON dcc.CostCenterTypeID = dcctyp.CostCenterTypeID
                       LEFT OUTER JOIN BING_EDW.dbo.DimOrganization dorg ON dcc.CostCenterNumber = dorg.CostCenterNumber
                       LEFT OUTER JOIN BING_EDW.dbo.DimLocation dc(NOLOCK) ON dorg.DefaultLocationID = dc.LocationID
                                                                              AND dc.EDWEndDate IS NULL
                       LEFT OUTER JOIN BING_EDW.dbo.DimCompany dcmp ON dcc.CompanyID = dcmp.CompanyID
                       LEFT OUTER JOIN BING_EDW.dbo.DimProgram dp(NOLOCK) ON si.idProgram = dp.ProgramID
                       LEFT OUTER JOIN BING_EDW.dbo.DimSponsor dspon(NOLOCK) ON si.idSponsor = dspon.SponsorID
                                                                                AND dspon.EDWEndDate IS NULL
                       LEFT OUTER JOIN BING_EDW.dbo.DimStudent dstu(NOLOCK) ON si.idStudent = dstu.StudentID
                                                                               AND dstu.EDWEndDate IS NULL
                       LEFT OUTER JOIN BING_EDW.dbo.DimTuitionAssistanceProvider dtap(NOLOCK) ON si.idSubsidyAgency = dtap.TuitionAssistanceProviderID
                       LEFT OUTER JOIN BING_EDW.dbo.DimScheduleType dsch(NOLOCK) ON si.idScheduleType = dsch.ScheduleTypeID
                       LEFT OUTER JOIN BING_EDW.dbo.DimSession dse(NOLOCK) ON si.idSessionType = dse.SessionID
                                                                              AND dse.EDWEndDate IS NULL
                       LEFT OUTER JOIN BING_EDW.dbo.DimCreditMemoType dcmt(NOLOCK) ON cm.idCreditMemoType = dcmt.CreditMemoTypeID
                       LEFT OUTER JOIN BING_EDW.dbo.DimInvoiceType dit(NOLOCK) ON si.idInvoiceType = dit.InvoiceTypeID
                       LEFT OUTER JOIN vEnrollmentDates ed(NOLOCK) ON Si.idStudent = ed.idStudent
                                                                                   AND si.idSite = ed.idSite
                                                                                   AND si.idProgram = ed.idProgram
                                                                                   AND si.idSessionType = ed.idSessionType
                                                                                   AND CAST(si.BillingStartDate AS DATE) BETWEEN ed.EnrollmentStartDate AND ed.EnrollmentEndDate
                       LEFT OUTER JOIN vTierAssignment vta(NOLOCK) ON ed.idEnrollment = vta.idEnrollment
                                                                                   AND CAST(si.BillingStartDate AS DATE) BETWEEN vta.TierStartDate AND vta.TierEndDate
                       LEFT OUTER JOIN BING_EDW.dbo.DimTier(NOLOCK) tr ON vta.idSiteTier = tr.TierID
                  WHERE si.StgModifiedDate >= @LastProcessedDate
                        AND cm.StgModifiedDate >= @LastProcessedDate)
                  -- 
			   -- Final select
			   --

                  SELECT src_inv.*
                  FROM Invoice src_inv
                       LEFT JOIN BING_EDW.dbo.DimDate dm_dt ON src_inv.BillingDateKey = dm_dt.DateKey
                  -- Check @ReturnAll, and if 1 we return all data.  If not, we return for the given Fiscal Year
                  WHERE(@ReturnAll = 1) -- Important we evaluate @ReturnAll first
                       OR dm_dt.FiscalYearNumber = @FiscalYearNumber
                  UNION ALL
                  SELECT src_memo.*
                  FROM CreditMemo src_memo
                       LEFT JOIN BING_EDW.dbo.DimDate dm_dt ON src_memo.BillingDateKey = dm_dt.DateKey
                  -- Check @ReturnAll, and if 1 we return all data.  If not, we return for the given Fiscal Year
                  WHERE(@ReturnAll = 1) -- Important we evaluate @ReturnAll first
                       OR dm_dt.FiscalYearNumber = @FiscalYearNumber;
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


