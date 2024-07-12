
/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spBING_EDW_Gather_FactDimensionCoverage'
)
    DROP PROCEDURE dbo.spBING_EDW_Gather_FactDimensionCoverage;
GO
--*/

CREATE PROCEDURE [dbo].[spBING_EDW_Gather_FactDimensionCoverage]
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spBING_EDW_Gather_FactDimensionCoverage
    --
    -- Parameters:         @EDWRunDateTime
    --                     @DebugMode - Used just for development & debug purposes,
    --                         outputting helpful info back to the caller.  Not
    --                         required for Production, and does not affect any
    --                         core logic.
    --
    --Usage:              EXEC dbo.spBING_EDW_Gather_FactDimensionCoverage @DebugMode = 1
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date          Modified By         Comments
    -- ----          -----------         --------
    --
    -- 11/26/18      sburke              Initial version
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;
         SET ANSI_WARNINGS OFF;

         --
         -- Housekeeping Variables
         --
         DECLARE @ProcName NVARCHAR(100)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @TaskName VARCHAR(100)= 'spBING_EDW_Gather_FactDimensionCoverage';

         --
         -- ETL status Variables
         --
         DECLARE @RowCount INT;
         DECLARE @Error INT;

         --
         -- ETL variables specific to this load
         --
         DECLARE @AuditId BIGINT;
         DECLARE @RunDateKey INT;
         DECLARE @SourceCount INT= 0;
         DECLARE @InsertCount INT= 0;
         DECLARE @UpdateCount INT= 0;
         DECLARE @DeleteCount INT= 0;

         --
         -- If we do not get an @EDWRunDateTime input, set to current date
         --
         IF @EDWRunDateTime IS NULL
             SET @EDWRunDateTime = GETDATE();
         --
         -- We use a DateKey (from DimDate) in our load to EDWFactDimensionCoverage
         --

         SELECT @RunDateKey = DateKey
         FROM dbo.DimDate
         WHERE FullDate = CAST(@EDWRunDateTime AS DATE);

         --
         IF @DebugMode = 1
             SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Starting..';
         PRINT @DebugMsg;
	    
         -- Write to AuditLog that we are starting	  
         EXEC [dbo].[spEDWBeginAuditLog]
              @SourceName = @TaskName,
              @AuditId = @AuditId OUTPUT;
         BEGIN TRY
             -- Only have one load per day, so delete and reload if re-running
             DELETE FROM dbo.EDWFactDimensionCoverage
             WHERE RunDateKey = @RunDateKey;

		   --

             SELECT @DeleteCount = @@ROWCOUNT;
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Deleted '+CONVERT(VARCHAR(10), @DeleteCount)+' records from EDWFactDimensionCoverage';
             PRINT @DebugMsg;

             -- --------------------------------------------------------------------------------
             -- FactARBalanceSnapshot
             -- --------------------------------------------------------------------------------
             INSERT INTO dbo.EDWFactDimensionCoverage
                    SELECT @RunDateKey,
                           dm_dt.FiscalYearNumber,
                           'FactARBalanceSnapshot' AS FactTableName,
                           fct_ar.SourceSystem,
                           -- Total Number of Records for the Fact
                           COUNT(1) AS TotalKeyCount,
                           -- Total Number of 'Unknown' or 'N/A' DateKeys (values of -1 & -2)
                           COUNT(CASE
                                     WHEN fct_ar.AsOfDateKey < 0
                                     THEN fct_ar.AsOfDateKey
                                 END) AS DateKeyCount,
                           -- Total Number of 'Unknown' or 'N/A' AccountSubAccountKeys (values of -1 & -2)
                           NULL AS AccountSubAccountKeyCount,
                           -- AdjustmentReasonKeys
                           NULL AS AdjustmentReasonKeysCount,
                           -- ARAgingTypeKey
                           NULL AS ARAgencyTypeKey,
                           -- ARBalanceTypeKey
                           COUNT(CASE
                                     WHEN fct_ar.ARBalanceTypeKey < 0
                                     THEN fct_ar.ARBalanceTypeKey
                                 END) AS ARBalanceTypeKey,
                           -- ARAgingBucketKey
                           NULL AS ARAgingBucketKey,
                           -- ClassroomKeys
                           NULL AS ClassroomKeyCount,
                           -- CompanyKeys
                           COUNT(CASE
                                     WHEN fct_ar.CompanyKey < 0
                                     THEN CompanyKey
                                 END) AS CompanyKeyCount,
                           -- ComplianceItemKey
                           NULL AS ComplianceItemKey,
                           -- ComplianceRatingKey
                           NULL AS ComplianceRatingKey,
                           -- CostCenterKeys
                           COUNT(CASE
                                     WHEN fct_ar.CostCenterKey < 0
                                     THEN CostCenterKey
                                 END) AS CostCenterKeyCount,
                           -- CostCenterTypeKeys
                           COUNT(CASE
                                     WHEN fct_ar.CostCenterTypeKey < 0
                                     THEN CostCenterTypeKey
                                 END) AS CostCenterTypeKeyCount,
                           -- CreditMemoTypeKey
                           NULL AS CreditMemoTypeKeyCount,
                           -- DataScenarioKey
                           NULL AS DataScenarioKeyCount,
                           -- DiscountTypeKey
                           NULL AS DiscountTypeKeyCount,
                           -- FeeTypeKeys
                           NULL AS FeeTypeKeyCount,
                           -- GLMetricTypeKey
                           NULL AS GLMetricTypeKeyCount,
                           -- HRUserKey
                           NULL AS HRUserKeyCount,
                           -- InvoiceTypeKey
                           NULL AS InvoiceTypeKeyCount,
                           -- LeadKey
                           NULL AS LeadKeyCount,
                           -- LeadEventTypeKeyKey
                           NULL AS LeadEventTypeKeyCount,
                           -- LeadTypeKey
                           NULL AS LeadTypeKeyCount,
                           -- LeaveReasonKey
                           NULL AS LeaveReasonKeyCount,
                           -- LeaveTypeKey
                           NULL AS LeaveTypeKeyCount,
                           -- LifecycleStatusKey
                           NULL AS LifecycleStatusKeyCount,
                           -- LocationKeys
                           COUNT(CASE
                                     WHEN fct_ar.LocationKey < 0
                                     THEN LocationKey
                                 END) AS LocationKeyCount,
                           -- OrgKeys
                           COUNT(CASE
                                     WHEN fct_ar.OrgKey < 0
                                     THEN OrgKey
                                 END) AS OrgKeyCount,
                           -- PayBasisKey
                           NULL AS PayBasisKeyCount,
                           -- PayGradeKey
                           NULL AS PayGradeKeyCount,
                           -- PaymentTypeKey
                           NULL AS PaymentTypeKeyCount,
                           -- PayRateChangeReasonKey
                           NULL AS PayRateChangeReasonKeyCount,
                           -- PayrollTypeKey
                           NULL AS PayrollTypeKeyCount,
                           -- PeopleGroupKey
                           NULL AS PeopleGroupKeyCount,
                           -- PerformanceRatingKey
                           NULL AS PerformanceRatingKeyCount,
                           -- PersonKey
                           NULL AS PersonKeyCount,
                           -- PositionKey
                           NULL AS PositionKeyCount,
                           -- ProgramKeys
                           NULL AS ProgramKeyCount,
                           -- QualificationTypeKey
                           NULL AS QualificationTypeKeyCount,
                           -- ScheduleTypeKey
                           NULL AS ScheduleTypeKeyCount,
                           -- ScheduleWeekKeys
                           NULL AS ScheduleWeekKeyCount,
                           -- SessionKeys
                           NULL AS SessionKeyCount,
                           -- SpecialInfoKey
                           NULL AS SpecialInfoKeyCount,
                           -- SponsorKeys
                           COUNT(CASE
                                     WHEN fct_ar.SponsorKey < 0
                                     THEN SponsorKey
                                 END) AS SponsorKeyCount,
                           -- StudentKeys
                           COUNT(CASE
                                     WHEN fct_ar.StudentKey < 0
                                     THEN StudentKey
                                 END) AS StudentKeyCount,
                           -- TierKeys
                           NULL AS TierKeyCount,
                           -- TransactionCodeKey
                           NULL AS TransactionCodeKeyCount,
                           -- TuitionAssistanceProviderKeys
                           COUNT(CASE
                                     WHEN fct_ar.TuitionAssistanceProviderKey < 0
                                     THEN TuitionAssistanceProviderKey
                                 END) AS TuitionAssistanceProviderKeyCount,
                           -- WebCampaignKey
                           NULL AS WebCampaignKeyCount,
					  -- EDWCreatedDate
                           GETDATE()
                    FROM dbo.FactARBalanceSnapshot fct_ar
                         LEFT JOIN dbo.DimDate dm_dt ON fct_ar.AsOfDateKey = dm_dt.DateKey
                    GROUP BY dm_dt.FiscalYearNumber,
                             fct_ar.SourceSystem
                    ORDER BY dm_dt.FiscalYearNumber,
                             fct_ar.SourceSystem;
             -- Log inserts

             SELECT @RowCount = @@ROWCOUNT;
             SELECT @InsertCount = @InsertCount + @RowCount;
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(VARCHAR(10), @RowCount)+' records for FactARBalanceSnapshot';
             PRINT @DebugMsg;

             -- --------------------------------------------------------------------------------
             -- FactCenterStatSnapshot
             -- --------------------------------------------------------------------------------
             INSERT INTO dbo.EDWFactDimensionCoverage
                    SELECT @RunDateKey,
                           dm_dt.FiscalYearNumber,
                           'FactCenterStatSnapshot' AS FactTableName,
                           fct_snp.SourceSystem,
                           -- Total Number of Records for the Fact
                           COUNT(1) AS TotalKeyCount,
                           -- Total Number of 'Unknown' or 'N/A' DateKeys (values of -1 & -2)
                           COUNT(CASE
                                     WHEN fct_snp.FiscalWeekEndDateKey < 0
                                     THEN fct_snp.FiscalWeekEndDateKey
                                 END) AS DateKeyCount,
                           -- Total Number of 'Unknown' or 'N/A' AccountSubAccountKeys (values of -1 & -2)
                           NULL AS AccountSubAccountKeyCount,
                           -- AdjustmentReasonKeys
                           NULL AS AdjustmentReasonKeysCount,
                           -- ARAgingTypeKey
                           NULL AS ARAgencyTypeKey,
                           -- ARBalanceTypeKey
                           NULL AS ARBalanceTypeKey,
                           -- ARAgingBucketKey
                           NULL AS ARAgingBucketKey,
                           -- ClassroomKeys
                           NULL AS ClassroomKeyCount,
                           -- CompanyKeys
                           COUNT(CASE
                                     WHEN fct_snp.CompanyKey < 0
                                     THEN CompanyKey
                                 END) AS CompanyKeyCount,
                           -- ComplianceItemKey
                           NULL AS ComplianceItemKey,
                           -- ComplianceRatingKey
                           NULL AS ComplianceRatingKey,
                           -- CostCenterKeys
                           COUNT(CASE
                                     WHEN fct_snp.CostCenterKey < 0
                                     THEN CostCenterKey
                                 END) AS CostCenterKeyCount,
                           -- CostCenterTypeKeys
                           COUNT(CASE
                                     WHEN fct_snp.CostCenterTypeKey < 0
                                     THEN CostCenterTypeKey
                                 END) AS CostCenterTypeKeyCount,
                           -- CreditMemoTypeKey
                           NULL AS CreditMemoTypeKeyCount,
                           -- DataScenarioKey
                           COUNT(CASE
                                     WHEN fct_snp.DataScenarioKey < 0
                                     THEN DataScenarioKey
                                 END) AS DataScenarioKeyCount,
                           -- DiscountTypeKey
                           NULL AS DiscountTypeKeyCount,
                           -- FeeTypeKeys
                           NULL AS FeeTypeKeyCount,
                           -- GLMetricTypeKey
                           NULL AS GLMetricTypeKeyCount,
                           -- HRUserKey
                           NULL AS HRUserKeyCount,
                           -- InvoiceTypeKey
                           NULL AS InvoiceTypeKeyCount,
                           -- LeadKey
                           NULL AS LeadKeyCount,
                           -- LeadEventTypeKeyKey
                           NULL AS LeadEventTypeKeyCount,
                           -- LeadTypeKey
                           NULL AS LeadTypeKeyCount,
                           -- LeaveReasonKey
                           NULL AS LeaveReasonKeyCount,
                           -- LeaveTypeKey
                           NULL AS LeaveTypeKeyCount,
                           -- LifecycleStatusKey
                           NULL AS LifecycleStatusKeyCount,
                           -- LocationKeys
                           COUNT(CASE
                                     WHEN fct_snp.LocationKey < 0
                                     THEN LocationKey
                                 END) AS LocationKeyCount,
                           -- OrgKeys
                           COUNT(CASE
                                     WHEN fct_snp.OrgKey < 0
                                     THEN OrgKey
                                 END) AS OrgKeyCount,
                           -- PayBasisKey
                           NULL AS PayBasisKeyCount,
                           -- PayGradeKey
                           NULL AS PayGradeKeyCount,
                           -- PaymentTypeKey
                           NULL AS PaymentTypeKeyCount,
                           -- PayRateChangeReasonKey
                           NULL AS PayRateChangeReasonKeyCount,
                           -- PayrollTypeKey
                           NULL AS PayrollTypeKeyCount,
                           -- PeopleGroupKey
                           NULL AS PeopleGroupKeyCount,
                           -- PerformanceRatingKey
                           NULL AS PerformanceRatingKeyCount,
                           -- PersonKey
                           NULL AS PersonKeyCount,
                           -- PositionKey
                           NULL AS PositionKeyCount,
                           -- ProgramKeys
                           NULL AS ProgramKeyCount,
                           -- QualificationTypeKey
                           NULL AS QualificationTypeKeyCount,
                           -- ScheduleTypeKey
                           NULL AS ScheduleTypeKeyCount,
                           -- ScheduleWeekKeys
                           NULL AS ScheduleWeekKeyCount,
                           -- SessionKeys
                           NULL AS SessionKeyCount,
                           -- SpecialInfoKey
                           NULL AS SpecialInfoKeyCount,
                           -- SponsorKeys
                           NULL AS SponsorKeyCount,
                           -- StudentKeys
                           NULL AS StudentKeyCount,
                           -- TierKeys
                           NULL AS TierKeyCount,
                           -- TransactionCodeKey
                           NULL AS TransactionCodeKeyCount,
                           -- TuitionAssistanceProviderKeys
                           NULL AS TuitionAssistanceProviderKeyCount,
                           -- WebCampaignKey
                           NULL AS WebCampaignKeyCount,
					  -- EDWCreatedDate
                           GETDATE()
                    FROM dbo.FactCenterStatSnapshot fct_snp
                         LEFT JOIN dbo.DimDate dm_dt ON fct_snp.FiscalWeekEndDateKey = dm_dt.DateKey
                    GROUP BY dm_dt.FiscalYearNumber,
                             SourceSystem
                    ORDER BY dm_dt.FiscalYearNumber,
                             SourceSystem;

             -- Log inserts

             SELECT @RowCount = @@ROWCOUNT;
             SELECT @InsertCount = @InsertCount + @RowCount;
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(VARCHAR(10), @RowCount)+' records for FactCenterStatSnapshot';
             PRINT @DebugMsg;

              -- --------------------------------------------------------------------------------
              -- FactFTESnapshot
              -- --------------------------------------------------------------------------------
             INSERT INTO dbo.EDWFactDimensionCoverage
                    SELECT @RunDateKey,
                           dm_dt.FiscalYearNumber,
                           'FactFTESnapshot' AS FactTableName,
                           fct_fte.SourceSystem,
                           -- Total Number of Records for the Fact
                           COUNT(1) AS TotalKeyCount,
                           -- Total Number of 'Unknown' or 'N/A' DateKeys (values of -1 & -2)
                           COUNT(CASE
                                     WHEN fct_fte.DateKey < 0
                                     THEN fct_fte.DateKey
                                 END) AS DateKeyCount,
                           -- Total Number of 'Unknown' or 'N/A' AccountSubAccountKeys (values of -1 & -2)
                           COUNT(CASE
                                     WHEN fct_fte.AccountSubAccountKey < 0
                                     THEN AccountSubAccountKey
                                 END) AS AccountSubAccountKeyCount,
                           -- AdjustmentReasonKeys
                           NULL AS AdjustmentReasonKeysCount,
                           -- ARAgingTypeKey
                           NULL AS ARAgencyTypeKey,
                           -- ARBalanceTypeKey
                           NULL AS ARBalanceTypeKey,
                           -- ARAgingBucketKey
                           NULL AS ARAgingBucketKey,
                           -- ClassroomKeys
                           NULL AS ClassroomKeyCount,
                           -- CompanyKeys
                           COUNT(CASE
                                     WHEN fct_fte.CompanyKey < 0
                                     THEN CompanyKey
                                 END) AS CompanyKeyCount,
                           -- ComplianceItemKey
                           NULL AS ComplianceItemKey,
                           -- ComplianceRatingKey
                           NULL AS ComplianceRatingKey,
                           -- CostCenterKeys
                           COUNT(CASE
                                     WHEN fct_fte.CostCenterKey < 0
                                     THEN CostCenterKey
                                 END) AS CostCenterKeyCount,
                           -- CostCenterTypeKeys
                           COUNT(CASE
                                     WHEN fct_fte.CostCenterTypeKey < 0
                                     THEN CostCenterTypeKey
                                 END) AS CostCenterTypeKeyCount,
                           -- CreditMemoTypeKey
                           NULL AS CreditMemoTypeKeyCount,
                           -- DataScenarioKey
                           NULL AS DataScenarioKeyCount,
                           -- DiscountTypeKey
                           NULL AS DiscountTypeKeyCount,
                           -- FeeTypeKeys
                           COUNT(CASE
                                     WHEN fct_fte.FeeTypeKey < 0
                                     THEN FeeTypeKey
                                 END) AS FeeTypeKeyCount,
                           -- GLMetricTypeKey
                           NULL AS GLMetricTypeKeyCount,
                           -- HRUserKey
                           NULL AS HRUserKeyCount,
                           -- InvoiceTypeKey
                           NULL AS InvoiceTypeKeyCount,
                           -- LeadKey
                           NULL AS LeadKeyCount,
                           -- LeadEventTypeKeyKey
                           NULL AS LeadEventTypeKeyCount,
                           -- LeadTypeKey
                           NULL AS LeadTypeKeyCount,
                           -- LeaveReasonKey
                           NULL AS LeaveReasonKeyCount,
                           -- LeaveTypeKey
                           NULL AS LeaveTypeKeyCount,
                           -- LifecycleStatusKey
                           COUNT(CASE
                                     WHEN fct_fte.LifecycleStatusKey < 0
                                     THEN LifecycleStatusKey
                                 END) AS LifecycleStatusKeyCount,
                           -- LocationKeys
                           COUNT(CASE
                                     WHEN fct_fte.LocationKey < 0
                                     THEN LocationKey
                                 END) AS LocationKeyCount,
                           -- OrgKeys
                           COUNT(CASE
                                     WHEN fct_fte.OrgKey < 0
                                     THEN OrgKey
                                 END) AS OrgKeyCount,
                           -- PayBasisKey
                           NULL AS PayBasisKeyCount,
                           -- PayGradeKey
                           NULL AS PayGradeKeyCount,
                           -- PaymentTypeKey
                           NULL AS PaymentTypeKeyCount,
                           -- PayRateChangeReasonKey
                           NULL AS PayRateChangeReasonKeyCount,
                           -- PayrollTypeKey
                           NULL AS PayrollTypeKeyCount,
                           -- PeopleGroupKey
                           NULL AS PeopleGroupKeyCount,
                           -- PerformanceRatingKey
                           NULL AS PerformanceRatingKeyCount,
                           -- PersonKey
                           NULL AS PersonKeyCount,
                           -- PositionKey
                           NULL AS PositionKeyCount,
                           -- ProgramKeys
                           COUNT(CASE
                                     WHEN fct_fte.ProgramKey < 0
                                     THEN ProgramKey
                                 END) AS ProgramKeyCount,
                           -- QualificationTypeKey
                           NULL AS QualificationTypeKeyCount,
                           -- ScheduleTypeKey
                           NULL AS ScheduleTypeKeyCount,
                           -- ScheduleWeekKeys
                           COUNT(CASE
                                     WHEN fct_fte.ScheduleWeekKey < 0
                                     THEN ScheduleWeekKey
                                 END) AS ScheduleWeekKeyCount,
                           -- SessionKeys
                           COUNT(CASE
                                     WHEN fct_fte.SessionKey < 0
                                     THEN SessionKey
                                 END) AS SessionKeyCount,
                           -- SpecialInfoKey
                           NULL AS SpecialInfoKeyCount,
                           -- SponsorKeys
                           COUNT(CASE
                                     WHEN fct_fte.SponsorKey < 0
                                     THEN SponsorKey
                                 END) AS SponsorKeyCount,
                           -- StudentKeys
                           COUNT(CASE
                                     WHEN fct_fte.StudentKey < 0
                                     THEN StudentKey
                                 END) AS StudentKeyCount,
                           -- TierKeys
                           COUNT(CASE
                                     WHEN fct_fte.TierKey < 0
                                     THEN TierKey
                                 END) AS TierKeyCount,
                           -- TransactionCodeKey
                           COUNT(CASE
                                     WHEN fct_fte.TransactionCodeKey < 0
                                     THEN TransactionCodeKey
                                 END) AS TransactionCodeKeyCount,
                           -- TuitionAssistanceProviderKeys
                           COUNT(CASE
                                     WHEN fct_fte.TuitionAssistanceProviderKey < 0
                                     THEN TuitionAssistanceProviderKey
                                 END) AS TuitionAssistanceProviderKeyCount,
                           -- WebCampaignKey
                           NULL AS WebCampaignKeyCount,
					  -- EDWCreatedDate
                           GETDATE()
                    FROM dbo.FactFTESnapshot fct_fte
                         LEFT JOIN dbo.DimDate dm_dt ON fct_fte.DateKey = dm_dt.DateKey
                    GROUP BY dm_dt.FiscalYearNumber,
                             fct_fte.SourceSystem
                    ORDER BY dm_dt.FiscalYearNumber,
                             fct_fte.SourceSystem;

             -- Log inserts

             SELECT @RowCount = @@ROWCOUNT;
             SELECT @InsertCount = @InsertCount + @RowCount;
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(VARCHAR(10), @RowCount)+' records for FactFTESnapshot';
             PRINT @DebugMsg;
             -- --------------------------------------------------------------------------------
             -- FactGLBalance
             -- --------------------------------------------------------------------------------
             INSERT INTO dbo.EDWFactDimensionCoverage
                    SELECT @RunDateKey,
                           dm_dt.FiscalYearNumber,
                           'FactGLBalance' AS FactTableName,
                           'G/L' AS SourceSystem,
                           -- Total Number of Records for the Fact
                           COUNT(1) AS TotalKeyCount,
                           -- Total Number of 'Unknown' or 'N/A' DateKeys (values of -1 & -2)
                           COUNT(CASE
                                     WHEN fct_bal.DateKey < 0
                                     THEN fct_bal.DateKey
                                 END) AS DateKeyCount,
                           -- Total Number of 'Unknown' or 'N/A' AccountSubAccountKeys (values of -1 & -2)
                           COUNT(CASE
                                     WHEN fct_bal.AccountSubAccountKey < 0
                                     THEN AccountSubAccountKey
                                 END) AS AccountSubAccountKeyCount,
                           -- AdjustmentReasonKeys
                           NULL AS AdjustmentReasonKeysCount,
                           -- ARAgingTypeKey
                           NULL AS ARAgencyTypeKey,
                           -- ARBalanceTypeKey
                           NULL AS ARBalanceTypeKey,
                           -- ARAgingBucketKey
                           NULL AS ARAgingBucketKey,
                           -- ClassroomKeys
                           NULL AS ClassroomKeyCount,
                           -- CompanyKeys
                           COUNT(CASE
                                     WHEN fct_bal.CompanyKey < 0
                                     THEN CompanyKey
                                 END) AS CompanyKeyCount,
                           -- ComplianceItemKey
                           NULL AS ComplianceItemKey,
                           -- ComplianceRatingKey
                           NULL AS ComplianceRatingKey,
                           -- CostCenterKeys
                           COUNT(CASE
                                     WHEN fct_bal.CostCenterKey < 0
                                     THEN CostCenterKey
                                 END) AS CostCenterKeyCount,
                           -- CostCenterTypeKeys
                           COUNT(CASE
                                     WHEN fct_bal.CostCenterTypeKey < 0
                                     THEN CostCenterTypeKey
                                 END) AS CostCenterTypeKeyCount,
                           -- CreditMemoTypeKey
                           NULL AS CreditMemoTypeKeyCount,
                           -- DataScenarioKey
                           COUNT(CASE
                                     WHEN fct_bal.DataScenarioKey < 0
                                     THEN DataScenarioKey
                                 END) AS DataScenarioKeyCount,
                           -- DiscountTypeKey
                           NULL AS DiscountTypeKeyCount,
                           -- FeeTypeKeys
                           NULL AS FeeTypeKeyCount,
                           -- GLMetricTypeKey
                           COUNT(CASE
                                     WHEN fct_bal.GLMetricTypeKey < 0
                                     THEN GLMetricTypeKey
                                 END) AS GLMetricTypeKeyCount,
                           -- HRUserKey
                           NULL AS HRUserKeyCount,
                           -- InvoiceTypeKey
                           NULL AS InvoiceTypeKeyCount,
                           -- LeadKey
                           NULL AS LeadKeyCount,
                           -- LeadEventTypeKeyKey
                           NULL AS LeadEventTypeKeyCount,
                           -- LeadTypeKey
                           NULL AS LeadTypeKeyCount,
                           -- LeaveReasonKey
                           NULL AS LeaveReasonKeyCount,
                           -- LeaveTypeKey
                           NULL AS LeaveTypeKeyCount,
                           -- LifecycleStatusKey
                           NULL AS LifecycleStatusKeyCount,
                           -- LocationKeys
                           COUNT(CASE
                                     WHEN fct_bal.LocationKey < 0
                                     THEN LocationKey
                                 END) AS LocationKeyCount,
                           -- OrgKeys
                           COUNT(CASE
                                     WHEN fct_bal.OrgKey < 0
                                     THEN OrgKey
                                 END) AS OrgKeyCount,
                           -- PayBasisKey
                           NULL AS PayBasisKeyCount,
                           -- PayGradeKey
                           NULL AS PayGradeKeyCount,
                           -- PaymentTypeKey
                           NULL AS PaymentTypeKeyCount,
                           -- PayRateChangeReasonKey
                           NULL AS PayRateChangeReasonKeyCount,
                           -- PayrollTypeKey
                           NULL AS PayrollTypeKeyCount,
                           -- PeopleGroupKey
                           NULL AS PeopleGroupKeyCount,
                           -- PerformanceRatingKey
                           NULL AS PerformanceRatingKeyCount,
                           -- PersonKey
                           NULL AS PersonKeyCount,
                           -- PositionKey
                           NULL AS PositionKeyCount,
                           -- ProgramKeys
                           NULL AS ProgramKeyCount,
                           -- QualificationTypeKey
                           NULL AS QualificationTypeKeyCount,
                           -- ScheduleTypeKey
                           NULL AS ScheduleTypeKeyCount,
                           -- ScheduleWeekKeys
                           NULL AS ScheduleWeekKeyCount,
                           -- SessionKeys
                           NULL AS SessionKeyCount,
                           -- SpecialInfoKey
                           NULL AS SpecialInfoKeyCount,
                           -- SponsorKeys
                           NULL AS SponsorKeyCount,
                           -- StudentKeys
                           NULL AS StudentKeyCount,
                           -- TierKeys
                           NULL AS TierKeyCount,
                           -- TransactionCodeKey
                           NULL AS TransactionCodeKeyCount,
                           -- TuitionAssistanceProviderKeys
                           NULL AS TuitionAssistanceProviderKeyCount,
                           -- WebCampaignKey
                           NULL AS WebCampaignKeyCount,
					  -- EDWCreatedDate
                           GETDATE()
                    FROM dbo.FactGLBalance fct_bal
                         LEFT JOIN dbo.DimDate dm_dt ON fct_bal.DateKey = dm_dt.DateKey
                    GROUP BY dm_dt.FiscalYearNumber
                    ORDER BY dm_dt.FiscalYearNumber;

             -- Log inserts

             SELECT @RowCount = @@ROWCOUNT;
             SELECT @InsertCount = @InsertCount + @RowCount;
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(VARCHAR(10), @RowCount)+' records for FactGLBalance';
             PRINT @DebugMsg;
             -- --------------------------------------------------------------------------------
             -- FactGLBalancePlanAllocation
             -- --------------------------------------------------------------------------------
             INSERT INTO dbo.EDWFactDimensionCoverage
                    SELECT @RunDateKey,
                           dm_dt.FiscalYearNumber,
                           'FactGLBalancePlanAllocation' AS FactTableName,
                           'G/L' AS SourceSystem,
                           -- Total Number of Records for the Fact
                           COUNT(1) AS TotalKeyCount,
                           -- Total Number of 'Unknown' or 'N/A' DateKeys (values of -1 & -2)
                           COUNT(CASE
                                     WHEN fct_pln.DateKey < 0
                                     THEN fct_pln.DateKey
                                 END) AS DateKeyCount,
                           -- Total Number of 'Unknown' or 'N/A' AccountSubAccountKeys (values of -1 & -2)
                           COUNT(CASE
                                     WHEN fct_pln.AccountSubAccountKey < 0
                                     THEN AccountSubAccountKey
                                 END) AS AccountSubAccountKeyCount,
                           -- AdjustmentReasonKeys
                           NULL AS AdjustmentReasonKeysCount,
                           -- ARAgingTypeKey
                           NULL AS ARAgencyTypeKey,
                           -- ARBalanceTypeKey
                           NULL AS ARBalanceTypeKey,
                           -- ARAgingBucketKey
                           NULL AS ARAgingBucketKey,
                           -- ClassroomKeys
                           NULL AS ClassroomKeyCount,
                           -- CompanyKeys
                           COUNT(CASE
                                     WHEN fct_pln.CompanyKey < 0
                                     THEN CompanyKey
                                 END) AS CompanyKeyCount,
                           -- ComplianceItemKey
                           NULL AS ComplianceItemKey,
                           -- ComplianceRatingKey
                           NULL AS ComplianceRatingKey,
                           -- CostCenterKeys
                           COUNT(CASE
                                     WHEN fct_pln.CostCenterKey < 0
                                     THEN CostCenterKey
                                 END) AS CostCenterKeyCount,
                           -- CostCenterTypeKeys
                           COUNT(CASE
                                     WHEN fct_pln.CostCenterTypeKey < 0
                                     THEN CostCenterTypeKey
                                 END) AS CostCenterTypeKeyCount,
                           -- CreditMemoTypeKey
                           NULL AS CreditMemoTypeKeyCount,
                           -- DataScenarioKey
                           NULL AS DataScenarioKeyCount,
                           -- DiscountTypeKey
                           NULL AS DiscountTypeKeyCount,
                           -- FeeTypeKeys
                           NULL AS FeeTypeKeyCount,
                           -- GLMetricTypeKey
                           COUNT(CASE
                                     WHEN fct_pln.GLMetricTypeKey < 0
                                     THEN GLMetricTypeKey
                                 END) AS GLMetricTypeKeyCount,
                           -- HRUserKey
                           NULL AS HRUserKeyCount,
                           -- InvoiceTypeKey
                           NULL AS InvoiceTypeKeyCount,
                           -- LeadKey
                           NULL AS LeadKeyCount,
                           -- LeadEventTypeKeyKey
                           NULL AS LeadEventTypeKeyCount,
                           -- LeadTypeKey
                           NULL AS LeadTypeKeyCount,
                           -- LeaveReasonKey
                           NULL AS LeaveReasonKeyCount,
                           -- LeaveTypeKey
                           NULL AS LeaveTypeKeyCount,
                           -- LifecycleStatusKey
                           NULL AS LifecycleStatusKeyCount,
                           -- LocationKeys
                           COUNT(CASE
                                     WHEN fct_pln.LocationKey < 0
                                     THEN LocationKey
                                 END) AS LocationKeyCount,
                           -- OrgKeys
                           COUNT(CASE
                                     WHEN fct_pln.OrgKey < 0
                                     THEN OrgKey
                                 END) AS OrgKeyCount,
                           -- PayBasisKey
                           NULL AS PayBasisKeyCount,
                           -- PayGradeKey
                           NULL AS PayGradeKeyCount,
                           -- PaymentTypeKey
                           NULL AS PaymentTypeKeyCount,
                           -- PayRateChangeReasonKey
                           NULL AS PayRateChangeReasonKeyCount,
                           -- PayrollTypeKey
                           NULL AS PayrollTypeKeyCount,
                           -- PeopleGroupKey
                           NULL AS PeopleGroupKeyCount,
                           -- PerformanceRatingKey
                           NULL AS PerformanceRatingKeyCount,
                           -- PersonKey
                           NULL AS PersonKeyCount,
                           -- PositionKey
                           NULL AS PositionKeyCount,
                           -- ProgramKeys
                           NULL AS ProgramKeyCount,
                           -- QualificationTypeKey
                           NULL AS QualificationTypeKeyCount,
                           -- ScheduleTypeKey
                           NULL AS ScheduleTypeKeyCount,
                           -- ScheduleWeekKeys
                           NULL AS ScheduleWeekKeyCount,
                           -- SessionKeys
                           NULL AS SessionKeyCount,
                           -- SpecialInfoKey
                           NULL AS SpecialInfoKeyCount,
                           -- SponsorKeys
                           NULL AS SponsorKeyCount,
                           -- StudentKeys
                           NULL AS StudentKeyCount,
                           -- TierKeys
                           NULL AS TierKeyCount,
                           -- TransactionCodeKey
                           NULL AS TransactionCodeKeyCount,
                           -- TuitionAssistanceProviderKeys
                           NULL AS TuitionAssistanceProviderKeyCount,
                           -- WebCampaignKey
                           NULL AS WebCampaignKeyCount,
					  -- EDWCreatedDate
                           GETDATE()
                    FROM dbo.FactGLBalancePlanAllocation fct_pln
                         LEFT JOIN dbo.DimDate dm_dt ON fct_pln.DateKey = dm_dt.DateKey
                    GROUP BY dm_dt.FiscalYearNumber
                    ORDER BY dm_dt.FiscalYearNumber;

             -- Log inserts

             SELECT @RowCount = @@ROWCOUNT;
             SELECT @InsertCount = @InsertCount + @RowCount;
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(VARCHAR(10), @RowCount)+' records for FactGLBalancePlanAllocation';
             PRINT @DebugMsg;

             -- --------------------------------------------------------------------------------
             -- FactLaborHour
             -- --------------------------------------------------------------------------------
             INSERT INTO dbo.EDWFactDimensionCoverage
                    SELECT @RunDateKey,
                           dm_dt.FiscalYearNumber,
                           'FactLaborHour' AS FactTableName,
                           'G/L' AS SourceSystem,
                           -- Total Number of Records for the Fact
                           COUNT(1) AS TotalKeyCount,
                           -- Total Number of 'Unknown' or 'N/A' DateKeys (values of -1 & -2)
                           COUNT(CASE
                                     WHEN fct_lbr.DateKey < 0
                                     THEN fct_lbr.DateKey
                                 END) AS DateKeyCount,
                           -- Total Number of 'Unknown' or 'N/A' AccountSubAccountKeys (values of -1 & -2)
                           COUNT(CASE
                                     WHEN fct_lbr.AccountSubAccountKey < 0
                                     THEN AccountSubAccountKey
                                 END) AS AccountSubAccountKeyCount,
                           -- AdjustmentReasonKeys
                           NULL AS AdjustmentReasonKeysCount,
                           -- ARAgingTypeKey
                           NULL AS ARAgencyTypeKey,
                           -- ARBalanceTypeKey
                           NULL AS ARBalanceTypeKey,
                           -- ARAgingBucketKey
                           NULL AS ARAgingBucketKey,
                           -- ClassroomKeys
                           NULL AS ClassroomKeyCount,
                           -- CompanyKeys
                           NULL AS CompanyKeyCount,
                           -- ComplianceItemKey
                           NULL AS ComplianceItemKey,
                           -- ComplianceRatingKey
                           NULL AS ComplianceRatingKey,
                           -- CostCenterKeys
                           COUNT(CASE
                                     WHEN fct_lbr.CostCenterKey < 0
                                     THEN CostCenterKey
                                 END) AS CostCenterKeyCount,
                           -- CostCenterTypeKeys
                           NULL AS CostCenterTypeKeyCount,
                           -- CreditMemoTypeKey
                           NULL AS CreditMemoTypeKeyCount,
                           -- DataScenarioKey
                           NULL AS DataScenarioKeyCount,
                           -- DiscountTypeKey
                           NULL AS DiscountTypeKeyCount,
                           -- FeeTypeKeys
                           NULL AS FeeTypeKeyCount,
                           -- GLMetricTypeKey
                           NULL AS GLMetricTypeKeyCount,
                           -- HRUserKey
                           NULL AS HRUserKeyCount,
                           -- InvoiceTypeKey
                           NULL AS InvoiceTypeKeyCount,
                           -- LeadKey
                           NULL AS LeadKeyCount,
                           -- LeadEventTypeKeyKey
                           NULL AS LeadEventTypeKeyCount,
                           -- LeadTypeKey
                           NULL AS LeadTypeKeyCount,
                           -- LeaveReasonKey
                           NULL AS LeaveReasonKeyCount,
                           -- LeaveTypeKey
                           NULL AS LeaveTypeKeyCount,
                           -- LifecycleStatusKey
                           NULL AS LifecycleStatusKeyCount,
                           -- LocationKeys
                           NULL AS LocationKeyCount,
                           -- OrgKeys
                           COUNT(CASE
                                     WHEN fct_lbr.OrgKey < 0
                                     THEN OrgKey
                                 END) AS OrgKeyCount,
                           -- PayBasisKey
                           COUNT(CASE
                                     WHEN fct_lbr.PayBasisKey < 0
                                     THEN PayBasisKey
                                 END) AS PayBasisKeyCount,
                           -- PayGradeKey
                           NULL AS PayGradeKeyCount,
                           -- PaymentTypeKey
                           NULL AS PaymentTypeKeyCount,
                           -- PayRateChangeReasonKey
                           NULL AS PayRateChangeReasonKeyCount,
                           -- PayrollTypeKey
                           NULL AS PayrollTypeKeyCount,
                           -- PeopleGroupKey
                           NULL AS PeopleGroupKeyCount,
                           -- PerformanceRatingKey
                           NULL AS PerformanceRatingKeyCount,
                           -- PersonKey
                           NULL AS PersonKeyCount,
                           -- PositionKey
                           NULL AS PositionKeyCount,
                           -- ProgramKeys
                           NULL AS ProgramKeyCount,
                           -- QualificationTypeKey
                           NULL AS QualificationTypeKeyCount,
                           -- ScheduleTypeKey
                           NULL AS ScheduleTypeKeyCount,
                           -- ScheduleWeekKeys
                           NULL AS ScheduleWeekKeyCount,
                           -- SessionKeys
                           NULL AS SessionKeyCount,
                           -- SpecialInfoKey
                           NULL AS SpecialInfoKeyCount,
                           -- SponsorKeys
                           NULL AS SponsorKeyCount,
                           -- StudentKeys
                           NULL AS StudentKeyCount,
                           -- TierKeys
                           NULL AS TierKeyCount,
                           -- TransactionCodeKey
                           NULL AS TransactionCodeKeyCount,
                           -- TuitionAssistanceProviderKeys
                           NULL AS TuitionAssistanceProviderKeyCount,
                           -- WebCampaignKey
                           NULL AS WebCampaignKeyCount,
					  -- EDWCreatedDate
                           GETDATE()
                    FROM dbo.FactLaborHour fct_lbr
                         LEFT JOIN dbo.DimDate dm_dt ON fct_lbr.DateKey = dm_dt.DateKey
                    GROUP BY dm_dt.FiscalYearNumber
                    ORDER BY dm_dt.FiscalYearNumber;

             -- Log inserts

             SELECT @RowCount = @@ROWCOUNT;
             SELECT @InsertCount = @InsertCount + @RowCount;
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(VARCHAR(10), @RowCount)+' records for FactLaborHour';
             PRINT @DebugMsg;

             -- --------------------------------------------------------------------------------
             -- FactLaborSalary
             -- --------------------------------------------------------------------------------
             INSERT INTO dbo.EDWFactDimensionCoverage
                    SELECT @RunDateKey,
                           dm_dt.FiscalYearNumber,
                           'FactLaborSalary' AS FactTableName,
                           'ADP' AS SourceSystem,
                           -- Total Number of Records for the Fact
                           COUNT(1) AS TotalKeyCount,
                           -- Total Number of 'Unknown' or 'N/A' DateKeys (values of -1 & -2)
                           COUNT(CASE
                                     WHEN fct_sly.DateKey < 0
                                     THEN fct_sly.DateKey
                                 END) AS DateKeyCount,
                           -- Total Number of 'Unknown' or 'N/A' AccountSubAccountKeys (values of -1 & -2)
                           COUNT(CASE
                                     WHEN fct_sly.AccountSubAccountKey < 0
                                     THEN AccountSubAccountKey
                                 END) AS AccountSubAccountKeyCount,
                           -- AdjustmentReasonKeys
                           NULL AS AdjustmentReasonKeysCount,
                           -- ARAgingTypeKey
                           NULL AS ARAgencyTypeKey,
                           -- ARBalanceTypeKey
                           NULL AS ARBalanceTypeKey,
                           -- ARAgingBucketKey
                           NULL AS ARAgingBucketKey,
                           -- ClassroomKeys
                           NULL AS ClassroomKeyCount,
                           -- CompanyKeys
                           COUNT(CASE
                                     WHEN fct_sly.CompanyKey < 0
                                     THEN CompanyKey
                                 END) AS CompanyKeyCount,
                           -- ComplianceItemKey
                           NULL AS ComplianceItemKey,
                           -- ComplianceRatingKey
                           NULL AS ComplianceRatingKey,
                           -- CostCenterKeys
                           COUNT(CASE
                                     WHEN fct_sly.CostCenterKey < 0
                                     THEN CostCenterKey
                                 END) AS CostCenterKeyCount,
                           -- CostCenterTypeKeys
                           COUNT(CASE
                                     WHEN fct_sly.CostCenterTypeKey < 0
                                     THEN CostCenterTypeKey
                                 END) AS CostCenterTypeKeyCount,
                           -- CreditMemoTypeKey
                           NULL AS CreditMemoTypeKeyCount,
                           -- DataScenarioKey
                           NULL AS DataScenarioKeyCount,
                           -- DiscountTypeKey
                           NULL AS DiscountTypeKeyCount,
                           -- FeeTypeKeys
                           NULL AS FeeTypeKeyCount,
                           -- GLMetricTypeKey
                           NULL AS GLMetricTypeKeyCount,
                           -- HRUserKey
                           NULL AS HRUserKeyCount,
                           -- InvoiceTypeKey
                           NULL AS InvoiceTypeKeyCount,
                           -- LeadKey
                           NULL AS LeadKeyCount,
                           -- LeadEventTypeKeyKey
                           NULL AS LeadEventTypeKeyCount,
                           -- LeadTypeKey
                           NULL AS LeadTypeKeyCount,
                           -- LeaveReasonKey
                           NULL AS LeaveReasonKeyCount,
                           -- LeaveTypeKey
                           NULL AS LeaveTypeKeyCount,
                           -- LifecycleStatusKey
                           NULL AS LifecycleStatusKeyCount,
                           -- LocationKeys
                           NULL AS LocationKeyCount,
                           -- OrgKeys
                           NULL AS OrgKeyCount,
                           -- PayBasisKey
                           NULL AS PayBasisKeyCount,
                           -- PayGradeKey
                           NULL AS PayGradeKeyCount,
                           -- PaymentTypeKey
                           NULL AS PaymentTypeKeyCount,
                           -- PayRateChangeReasonKey
                           NULL AS PayRateChangeReasonKeyCount,
                           -- PayrollTypeKey
                           COUNT(CASE
                                     WHEN fct_sly.PayrollTypeKey < 0
                                     THEN PayrollTypeKey
                                 END) AS PayrollTypeKeyCount,
                           -- PeopleGroupKey
                           NULL AS PeopleGroupKeyCount,
                           -- PerformanceRatingKey
                           NULL AS PerformanceRatingKeyCount,
                           -- PersonKey
                           COUNT(CASE
                                     WHEN fct_sly.EmployeeNumber < 0
                                     THEN EmployeeNumber
                                 END) AS PersonKeyCount,
                           -- PositionKey
                           NULL AS PositionKeyCount,
                           -- ProgramKeys
                           NULL AS ProgramKeyCount,
                           -- QualificationTypeKey
                           NULL AS QualificationTypeKeyCount,
                           -- ScheduleTypeKey
                           NULL AS ScheduleTypeKeyCount,
                           -- ScheduleWeekKeys
                           NULL AS ScheduleWeekKeyCount,
                           -- SessionKeys
                           NULL AS SessionKeyCount,
                           -- SpecialInfoKey
                           NULL AS SpecialInfoKeyCount,
                           -- SponsorKeys
                           NULL AS SponsorKeyCount,
                           -- StudentKeys
                           NULL AS StudentKeyCount,
                           -- TierKeys
                           NULL AS TierKeyCount,
                           -- TransactionCodeKey
                           NULL AS TransactionCodeKeyCount,
                           -- TuitionAssistanceProviderKeys
                           NULL AS TuitionAssistanceProviderKeyCount,
                           -- WebCampaignKey
                           NULL AS WebCampaignKeyCount,
					  -- EDWCreatedDate
                           GETDATE()
                    FROM dbo.FactLaborSalary fct_sly
                         LEFT JOIN dbo.DimDate dm_dt ON fct_sly.DateKey = dm_dt.DateKey
                    GROUP BY dm_dt.FiscalYearNumber
                    ORDER BY dm_dt.FiscalYearNumber;

             -- Log inserts

             SELECT @RowCount = @@ROWCOUNT;
             SELECT @InsertCount = @InsertCount + @RowCount;
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(VARCHAR(10), @RowCount)+' records for FactLaborSalary';
             PRINT @DebugMsg;
             -- --------------------------------------------------------------------------------
             -- FactLeadEvent
             -- --------------------------------------------------------------------------------
             INSERT INTO dbo.EDWFactDimensionCoverage
                    SELECT @RunDateKey,
                           dm_dt.FiscalYearNumber,
                           'FactLeadEvent' AS FactTableName,
                           'SalesForce' AS SourceSystem,
                           -- Total Number of Records for the Fact
                           COUNT(1) AS TotalKeyCount,
                           -- Total Number of 'Unknown' or 'N/A' DateKeys (values of -1 & -2)
                           COUNT(CASE
                                     WHEN fct_ldevt.DateKey < 0
                                     THEN fct_ldevt.DateKey
                                 END) AS DateKeyCount,
                           -- Total Number of 'Unknown' or 'N/A' AccountSubAccountKeys (values of -1 & -2)
                           NULL AS AccountSubAccountKeyCount,
                           -- AdjustmentReasonKeys
                           NULL AS AdjustmentReasonKeysCount,
                           -- ARAgingTypeKey
                           NULL AS ARAgencyTypeKey,
                           -- ARBalanceTypeKey
                           NULL AS ARBalanceTypeKey,
                           -- ARAgingBucketKey
                           NULL AS ARAgingBucketKey,
                           -- ClassroomKeys
                           NULL AS ClassroomKeyCount,
                           -- CompanyKeys
                           COUNT(CASE
                                     WHEN fct_ldevt.CompanyKey < 0
                                     THEN CompanyKey
                                 END) AS CompanyKeyCount,
                           -- ComplianceItemKey
                           NULL AS ComplianceItemKey,
                           -- ComplianceRatingKey
                           NULL AS ComplianceRatingKey,
                           -- CostCenterKeys
                           COUNT(CASE
                                     WHEN fct_ldevt.CostCenterKey < 0
                                     THEN CostCenterKey
                                 END) AS CostCenterKeyCount,
                           -- CostCenterTypeKeys
                           COUNT(CASE
                                     WHEN fct_ldevt.CostCenterTypeKey < 0
                                     THEN CostCenterTypeKey
                                 END) AS CostCenterTypeKeyCount,
                           -- CreditMemoTypeKey
                           NULL AS CreditMemoTypeKeyCount,
                           -- DataScenarioKey
                           NULL AS DataScenarioKeyCount,
                           -- DiscountTypeKey
                           NULL AS DiscountTypeKeyCount,
                           -- FeeTypeKeys
                           NULL AS FeeTypeKeyCount,
                           -- GLMetricTypeKey
                           NULL AS GLMetricTypeKeyCount,
                           -- HRUserKey
                           NULL AS HRUserKeyCount,
                           -- InvoiceTypeKey
                           NULL AS InvoiceTypeKeyCount,
                           -- LeadKey
                           COUNT(CASE
                                     WHEN fct_ldevt.LeadKey < 0
                                     THEN LeadKey
                                 END) AS LeadKeyCount,
                           -- LeadEventTypeKeyKey
                           COUNT(CASE
                                     WHEN fct_ldevt.LeadEventTypeKey < 0
                                     THEN LeadKey
                                 END) AS LeadEventTypeKeyKey,
                           -- LeadTypeKey
                           NULL AS LeadTypeKeyCount,
                           -- LeaveReasonKey
                           NULL AS LeaveReasonKeyCount,
                           -- LeaveTypeKey
                           NULL AS LeaveTypeKeyCount,
                           -- LifecycleStatusKey
                           NULL AS LifecycleStatusKeyCount,
                           -- LocationKeys
                           COUNT(CASE
                                     WHEN fct_ldevt.LocationKey < 0
                                     THEN LocationKey
                                 END) AS LocationKeyCount,
                           -- OrgKeys
                           COUNT(CASE
                                     WHEN fct_ldevt.OrgKey < 0
                                     THEN OrgKey
                                 END) AS OrgKeyCount,
                           -- PayBasisKey
                           NULL AS PayBasisKeyCount,
                           -- PayGradeKey
                           NULL AS PayGradeKeyCount,
                           -- PaymentTypeKey
                           NULL AS PaymentTypeKeyCount,
                           -- PayRateChangeReasonKey
                           NULL AS PayRateChangeReasonKeyCount,
                           -- PayrollTypeKey
                           NULL AS PayrollTypeKeyCount,
                           -- PeopleGroupKey
                           NULL AS PeopleGroupKeyCount,
                           -- PerformanceRatingKey
                           NULL AS PerformanceRatingKeyCount,
                           -- PersonKey
                           NULL AS PersonKeyCount,
                           -- PositionKey
                           NULL AS PositionKeyCount,
                           -- ProgramKeys
                           NULL AS ProgramKeyCount,
                           -- QualificationTypeKey
                           NULL AS QualificationTypeKeyCount,
                           -- ScheduleTypeKey
                           NULL AS ScheduleTypeKeyCount,
                           -- ScheduleWeekKeys
                           NULL AS ScheduleWeekKeyCount,
                           -- SessionKeys
                           NULL AS SessionKeyCount,
                           -- SpecialInfoKey
                           NULL AS SpecialInfoKeyCount,
                           -- SponsorKeys
                           NULL AS SponsorKeyCount,
                           -- StudentKeys
                           NULL AS StudentKeyCount,
                           -- TierKeys
                           NULL AS TierKeyCount,
                           -- TransactionCodeKey
                           NULL AS TransactionCodeKeyCount,
                           -- TuitionAssistanceProviderKeys
                           NULL AS TuitionAssistanceProviderKeyCount,
                           -- WebCampaignKey
                           COUNT(CASE
                                     WHEN fct_ldevt.WebCampaignKey < 0
                                     THEN WebCampaignKey
                                 END) AS WebCampaignKeyCount,
					  -- EDWCreatedDate
                           GETDATE()
                    FROM dbo.FactLeadEvent fct_ldevt
                         LEFT JOIN dbo.DimDate dm_dt ON fct_ldevt.DateKey = dm_dt.DateKey
                    GROUP BY dm_dt.FiscalYearNumber
                    ORDER BY dm_dt.FiscalYearNumber;

             -- Log inserts

             SELECT @RowCount = @@ROWCOUNT;
             SELECT @InsertCount = @InsertCount + @RowCount;
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(VARCHAR(10), @RowCount)+' records for FactLeadEvent';
             PRINT @DebugMsg;
             -- --------------------------------------------------------------------------------
             -- FactLeadPipeline
             -- --------------------------------------------------------------------------------
             INSERT INTO dbo.EDWFactDimensionCoverage
                    SELECT @RunDateKey,
                           dm_dt.FiscalYearNumber,
                           'FactLeadPipeline' AS FactTableName,
                           'SalesForce' AS SourceSystem,
                           -- Total Number of Records for the Fact
                           COUNT(1) AS TotalKeyCount,
                           -- Total Number of 'Unknown' or 'N/A' DateKeys (values of -1 & -2)
                           COUNT(CASE
                                     WHEN fct_ldppl.InquiryDateKey < 0
                                     THEN fct_ldppl.InquiryDateKey
                                 END) AS DateKeyCount,
                           -- Total Number of 'Unknown' or 'N/A' AccountSubAccountKeys (values of -1 & -2)
                           NULL AS AccountSubAccountKeyCount,
                           -- AdjustmentReasonKeys
                           NULL AS AdjustmentReasonKeysCount,
                           -- ARAgingTypeKey
                           NULL AS ARAgencyTypeKey,
                           -- ARBalanceTypeKey
                           NULL AS ARBalanceTypeKey,
                           -- ARAgingBucketKey
                           NULL AS ARAgingBucketKey,
                           -- ClassroomKeys
                           NULL AS ClassroomKeyCount,
                           -- CompanyKeys
                           COUNT(CASE
                                     WHEN fct_ldppl.CompanyKey < 0
                                     THEN CompanyKey
                                 END) AS CompanyKeyCount,
                           -- ComplianceItemKey
                           NULL AS ComplianceItemKey,
                           -- ComplianceRatingKey
                           NULL AS ComplianceRatingKey,
                           -- CostCenterKeys
                           COUNT(CASE
                                     WHEN fct_ldppl.CostCenterKey < 0
                                     THEN CostCenterKey
                                 END) AS CostCenterKeyCount,
                           -- CostCenterTypeKeys
                           COUNT(CASE
                                     WHEN fct_ldppl.CostCenterTypeKey < 0
                                     THEN CostCenterTypeKey
                                 END) AS CostCenterTypeKeyCount,
                           -- CreditMemoTypeKey
                           NULL AS CreditMemoTypeKeyCount,
                           -- DataScenarioKey
                           NULL AS DataScenarioKeyCount,
                           -- DiscountTypeKey
                           NULL AS DiscountTypeKeyCount,
                           -- FeeTypeKeys
                           NULL AS FeeTypeKeyCount,
                           -- GLMetricTypeKey
                           NULL AS GLMetricTypeKeyCount,
                           -- HRUserKey
                           NULL AS HRUserKeyCount,
                           -- InvoiceTypeKey
                           NULL AS InvoiceTypeKeyCount,
                           -- LeadKey
                           COUNT(CASE
                                     WHEN fct_ldppl.LeadKey < 0
                                     THEN LeadKey
                                 END) AS LeadKeyCount,
                           -- LeadEventTypeKeyKey
                           NULL AS LeadEventTypeKeyKey,
                           -- LeadTypeKey
                           NULL AS LeadTypeKeyCount,
                           -- LeaveReasonKey
                           NULL AS LeaveReasonKeyCount,
                           -- LeaveTypeKey
                           NULL AS LeaveTypeKeyCount,
                           -- LifecycleStatusKey
                           NULL AS LifecycleStatusKeyCount,
                           -- LocationKeys
                           COUNT(CASE
                                     WHEN fct_ldppl.LocationKey < 0
                                     THEN LocationKey
                                 END) AS LocationKeyCount,
                           -- OrgKeys
                           COUNT(CASE
                                     WHEN fct_ldppl.OrgKey < 0
                                     THEN OrgKey
                                 END) AS OrgKeyCount,
                           -- PayBasisKey
                           NULL AS PayBasisKeyCount,
                           -- PayGradeKey
                           NULL AS PayGradeKeyCount,
                           -- PaymentTypeKey
                           NULL AS PaymentTypeKeyCount,
                           -- PayRateChangeReasonKey
                           NULL AS PayRateChangeReasonKeyCount,
                           -- PayrollTypeKey
                           NULL AS PayrollTypeKeyCount,
                           -- PeopleGroupKey
                           NULL AS PeopleGroupKeyCount,
                           -- PerformanceRatingKey
                           NULL AS PerformanceRatingKeyCount,
                           -- PersonKey
                           NULL AS PersonKeyCount,
                           -- PositionKey
                           NULL AS PositionKeyCount,
                           -- ProgramKeys
                           NULL AS ProgramKeyCount,
                           -- QualificationTypeKey
                           NULL AS QualificationTypeKeyCount,
                           -- ScheduleTypeKey
                           NULL AS ScheduleTypeKeyCount,
                           -- ScheduleWeekKeys
                           NULL AS ScheduleWeekKeyCount,
                           -- SessionKeys
                           NULL AS SessionKeyCount,
                           -- SpecialInfoKey
                           NULL AS SpecialInfoKeyCount,
                           -- SponsorKeys
                           NULL AS SponsorKeyCount,
                           -- StudentKeys
                           NULL AS StudentKeyCount,
                           -- TierKeys
                           NULL AS TierKeyCount,
                           -- TransactionCodeKey
                           NULL AS TransactionCodeKeyCount,
                           -- TuitionAssistanceProviderKeys
                           NULL AS TuitionAssistanceProviderKeyCount,
                           -- WebCampaignKey
                           COUNT(CASE
                                     WHEN fct_ldppl.WebCampaignKey < 0
                                     THEN WebCampaignKey
                                 END) AS WebCampaignKeyCount,
					  -- EDWCreatedDate
                           GETDATE()
                    FROM dbo.FactLeadPipeline fct_ldppl
                         LEFT JOIN dbo.DimDate dm_dt ON fct_ldppl.InquiryDateKey = dm_dt.DateKey
                    GROUP BY dm_dt.FiscalYearNumber
                    ORDER BY dm_dt.FiscalYearNumber;
             -- Log inserts

             SELECT @RowCount = @@ROWCOUNT;
             SELECT @InsertCount = @InsertCount + @RowCount;
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(VARCHAR(10), @RowCount)+' records for FactLeadPipeline';
             PRINT @DebugMsg;
             -- --------------------------------------------------------------------------------
             -- FactLifecycleStatusSnapshot
             -- --------------------------------------------------------------------------------
             INSERT INTO dbo.EDWFactDimensionCoverage
                    SELECT @RunDateKey,
                           dm_dt.FiscalYearNumber,
                           'FactLifecycleStatusSnapshot' AS FactTableName,
                           fct_lfc.SourceSystem,
                           -- Total Number of Records for the Fact
                           COUNT(1) AS TotalKeyCount,
                           -- Total Number of 'Unknown' or 'N/A' DateKeys (values of -1 & -2)
                           COUNT(CASE
                                     WHEN fct_lfc.DateKey < 0
                                     THEN fct_lfc.DateKey
                                 END) AS DateKeyCount,
                           -- Total Number of 'Unknown' or 'N/A' AccountSubAccountKeys (values of -1 & -2)
                           NULL AS AccountSubAccountKeyCount,
                           -- AdjustmentReasonKeys
                           NULL AS AdjustmentReasonKeysCount,
                           -- ARAgingTypeKey
                           NULL AS ARAgencyTypeKey,
                           -- ARBalanceTypeKey
                           NULL AS ARBalanceTypeKey,
                           -- ARAgingBucketKey
                           NULL AS ARAgingBucketKey,
                           -- ClassroomKeys
                           NULL AS ClassroomKeyCount,
                           -- CompanyKeys
                           NULL AS CompanyKeyCount,
                           -- ComplianceItemKey
                           NULL AS ComplianceItemKey,
                           -- ComplianceRatingKey
                           NULL AS ComplianceRatingKey,
                           -- CostCenterKeys
                           COUNT(CASE
                                     WHEN fct_lfc.CostCenterKey < 0
                                     THEN CostCenterKey
                                 END) AS CostCenterKeyCount,
                           -- CostCenterTypeKeys
                           COUNT(CASE
                                     WHEN fct_lfc.CostCenterTypeKey < 0
                                     THEN CostCenterTypeKey
                                 END) AS CostCenterTypeKeyCount,
                           -- CreditMemoTypeKey
                           NULL AS CreditMemoTypeKeyCount,
                           -- DataScenarioKey
                           NULL AS DataScenarioKeyCount,
                           -- DiscountTypeKey
                           NULL AS DiscountTypeKeyCount,
                           -- FeeTypeKeys
                           NULL AS FeeTypeKeyCount,
                           -- GLMetricTypeKey
                           NULL AS GLMetricTypeKeyCount,
                           -- HRUserKey
                           NULL AS HRUserKeyCount,
                           -- InvoiceTypeKey
                           NULL AS InvoiceTypeKeyCount,
                           -- LeadKey
                           NULL AS LeadKeyCount,
                           -- LeadEventTypeKeyKey
                           NULL AS LeadEventTypeKeyCount,
                           -- LeadTypeKey
                           NULL AS LeadTypeKeyCount,
                           -- LeaveReasonKey
                           NULL AS LeaveReasonKeyCount,
                           -- LeaveTypeKey
                           NULL AS LeaveTypeKeyCount,
                           -- LifecycleStatusKey
                           NULL AS LifecycleStatusKeyCount,
                           -- LocationKeys
                           COUNT(CASE
                                     WHEN fct_lfc.LocationKey < 0
                                     THEN LocationKey
                                 END) AS LocationKeyCount,
                           -- OrgKeys
                           COUNT(CASE
                                     WHEN fct_lfc.OrgKey < 0
                                     THEN OrgKey
                                 END) AS OrgKeyCount,
                           -- PayBasisKey
                           NULL AS PayBasisKeyCount,
                           -- PayGradeKey
                           NULL AS PayGradeKeyCount,
                           -- PaymentTypeKey
                           NULL AS PaymentTypeKeyCount,
                           -- PayRateChangeReasonKey
                           NULL AS PayRateChangeReasonKeyCount,
                           -- PayrollTypeKey
                           NULL AS PayrollTypeKeyCount,
                           -- PeopleGroupKey
                           NULL AS PeopleGroupKeyCount,
                           -- PerformanceRatingKey
                           NULL AS PerformanceRatingKeyCount,
                           -- PersonKey
                           NULL AS PersonKeyCount,
                           -- PositionKey
                           NULL AS PositionKeyCount,
                           -- ProgramKeys
                           NULL AS ProgramKeyCount,
                           -- QualificationTypeKey
                           NULL AS QualificationTypeKeyCount,
                           -- ScheduleTypeKey
                           NULL AS ScheduleTypeKeyCount,
                           -- ScheduleWeekKeys
                           NULL AS ScheduleWeekKeyCount,
                           -- SessionKeys
                           NULL AS SessionKeyCount,
                           -- SpecialInfoKey
                           NULL AS SpecialInfoKeyCount,
                           -- SponsorKeys
                           NULL AS SponsorKeyCount,
                           -- StudentKeys
                           COUNT(CASE
                                     WHEN fct_lfc.StudentKey < 0
                                     THEN StudentKey
                                 END) AS StudentKeyCount,
                           -- TierKeys
                           NULL AS TierKeyCount,
                           -- TransactionCodeKey
                           NULL AS TransactionCodeKeyCount,
                           -- TuitionAssistanceProviderKeys
                           NULL AS TuitionAssistanceProviderKeyCount,
                           -- WebCampaignKey
                           NULL AS WebCampaignKeyCount,
					  -- EDWCreatedDate
                           GETDATE()
                    FROM dbo.FactLifecycleStatusSnapshot fct_lfc
                         LEFT JOIN dbo.DimDate dm_dt ON fct_lfc.DateKey = dm_dt.DateKey
                    GROUP BY dm_dt.FiscalYearNumber,
                             SourceSystem
                    ORDER BY dm_dt.FiscalYearNumber,
                             SourceSystem;

             -- Log inserts

             SELECT @RowCount = @@ROWCOUNT;
             SELECT @InsertCount = @InsertCount + @RowCount;
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(VARCHAR(10), @RowCount)+' records for FactLifecycleStatusSnapshot';
             PRINT @DebugMsg;
             -- --------------------------------------------------------------------------------
             -- FactNetRevenue
             -- --------------------------------------------------------------------------------
             INSERT INTO dbo.EDWFactDimensionCoverage
                    SELECT @RunDateKey,
                           dm_dt.FiscalYearNumber,
                           'FactNetRevenue' AS FactTableName,
                           fct_rev.SourceSystem,
                           -- Total Number of Records for the Fact
                           COUNT(1) AS TotalKeyCount,
                           -- Total Number of 'Unknown' or 'N/A' DateKeys (values of -1 & -2)
                           COUNT(CASE
                                     WHEN fct_rev.DateKey < 0
                                     THEN fct_rev.DateKey
                                 END) AS DateKeyCount,
                           -- Total Number of 'Unknown' or 'N/A' AccountSubAccountKeys (values of -1 & -2)
                           COUNT(CASE
                                     WHEN fct_rev.AccountSubAccountKey < 0
                                     THEN AccountSubAccountKey
                                 END) AS AccountSubAccountKeyCount,
                           -- AdjustmentReasonKeys
                           NULL AS AdjustmentReasonKeysCount,
                           -- ARAgingTypeKey
                           NULL AS ARAgencyTypeKey,
                           -- ARBalanceTypeKey
                           NULL AS ARBalanceTypeKey,
                           -- ARAgingBucketKey
                           NULL AS ARAgingBucketKey,
                           -- ClassroomKeys
                           NULL AS ClassroomKeyCount,
                           -- CompanyKeys
                           COUNT(CASE
                                     WHEN fct_rev.CompanyKey < 0
                                     THEN CompanyKey
                                 END) AS CompanyKeyCount,
                           -- ComplianceItemKey
                           NULL AS ComplianceItemKey,
                           -- ComplianceRatingKey
                           NULL AS ComplianceRatingKey,
                           -- CostCenterKeys
                           COUNT(CASE
                                     WHEN fct_rev.CostCenterKey < 0
                                     THEN CostCenterKey
                                 END) AS CostCenterKeyCount,
                           -- CostCenterTypeKeys
                           COUNT(CASE
                                     WHEN fct_rev.CostCenterTypeKey < 0
                                     THEN CostCenterTypeKey
                                 END) AS CostCenterTypeKeyCount,
                           -- CreditMemoTypeKey
                           COUNT(CASE
                                     WHEN fct_rev.CreditMemoTypeKey < 0
                                     THEN CreditMemoTypeKey
                                 END) AS CreditMemoTypeKeyCount,
                           -- DataScenarioKey
                           NULL AS DataScenarioKeyCount,
                           -- DiscountTypeKey
                           COUNT(CASE
                                     WHEN fct_rev.DiscountTypeKey < 0
                                     THEN DiscountTypeKey
                                 END) AS DiscountTypeKeyCount,
                           -- FeeTypeKeys
                           COUNT(CASE
                                     WHEN fct_rev.FeeTypeKey < 0
                                     THEN FeeTypeKey
                                 END) AS FeeTypeKeyCount,
                           -- GLMetricTypeKey
                           NULL AS GLMetricTypeKeyCount,
                           -- HRUserKey
                           NULL AS HRUserKeyCount,
                           -- InvoiceTypeKey
                           COUNT(CASE
                                     WHEN fct_rev.InvoiceTypeKey < 0
                                     THEN InvoiceTypeKey
                                 END) AS InvoiceTypeKeyCount,
                           -- LeadKey
                           NULL AS LeadKeyCount,
                           -- LeadEventTypeKeyKey
                           NULL AS LeadEventTypeKeyCount,
                           -- LeadTypeKey
                           NULL AS LeadTypeKeyCount,
                           -- LeaveReasonKey
                           NULL AS LeaveReasonKeyCount,
                           -- LeaveTypeKey
                           NULL AS LeaveTypeKeyCount,
                           -- LifecycleStatusKey
                           COUNT(CASE
                                     WHEN fct_rev.LifecycleStatusKey < 0
                                     THEN LifecycleStatusKey
                                 END) AS LifecycleStatusKeyCount,
                           -- LocationKeys
                           COUNT(CASE
                                     WHEN fct_rev.LocationKey < 0
                                     THEN LocationKey
                                 END) AS LocationKeyCount,
                           -- OrgKeys
                           COUNT(CASE
                                     WHEN fct_rev.OrgKey < 0
                                     THEN OrgKey
                                 END) AS OrgKeyCount,
                           -- PayBasisKey
                           NULL AS PayBasisKeyCount,
                           -- PayGradeKey
                           NULL AS PayGradeKeyCount,
                           -- PaymentTypeKey
                           NULL AS PaymentTypeKeyCount,
                           -- PayRateChangeReasonKey
                           NULL AS PayRateChangeReasonKeyCount,
                           -- PayrollTypeKey
                           NULL AS PayrollTypeKeyCount,
                           -- PeopleGroupKey
                           NULL AS PeopleGroupKeyCount,
                           -- PerformanceRatingKey
                           NULL AS PerformanceRatingKeyCount,
                           -- PersonKey
                           NULL AS PersonKeyCount,
                           -- PositionKey
                           NULL AS PositionKeyCount,
                           -- ProgramKeys
                           COUNT(CASE
                                     WHEN fct_rev.ProgramKey < 0
                                     THEN ProgramKey
                                 END) AS ProgramKeyCount,
                           -- QualificationTypeKey
                           NULL AS QualificationTypeKeyCount,
                           -- ScheduleTypeKey
                           NULL AS ScheduleTypeKeyCount,
                           -- ScheduleWeekKeys
                           COUNT(CASE
                                     WHEN fct_rev.ScheduleWeekKey < 0
                                     THEN ScheduleWeekKey
                                 END) AS ScheduleWeekKeyCount,
                           -- SessionKeys
                           COUNT(CASE
                                     WHEN fct_rev.SessionKey < 0
                                     THEN SessionKey
                                 END) AS SessionKeyCount,
                           -- SpecialInfoKey
                           NULL AS SpecialInfoKeyCount,
                           -- SponsorKeys
                           COUNT(CASE
                                     WHEN fct_rev.SponsorKey < 0
                                     THEN SponsorKey
                                 END) AS SponsorKeyCount,
                           -- StudentKeys
                           COUNT(CASE
                                     WHEN fct_rev.StudentKey < 0
                                     THEN StudentKey
                                 END) AS StudentKeyCount,
                           -- TierKeys
                           COUNT(CASE
                                     WHEN fct_rev.TierKey < 0
                                     THEN TierKey
                                 END) AS TierKeyCount,
                           -- TransactionCodeKey
                           COUNT(CASE
                                     WHEN fct_rev.TransactionCodeKey < 0
                                     THEN TransactionCodeKey
                                 END) AS TransactionCodeKeyCount,
                           -- TuitionAssistanceProviderKeys
                           COUNT(CASE
                                     WHEN fct_rev.TuitionAssistanceProviderKey < 0
                                     THEN TuitionAssistanceProviderKey
                                 END) AS TuitionAssistanceProviderKeyCount,
                           -- WebCampaignKey
                           NULL AS WebCampaignKeyCount,
					  -- EDWCreatedDate
                           GETDATE()
                    FROM dbo.FactNetRevenue fct_rev
                         LEFT JOIN dbo.DimDate dm_dt ON fct_rev.DateKey = dm_dt.DateKey
                    GROUP BY dm_dt.FiscalYearNumber,
                             fct_rev.SourceSystem
                    ORDER BY dm_dt.FiscalYearNumber,
                             fct_rev.SourceSystem;
             -- Log inserts

             SELECT @RowCount = @@ROWCOUNT;
             SELECT @InsertCount = @InsertCount + @RowCount;
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(VARCHAR(10), @RowCount)+' records for FactNetRevenue';
             PRINT @DebugMsg;

             -- Write the successful load to EDWAuditLog
             EXEC [dbo].[spEDWEndAuditLog]
                  @InsertCount = @InsertCount,
                  @UpdateCount = @UpdateCount,
                  @DeleteCount = @DeleteCount,
                  @SourceCount = @SourceCount,
                  @AuditId = @AuditId;
             --
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Completed.';
             PRINT @DebugMsg;
         END TRY
         BEGIN CATCH
             EXEC [dbo].[spEDWErrorAuditLog]
                  @AuditId = @AuditId;
             --
             -- Raiserror
             --	
             DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
             SELECT @ErrMsg = ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;
GO
