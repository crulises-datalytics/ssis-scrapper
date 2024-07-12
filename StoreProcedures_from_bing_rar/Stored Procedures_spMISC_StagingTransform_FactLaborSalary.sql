CREATE PROCEDURE [dbo].[spMISC_StagingTransform_FactLaborSalary]
(@EDWRunDateTime DATETIME2 = NULL,
 @PayRollDate    DATE
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spMISC_StagingTransform_FactLaborSalary
    --
    -- Purpose:            Performs the transformation logic with the source database
    --                         for a given Fact or Dimension table, and returns the
    --                         results set to the caller (usually for populating a
    --                         temporary table).
    --
    -- Parameters:         @EDWRunDateTime
	--                     @FiscalWeekEndDate - Proc will run for a given FiscalWeekEndDate 
    --
    -- Returns:            Results set containing the transformed data ready for
    --                         consumption by the ETL process for load into BING_EDW
    --
    -- Usage:              INSERT #FactLaborHourUpsert -- (Temporary table)
    --                     EXEC dbo.spMISC_StagingTransform_FactLaborSalary @FiscalWeekEndDate = '20171231'
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date          Modified By        Comments
    -- ----          -----------        --------
    --
    --  07/31/2018   Adevabhakthuni     BNG-1749 StagingToEDW FactLaborSalary
	--  04/22/2019   hhebbalu           BI-851  Sourcesystem is not defined - Added the SourceSystem Column
	--  07/17/2019   hhebbalu           BI-1339 We're passing weekend date as parameter but the procedure was 
	--									written to accept daily date. Fixed that to Weekend date. 
	--									Also mapped to FiscalweekendDatekey where Payroll is between 
	--									FiscalweekStartDate and FiscalWeekendDate
	-- 01/21/20     Adevabhakthuni      Removed the DimPerson Join to avoid Duplicates
    -- ===========================================================================================================
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
            SELECT COALESCE(dm_dt_pyrll.FiscalWeekEndDatekey, -1) AS DateKey,
                    COALESCE(fct_lbr_cst.FileNumber, '-1') AS EmployeeNumber,
                    COALESCE(dm_acc_sb.AccountSubaccountKey, -1) AS AccountSubaccountKey,
                    COALESCE(dm_cctr.CostCenterKey, -1) AS CostCenterKey,
                    COALESCE(dm_cctyp.CostCenterTypeKey, -1) AS CostCenterTypeKey,
                    COALESCE(dm_cmp.CompanyKey, -1) AS CompanyKey,
					COALESCE(dm_o.OrgKey, -1) AS OrgKey,
                    COALESCE(dm_pyrl_typ.PayrollTypeKey, -1) AS PayrollTypeKey, -- This will denote whether it is Checking, 401k, Taxes.  Will create new Dimension table for this.
                    CONVERT(NUMERIC(19, 4), ISNULL(fct_lbr_cst.CreditAmount, 0)) AS CreditAmount,
                    CONVERT(NUMERIC(19, 4), ISNULL(fct_lbr_cst.DebitAmount, 0)) AS DebitAmount,
                    CONVERT(NUMERIC(19, 4), ISNULL(fct_lbr_cst.DebitAmount, 0)) - CONVERT(NUMERIC(19, 4), ISNULL(fct_lbr_cst.CreditAmount, 0)) AS NetAmount,
					'ADP' AS SourceSystem,
                    @EDWRunDateTime AS EDWCreatedDate
             FROM dbo.vLaborHoursCosting fct_lbr_cst
                  LEFT JOIN BING_EDW..DimDate dm_dt_acnt ON fct_lbr_cst.AccountingDate = dm_dt_acnt.FullDate
                  --LEFT JOIN BING_EDW..DimDate dm_dt_pyrll ON fct_lbr_cst.PayrollDate = dm_dt_pyrll.FullDate
				  LEFT JOIN BING_EDW.dbo.vDimFiscalWeek AS dm_dt_pyrll ON fct_lbr_cst.PayrollDate BETWEEN dm_dt_pyrll.FiscalWeekStartDate AND dm_dt_pyrll.FiscalWeekEndDate
                  LEFT JOIN BING_EDW..DimAccountSubaccount dm_acc_sb ON fct_lbr_cst.Account = dm_acc_sb.AccountID and fct_lbr_cst.SubAccount=dm_acc_sb.SubaccountID
                  LEFT JOIN BING_EDW.dbo.DimCostCenter dm_cctr ON fct_lbr_cst.CostCenter = dm_cctr.CostCenterNumber
                                                                  AND dm_cctr.EDWEndDate IS NULL -- DimCostCenter is SCD2, so get the latest version
				  LEFT JOIN BING_EDW.dbo.DimOrganization AS dm_o ON dm_o.CostCenterNumber = fct_lbr_cst.CostCenter AND dm_o.EDWEndDate IS NULL
                  LEFT JOIN BING_EDW.dbo.DimCostCenterType dm_cctyp ON dm_cctr.CostCenterTypeID = dm_cctyp.CostCenterTypeID
                  LEFT JOIN BING_EDW.dbo.DimCompany dm_cmp ON dm_cctr.CompanyID = dm_cmp.CompanyID
                 -- LEFT JOIN BING_EDW.dbo.DimPerson dm_psn ON fct_lbr_cst.FileNumber = dm_psn.EmployeeNumber
                           --                                  AND PersonCurrentRecordFlag = 'Y' /* Removed the Join to avoid Duplicates*/
                  LEFT JOIN BING_EDW.dbo.DimPayrollType dm_pyrl_typ ON fct_lbr_cst.Code = dm_pyrl_typ.PayrollCode
                                                                       AND fct_lbr_cst.PayElement = dm_pyrl_typ.PayrollElement
                                                                       AND fct_lbr_cst.PayElement = dm_pyrl_typ.PayrollElement
			 WHERE dm_dt_pyrll.FiscalWeekEndDate = @PayrollDate AND dm_dt_pyrll.FiscalPeriodType <> 'Adjustment';
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