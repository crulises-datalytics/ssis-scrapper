

CREATE PROCEDURE [dbo].[spMISC_StagingTransform_FactLaborHourActual]
(@EDWRunDateTime    DATETIME2 = NULL, 
 @FiscalWeekEndDate DATE,
 @RefDate Date
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spMISC_StagingTransform_FactLaborHourActual
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
    --                     EXEC dbo.spMISC_StagingTransform_FactLaborHourActual @FiscalWeekEndDate = '20171231'
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date          Modified By        Comments
    -- ----          -----------        --------
    --
    -- 6/29/18       anmorales          BNG-1747 - Labor hour mapping - Write Logic that does look up for Fact table load
    --04/22/19       hhebbalu           BI-851 - SourceSystem is not defined - Added the SourceSystem Column
    --07/17/19       hhebbalu           BI-1664 - THe requirement is to join on WeekendingDate(which is a WeekendDate) 
    --									and not on the DateWorked(daily date). Fixed that. An bringing FiscalweekendDateKey
    -- 01/21/20     Adevabhakthuni      Removed the DimPerson Join to avoid Duplicates
	-- 03/05/20     Adevabhakthuni      BI-3569 Updated the Transformation proc to get dollar extension and AF based on week selected 
	--12/29/2020    Adevabhakthuni      BI-4394 updated the stored proc to use weekened date
    -- ========================================================================================================
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
     

        -- get AdjustmentFactor  based on current week
        DECLARE @AF1 NUMERIC(19, 4);
        DECLARE @AF2 NUMERIC(19, 4);
        DECLARE @AF3 NUMERIC(19, 4);
        DECLARE @AF4 NUMERIC(19, 4);
			Declare  @ReferenceDate Date;
	   --get the fiscalweekending date for @Refdate
		Set @ReferenceDate = (Select Distinct FiscalWeekEndDate from BING_EDW..DimDate Where Fulldate= @RefDate);
        DECLARE @RW INT;
        SELECT @AF1 = AdjustmentFactor1, 
               @AF2 = AdjustmentFactor2, 
               @AF3 = AdjustmentFactor3,   
               @AF4 = AdjustmentFactor4
        FROM LaborAdjustmentFactor
        WHERE  WeekEndingDate = @ReferenceDate ;
        DROP TABLE IF EXISTS #FiscalWeekenddate;
        CREATE TABLE #FiscalWeekenddate(RefFiscalWeekEndDate DATE NOT NULL);
     
	 IF DATENAME(WEEKDAY, @RefDate) <> 'Saturday'
            BEGIN
                INSERT INTO #FiscalWeekenddate
                       SELECT DISTINCT TOP 1 FiscalWeekEndDate
                       FROM BING_EDW..DimDate
                       WHERE FullDate<= @RefDate
                       ORDER BY FiscalWeekEndDate DESC;
        END;
            ELSE
            BEGIN
                INSERT INTO #FiscalWeekenddate
                       SELECT DISTINCT Top 4 FiscalWeekEndDate
                       FROM BING_EDW..DimDate
                       WHERE FullDate<=@RefDate
                       ORDER BY FiscalWeekEndDate DESC;
        END;
        WITH CTEA
             AS (SELECT RefFiscalWeekEndDate, 
                        ROW_NUMBER() OVER(
                        ORDER BY RefFiscalWeekEndDate DESC) AS RW
                 FROM #FiscalWeekenddate)
             SELECT @RW = Rw
             FROM CTEA
             WHERE RefFiscalWeekEndDate = @FiscalWeekEndDate;

          -- Execute the extract / transform from the Staging database source
  BEGIN TRY
            SELECT COALESCE(EmployeeNumber, '-1') AS EmployeeNumber, 
                   COALESCE(dm_cc.CostCenterKey, -1) AS CostCenterKey, 
                   COALESCE(dm_o.OrgKey, -1) AS OrgKey, 
                   COALESCE(dm_f.FiscalWeekEndDateKey, -1) AS DateKey, 
                   COALESCE(dm_asa.AccountSubaccountKey, -1) AS AccountSubaccountKey, 
                   COALESCE(dm_pb.PayBasisKey, -1) AS PayBasisKey, 
                   COALESCE(dm_ds.DataScenarioKey, -1) AS DataScenarioKey, 
                   lh.TotalHrs AS Hours,
                   CASE
                       WHEN @RW = 1
                       THEN @AF1
                       WHEN @RW = 2
                       THEN @AF2
                       WHEN @RW = 3
                       THEN @AF3
                       WHEN @RW = 4
                       THEN @AF4
                       WHEN @RW IS NULL
                       THEN @AF4
                   END AS AdjustmentFactor, 
                   DollarExtension, 
                   'ADP' AS SourceSystem, 
                   GETDATE() AS EDWCreatedDate
            FROM dbo.vLaborHoursActuals AS lh
                 --LEFT JOIN BING_EDW.dbo.DimDate AS dm_d ON dm_d.FullDate = lh.WeekEndingDate
                 LEFT JOIN BING_EDW.dbo.vDimFiscalWeek AS dm_f ON lh.WeekEndingDate BETWEEN dm_f.FiscalWeekStartDate AND dm_f.FiscalWeekEndDate --BI-1664
                 --  LEFT JOIN BING_EDW.dbo.DimPerson AS dm_p ON dm_p.EmployeeNumber = lh.EmployeeNumber AND lh.DateWorked BETWEEN dm_p.PersonEffectiveDate AND dm_p.PersonEndDate /* Removed the Join to avoid Duplicates*/
                 LEFT JOIN BING_EDW.dbo.DimCostCenter AS dm_cc ON dm_cc.CostCenterNumber = lh.CostCenter
                                                                  AND dm_cc.EDWEndDate IS NULL
                 LEFT JOIN BING_EDW.dbo.DimOrganization AS dm_o ON dm_o.CostCenterNumber = lh.CostCenter
                                                                   AND dm_o.EDWEndDate IS NULL
                 LEFT JOIN BING_EDW.dbo.DimAccountSubaccount AS dm_asa ON dm_asa.AccountID = lh.Account
                                                                          AND dm_asa.SubaccountID = lh.Subaccount
                 LEFT JOIN BING_EDW.dbo.DimPayBasis AS dm_pb ON dm_pb.PayBasisName = lh.PayBasis
                 LEFT JOIN BING_EDW.dbo.DimDataScenario AS dm_ds ON dm_ds.DataScenarioName = 'Actual'
            WHERE dm_f.FiscalWeekEndDate = @FiscalWeekEndDate
                  AND dm_f.FiscalPeriodType <> 'Adjustment';
            DROP TABLE IF EXISTS #FiscalWeekenddate;
        END TRY

        --
        -- Catch, and throw the error back to the calling procedure or client
        --
        BEGIN CATCH
            DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
            SELECT @ErrMsg = 'Sub-procedure ' + @ProcName + ' - ' + ERROR_MESSAGE(), 
                   @ErrSeverity = ERROR_SEVERITY();
            RAISERROR(@ErrMsg, @ErrSeverity, 1);
        END CATCH;
    END;
GO


