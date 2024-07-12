
/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spMISC_StagingTransform_FactLaborHourTarget'
)
    DROP PROCEDURE dbo.spMISC_StagingTransform_FactLaborHourTarget;
GO
*/
CREATE PROCEDURE [dbo].[spMISC_StagingTransform_FactLaborHourTargetLegacy]
(@EDWRunDateTime    DATETIME2 = NULL,
 @FiscalWeekEndDate DATE
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spMISC_StagingTransform_FactLaborHourTargetLegacy
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
    --                     EXEC dbo.spMISC_StagingTransform_FactLaborHourTarget @FiscalWeekEndDate = '20171231'
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date          Modified By        Comments
    -- ----          -----------        --------
    --
    -- 07/31/18       valimineti          BNG-3373 - FactLaborTarget 
     --08/13/18       valimineti		  BNG-3503 - Add TSEF column in Fact Labor Hour
	 -- 01/21/20     Adevabhakthuni      Replace PersonKEy with Employeenumber 
	 -- 11/09/20     Adevabhakthuni      BI-3569 Updated the Transformation proc to get dollar extension and AF based on week selected 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;
	    --
	    -- Housekeeping Variables
	    -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
	    DECLARE @SourceSystem NVARCHAR(3)='LDW';
	    --
	    -- If we do not get an @EDWRunDateTime input, set to current date
	    --
         IF @EDWRunDateTime IS NULL
             SET @EDWRunDateTime = GETDATE();
	    --
	    -- Execute the extract / transform from the Staging database source
	    --
         BEGIN TRY
            SELECT '-2' AS EmployeeNumber
                  ,COALESCE(dm_cc.CostCenterKey, -1) AS CostCenterKey
                  ,COALESCE(dm_o.OrgKey, -1) AS OrgKey
                  ,COALESCE(dm_d.DateKey, -1) AS DateKey
                  ,COALESCE(dm_asa.AccountSubaccountKey, -1) AS AccountSubaccountKey
			   ,CASE WHEN LaborTargetDescription  ='TSEF' THEN 'TSEF' ELSE 'Not TSEF' END AS IsTSEF
                  ,'-2' AS PayBasisKey
                  ,COALESCE(dm_ds.DataScenarioKey, -1) AS DataScenarioKey
                  ,COALESCE(lh.[hours],0) AS [Hours]
				  ,'0.00' AS AdjustmentFactor
				  ,'0.00' AS DollarExtension
			   ,@SourceSystem AS SourceSystem
                  ,@EDWRunDateTime AS EDWCreatedDate
               FROM dbo.vLegacyLaborHoursTarget AS lh
                  LEFT JOIN BING_EDW.dbo.DimDate AS dm_d ON dm_d.FullDate = lh.[Date]
                  LEFT JOIN BING_EDW.dbo.DimCostCenter AS dm_cc ON dm_cc.CostCenterNumber = lh.CostCenter AND dm_cc.EDWEndDate IS NULL
                  LEFT JOIN BING_EDW.dbo.DimOrganization AS dm_o ON dm_o.CostCenterNumber = lh.CostCenter AND dm_o.EDWEndDate IS NULL
                  LEFT JOIN BING_EDW.dbo.DimAccountSubaccount AS dm_asa ON dm_asa.AccountSubaccountID = lh.AccountSubaccountId
                  LEFT JOIN BING_EDW.dbo.DimDataScenario AS dm_ds ON dm_ds.DataScenarioName = 'Target'
               WHERE dm_d.FullDate = @FiscalWeekEndDate;
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