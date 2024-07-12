

/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spCambridge_StagingTransform_FactFTESnapshot'
)
    DROP PROCEDURE dbo.spCambridge_StagingTransform_FactFTESnapshot;
GO
*/
CREATE PROCEDURE [dbo].[spCambridge_StagingTransform_FactFTESnapshot]
(@EDWRunDateTime DATETIME2 = NULL,
 @FiscalWeek     INT
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCambridge_StagingTransform_FactFTESnapshot
    --
    -- Purpose:            Performs the transformation logic with the source database
    --                         for a given Fact or Dimension table, and returns the
    --                         results set to the caller (usually for populating a
    --                         temporary table).
    --
    -- Parameters:         @EDWRunDateTime
    --                     @@FiscalDate - Proc will run for a given Fiscal Date 
    --
    -- Returns:            Results set containing the transformed data ready for
    --                         consumption by the ETL process for load into BING_EDW
    --
    -- Usage:              INSERT #FactFTESnapshotUpsert -- (Temporary table)
    --                     EXEC dbo.spCambridge_StagingTransform_FactFTESnapshot @FiscalWeek = 20161125 
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date          Modified By        Comments
    -- --------      -----------        --------
    --
    --  1/29/18      sburke             BNG-294 - Refactored EDW FactFTESnapshot (Cambridge Source) load
	--  03/08/2018   Adevabhakthuni     Updated sp to reference new fuction tfnFTE 
    --
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;
	    --
	    -- Housekeeping Variables
	    -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceSystem NVARCHAR(3)= 'PRO';
	    --
	    -- If we do not get an @EDWRunDateTime input, set to current date
	    --
         IF @EDWRunDateTime IS NULL
             SET @EDWRunDateTime = GETDATE();
	    --
	    -- Execute the extract / transform from the Staging database source
	    --
         BEGIN TRY
             SELECT COALESCE(dm_dt.DateKey, -1) AS DateKey,
                    COALESCE(dm_org.OrgKey, -1) AS OrgKey,
                    COALESCE(dm_loc.LocationKey, -1) AS LocationKey,
                    -2 AS StudentKey,
                    -2 AS SponsorKey,
                    -2 AS TuitionAssistanceProviderKey,
                    COALESCE(dm_cmp.CompanyKey, -1) AS CompanyKey,
                    COALESCE(dm_cctyp.CostCenterTypeKey, -1) AS CostCenterTypeKey,
                    COALESCE(dm_cctr.CostCenterKey, -1) AS CostCenterKey,
                    COALESCE(dm_accsa.AccountSubaccountKey, -1) AS AccountSubaccountKey,
                    -2 AS TransactionCodeKey,
                    -2 AS TierKey,
                    -2 AS ProgramKey,
                    -2 AS SessionKey,
                    -2 AS ScheduleWeekKey,
                    -2 AS ClassroomKey,
                    -2 AS FeeTypeKey,
                    -2 AS LifecycleStatusKey,
				    fct_fte.ReferenceID AS ReferenceID,
                    COALESCE(fct_fte.FTE, 0) AS FTE,
                    @SourceSystem AS SourceSystem,
                    @EDWRunDateTime AS EDWCreatedDate
             FROM [dbo].[tfnFTE](@FiscalWeek) fct_fte
                  LEFT JOIN BING_EDW.dbo.DimDate dm_dt ON fct_fte.FiscalDate = dm_dt.FullDate
                  LEFT JOIN BING_EDW.dbo.DimCostCenter dm_cctr ON fct_fte.CostCenterID = dm_cctr.CostCenterNumber
                                                                  AND dm_cctr.EDWEndDate IS NULL -- DimCostCenter is SCD2, so get the latest version
                  LEFT JOIN BING_EDW.dbo.DimCostCenterType dm_cctyp ON dm_cctr.CostCenterTypeID = dm_cctyp.CostCenterTypeID
                  LEFT JOIN BING_EDW.dbo.DimOrganization dm_org ON dm_cctr.CostCenterNumber = dm_org.CostCenterNumber
                                                                   AND dm_org.EDWEndDate IS NULL -- DimOrganization is SCD2, so get the latest version
                  LEFT JOIN BING_EDW.dbo.DimCompany dm_cmp ON dm_cctr.CompanyID = dm_cmp.CompanyID
                  LEFT JOIN BING_EDW.dbo.DimLocation dm_loc ON dm_org.DefaultLocationID = dm_loc.LocationID
                                                               AND dm_loc.EDWEndDate IS NULL -- DimLocation is SCD2, so get the latest version
                  LEFT JOIN BING_EDW.dbo.DimAccountSubaccount dm_accsa ON fct_fte.AccountID = dm_accsa.AccountID
                                                                          AND fct_fte.SubaccountID = dm_accsa.SubaccountID
																		  WHERE dm_dt.FiscalWeekNumber < 202109;
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