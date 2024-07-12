CREATE PROCEDURE [dbo].[spGL_StagingTransform_FactCenterStatSnapshot]
(@EDWRunDateTime    DATETIME2 = NULL,
 @FiscalWeekEndDate DATE
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spGL_StagingTransform_FactCenterStatSnapshot
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
    -- Usage:              INSERT #TemplateUpsert -- (Temporary table)
    --                     EXEC dbo.spGL_StagingTransform_FactCenterStatSnapshot @FiscalWeekEndDate = '5/6/17'
    -- 
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date           Modified By     Comments
    -- ----------     -----------     --------
    --
    --  1/02/2018     sburke          BNG-254 - Initial version
	--  11/13/2018    hhebbalu        BNG-4422 - COALESCE value for measures was -1, changed to 0
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
             SELECT COALESCE(dm_dt.DateKey, -1) AS FiscalWeekEndDateKey,
                    COALESCE(dm_org.OrgKey, -1) AS OrgKey,
                    COALESCE(dm_loc.LocationKey, -1) AS LocationKey,
                    COALESCE(dm_cmp.CompanyKey, -1) AS CompanyKey,
                    COALESCE(dm_cctyp.CostCenterTypeKey, -1) AS CostCenterTypeKey,
                    COALESCE(dm_cctr.CostCenterKey, -1) AS CostCenterKey,
                    COALESCE(dm_scn.DataScenarioKey, -1) AS DataScenarioKey,
                    COALESCE(ctr_stat.LicensedCapacity, 0) AS LicensedCapacity,
                    COALESCE(ctr_stat.BuildingCapacity, 0) AS BuildingCapacity,
                    COALESCE(ctr_stat.OperationalCapacity, 0) AS OperationalCapacity,
                    COALESCE(ctr_stat.SourceSystem, 'UNK') AS SourceSystem,
                    @EDWRunDateTime AS EDWCreatedDate
             FROM dbo.tfnGL_StagingGenerate_CenterStats_FactCenterStatSnapshot(@FiscalWeekEndDate) ctr_stat
                  LEFT JOIN BING_EDW.dbo.DimDate dm_dt ON ctr_stat.FiscalWeekEndDateKey = dm_dt.DateKey
                  LEFT JOIN BING_EDW.dbo.DimCostCenter dm_cctr ON ctr_stat.CostCenterNumber = dm_cctr.CostCenterNumber
                                                                  AND EDWEndDate IS NULL -- DimCostCenter is SCD2, so get current record;
                  LEFT JOIN BING_EDW.dbo.DimCostCenterType dm_cctyp ON dm_cctr.CostCenterTypeID = dm_cctyp.CostCenterTypeID
                  LEFT JOIN BING_EDW.dbo.DimOrganization dm_org ON dm_cctr.CostCenterNumber = dm_org.CostCenterNumber
                                                                   AND dm_org.EDWEndDate IS NULL -- DimOrganization is SCD2, so get current record;
                  LEFT JOIN BING_EDW.dbo.DimLocation dm_loc ON dm_org.DefaultLocationID = dm_loc.LocationID
                                                               AND dm_loc.EDWEndDate IS NULL -- DimLocation is SCD2, so get current record;
                  LEFT JOIN BING_EDW.dbo.DimCompany dm_cmp ON dm_cctr.CompanyID = dm_cmp.CompanyID
                  LEFT JOIN BING_EDW.dbo.DimDataScenario dm_scn ON ctr_stat.GLActualFlag = dm_scn.GLActualFlag
                                                                   AND (ctr_stat.GLBudgetVersionID = dm_scn.GLBudgetVersionID
                                                                        OR (ctr_stat.GLBudgetVersionID IS NULL
                                                                            AND dm_scn.GLBudgetVersionID IS NULL));
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
