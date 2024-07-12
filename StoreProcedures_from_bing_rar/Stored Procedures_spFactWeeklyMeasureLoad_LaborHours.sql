﻿-- ================================================================================
    -- 
    -- Stored Procedure:   spFactWeeklyMeasureLoad_LaborHours
    --
    -- Purpose:            Inserting Dimesionkeys from DW_Landing.[LaborHoursLanding]
	--                     to main table DW_Mart.[FactWeeklyMeasure] table.
    --
    -- Parameters:		   n/a
    --
    -- Usage:              exec dbo.spFactWeeklyMeasureLoad_LaborHours @FicalWeek =20220306
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date				Modified By			Comments
    -- ----------		-----------			--------
    --
    -- 20220323			Aniket Navale 		BI-5652 - Initial version
    --			 
    -- ================================================================================
CREATE PROCEDURE [dbo].[spFactWeeklyMeasureLoad_LaborHours]
@FicalWeek int
AS
BEGIN
INSERT INTO [dbo].[FactWeeklyMeasure]
           ([CostCenterKey]
           ,[FiscalWeekKey]
           ,[MeasureKey]
           ,[DataScenarioKey]
           ,[SelectionValue1]
           ,[SelectionValue2]
           ,[SelectionValue3]
           ,[SelectionValue4]
           ,[PTDAmount1]
           ,[PTDAmount2]
           ,[PTDAmount3]
           ,[PTDAmount4]
           ,[QTDAmount1]
           ,[QTDAmount2]
           ,[QTDAmount3]
           ,[QTDAmount4]
           ,[YTDAmount1]
           ,[YTDAmount2]
           ,[YTDAmount3]
           ,[YTDAmount4]
           ,[SourceSystem]
           ,[DateTimeCreated]
           ,[DateTimeModified]
           )

SELECT 
      COALESCE(b.[CostCenterKey],-1)
      ,[FiscalWeekKey]
      ,COALESCE(d.[MeasureKey],-1)
      ,COALESCE(c.[DataScenarioKey],-1)
      ,[SelectionValue1]
      ,[SelectionValue2]
      ,[SelectionValue3]
      ,[SelectionValue4]
      ,[PTDAmount1]
      ,[PTDAmount2]
      ,[PTDAmount3]
      ,[PTDAmount4]
      ,[QTDAmount1]
      ,[QTDAmount2]
      ,[QTDAmount3]
      ,[QTDAmount4]
      ,[YTDAmount1]
      ,[YTDAmount2]
      ,[YTDAmount3]
      ,[YTDAmount4]
      ,[SourceSystem]
      ,[DateTimeCreated]
      ,[DateTimeModified]
	 
  FROM [DW_Landing].[dbo].[LaborHoursLanding] as a
left join [dbo].[DimCostCenter] as b on a.CostCenterNumber=b.costcenternumber and a.[FiscalWeekendDate] between EDWEffectiveFrom and EDWEffectiveTo
left join [dbo].[DimDataScenario] as c on a.[DataScenarioName]=c.[datascenarioname] 
left join [dbo].[DimMeasure] as d on a.[MeasureName]=d.measurename 
where a.FiscalWeekKey=@ficalweek

END
