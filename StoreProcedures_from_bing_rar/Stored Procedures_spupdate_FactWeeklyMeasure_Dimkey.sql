-- =============================================
-- Author:		<Jimmy Ji>
-- Create date: <03/03/2022>
-- Description:	<Update Lead Event dimention keys>
-- =============================================
CREATE PROCEDURE [dbo].[spupdate_FactWeeklyMeasure_Dimkey]
@ficalweek int
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
      coalesce(b.[CostCenterKey],-1)
      ,[FiscalWeekKey]
      ,coalesce(d.[MeasureKey],-1)
      ,coalesce(c.[DataScenarioKey],-1)
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
	 
  FROM [DW_Landing].[dbo].[FactWeeklyMeasureLanding] as a
left join [dbo].[dimcostcenter] as b on a.Costcenternumber=b.costcenternumber and a.[fiscalweekenddate] between EDWEffectiveFrom and EDWEffectiveTo
left join [dbo].[dimdatascenario] as c on a.[datascenarioname]=c.[datascenarioname] 
left join [dbo].[dimmeasure] as d on a.[measurename]=d.measurename 
where a.FiscalWeekKey=@ficalweek

END