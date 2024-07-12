
	-- ================================================================================
	-- 
	-- Stored Procedure:	rep.sp_ReportQuery_DailyWebInquiriesAndTours
	--
	-- Purpose:				Extracts Count of Open Web Inquiries from Yesterday; Completed and Scheduled Tours of the Current Week
	--						Manipulates the date parameter by subtracting it by one day; Sets WeekendDate using the Date Parameter
	--							      However, we have
	--								 the population process encapsulated in a proc if we  
	--								 need to update or [re]deploy the entire database solution
	--								 from scratch.	  
	--
	--							The logic putting in sproc
	--								 makes it easier to locate what's actually populating the dataset				 		 					 
	--
	--
	-- Populates:			 Extracts Count of Open Web Inquiries from Yesterday; Completed and Scheduled Tours of the Current Week
	--
	-- Usage:				  EXEC [rep].[sp_ReportQuery_DailyWebInquiriesAndTours]
	--
	-- --------------------------------------------------------------------------------
	--
	-- Change Log:			
	-- ----------
	--
	-- Date			Modified By	  Comments
	-- ----			-----------	  --------
	--
	-- 09/18/18	    valimineti		 Created
	-- 10/09/18	    valimineti       BNG-3803 - Made changes in the DimDate join and added the needed filters like LeadCreatedBy and Lead Status
	-- 11-29-2018       Adevabhakthuni    BNG -512 updated the Stored porc to show Zero if there were no tours			 
	-- ================================================================================
CREATE PROCEDURE [rep].[sp_ReportQuery_DailyWebInquiriesAndToursCount](@Date DATE)
AS
     BEGIN
--Declare @Date Date
--SET @Date = '2018-11-24'
         DECLARE @WeekStartDate DATE;
         DECLARE @WeekEndDate DATE;
         SELECT @WeekStartDate = FiscalWeekStartDate,
                @WeekEndDate = FiscalWeekEndDate
         FROM DimDate
         WHERE FullDate = @Date;
         WITH CostCenterCTE
              AS (-- List of KCLC and KC@W Cost Centers by Region, District and Line of Business

              SELECT CostCenterKey,
                     OrgRegionName+COALESCE(' - '+NULLIF(OrgRegionLeaderName, 'No Region Leader'), '') AS CCHierarchyLevel4Name,
                     OrgDistrictName+COALESCE(' - '+NULLIF(OrgDistrictLeaderName, ''), '') AS CCHierarchyLevel5Name,
                     [Cost Center Number] AS CostCenterNumber,
                     CCTLineOfBusinessCode
              FROM model.vCostCenter AS CC
                   INNER JOIN DimOrganization AS ORG ON ORG.CostCenterNumber = CC.[Cost Center Number]
                                                        AND ORG.EDWEndDate IS NULL
                   INNER JOIN DimCostCenterType AS CCT ON CC.[Cost Center Type ID] = CCT.CostCenterTypeID
              WHERE [CC Classification] IN('Same', 'New')
                   AND CCTLineOfBusinessCode IN('KCLC', 'KCAW')
              AND (CCT.CCTFunctionName = 'CENTER'
                   OR CCT.CCTFunctionName = 'SPECIAL SERVICES'
                   OR CCT.CCTFunctionName = 'MANAGEMENT FEE')
             ),
              CompletedTourCTE
              AS ( -- Completed tours between Week Start Date and Selected Date

              SELECT DISTINCT
                     FLE.LeadEventID,
                     CostCenterKey,
                     FLE.EventDate
              FROM FactLeadEvent FLE
                   INNER JOIN DimLeadEventType DET ON FLE.LeadEventTypeKey = DET.LeadEventTypeKey
              WHERE DET.LeadEventTypeName = 'Completed Tour'
                    AND CONVERT(DATE, EventDate) BETWEEN @WeekStartDate AND DATEADD(DD, -1, @Date)),
              ScheduleTourCTE
              AS ( -- Scheduled tours between day after Selected Date and Week End Date

              SELECT DISTINCT
                     FLE.LeadEventID,
                     CostCenterKey,
                     FLE.EventDate
              FROM FactLeadEvent FLE
                   INNER JOIN DimLeadEventType DET ON FLE.LeadEventTypeKey = DET.LeadEventTypeKey
              WHERE DET.LeadEventTypeName = 'Scheduled Tour'
                    AND CONVERT(DATE, EventDate) BETWEEN @Date AND @WeekEndDate),
              InquiryCountCTE
              AS ( -- List of Web Inquiries from Previous day (including weekend dates if start of week)

              SELECT CC.CCHierarchyLevel4Name,
                     CC.CCHierarchyLevel5Name,
                     @Date AS FullDate,
                     CC.CCTLineOfBusinessCode,
                     (LE.LeadEventID)
              FROM [BING_EDW].[dbo].[FactLeadEvent] LE
                   INNER JOIN CostCenterCTE CC ON CC.CostCenterKey = LE.CostCenterKey
                   INNER JOIN DimLeadEventType ET ON LE.LeadEventTypeKey = ET.LeadEventTypeKey
                   INNER JOIN DimDate DD ON DD.FullDate = CONVERT(DATE, LE.EventDate)
                   INNER JOIN DimLead DL ON DL.LeadKey = LE.LeadKey
              WHERE [InquiryType] = 'Web'
                    AND DL.LeadStatus = 'Inquiry'
                    AND LE.LeadCreatedbyId IN('005A0000000RIy4IAG', '005A0000000RKHXIA4') -- SalesForceUsers: FB Admin and White Horse 
                   AND DD.FullDate BETWEEN CASE
                                               WHEN DATEPART(dw, @Date) = 1
                                               THEN DATEADD(dd, -2, @Date)
                                               WHEN DATEPART(dw, @Date) = 2
                                               THEN DATEADD(dd, -3, @Date)
                                               ELSE DATEADD(dd, -1, @Date)
                                           END AND DATEADD(dd, -1, @Date)),
              CTE
              AS (
              SELECT CC.CCHierarchyLevel4Name AS Region,
                     CC.CCHierarchyLevel5Name AS District,
                     COUNT(Comp.LeadEventID) AS CompletedTourCount,
                     0 AS ScheduledTourCount,
                     dd.FullDate,
                     'Comp' AS Type,
                     0 AS InquiryCount,
                     0 AS ISBrand
              FROM CostCenterCTE CC
                   JOIN DimDate DD ON DD.FullDate BETWEEN @WeekStartDate AND DATEADD(DD, -1, @Date)
                   LEFT JOIN CompletedTourCTE Comp ON Comp.CostCenterKey = CC.CostCenterKey
                                                      AND CONVERT(DATE, Comp.EventDate) = DD.FullDate
              GROUP BY CCTLineOfBusinessCode,
                       CC.CCHierarchyLevel4Name,
                       CC.CCHierarchyLevel5Name,
                       dd.FullDate
              UNION ALL
              SELECT CC.CCHierarchyLevel4Name AS Region,
                     CC.CCHierarchyLevel5Name AS District,
                     0 AS CompletedTourCount,
                     COUNT(Sched.LeadEventID) AS ScheduledTourCount,
                     dd.FullDate,
                     'Sched' AS Type,
                     0 AS InquiryCount,
                     0 AS ISBrand
              FROM CostCenterCTE CC
                   JOIN DimDate DD ON DD.FullDate BETWEEN @Date AND @WeekEndDate
                   LEFT JOIN ScheduleTourCTE Sched ON Sched.CostCenterKey = CC.CostCenterKey
                                                      AND CONVERT(DATE, Sched.EventDate) = DD.FullDate
              GROUP BY CCTLineOfBusinessCode,
                       CC.CCHierarchyLevel4Name,
                       CC.CCHierarchyLevel5Name,
                       dd.FullDate
              UNION ALL
              SELECT CC.CCHierarchyLevel4Name AS Region,
                     CC.CCHierarchyLevel5Name AS District,
                     0 AS CompletedTourCount,
                     0 AS ScheduledTourCount,
                     @Date AS FullDate,
                     'Inquiry' AS Type,
                     COUNT(LeadEventID) AS InquiryCount,
                     0 AS ISBrand
              FROM InquiryCountCTE CC
              GROUP BY CCTLineOfBusinessCode,
                       CC.CCHierarchyLevel4Name,
                       CC.CCHierarchyLevel5Name,
                       FullDate
              UNION ALL
    --Brand Section

              SELECT CCTLineOfBusinessCode AS Region,
                     CC.CCHierarchyLevel5Name AS District,
                     COUNT(Comp.LeadEventID) AS CompletedTourCount,
                     0 AS ScheduledTourCount,
                     dd.FullDate,
                     'Comp' AS Type,
                     0 AS InquiryCount,
                     1 AS ISBrand
              FROM CostCenterCTE CC
                   JOIN DimDate DD ON DD.FullDate BETWEEN @WeekStartDate AND DATEADD(DD, -1, @Date)
                   LEFT JOIN CompletedTourCTE Comp ON Comp.CostCenterKey = CC.CostCenterKey
                                                      AND CONVERT(DATE, Comp.EventDate) = DD.FullDate
              GROUP BY dd.FullDate,
                       CCTLineOfBusinessCode,
                       CCHierarchyLevel5Name
              UNION ALL
              SELECT CCTLineOfBusinessCode AS Region,
                     CC.CCHierarchyLevel5Name AS District,
                     0 AS CompletedTourCount,
                     COUNT(Sched.LeadEventID) AS ScheduledTourCount,
                     dd.FullDate,
                     'Sched' AS Type,
                     0 AS InquiryCount,
                     1 AS ISBrand
              FROM CostCenterCTE CC
                   JOIN DimDate DD ON DD.FullDate BETWEEN @Date AND @WeekEndDate
                   LEFT JOIN ScheduleTourCTE Sched ON Sched.CostCenterKey = CC.CostCenterKey
                                                      AND CONVERT(DATE, Sched.EventDate) = DD.FullDate
              GROUP BY dd.FullDate,
                       CCTLineOfBusinessCode,
                       CCHierarchyLevel5Name
              UNION ALL
              SELECT CCTLineOfBusinessCode AS Region,
                     CC.CCHierarchyLevel5Name AS District,
                     0 AS CompletedTourCount,
                     0 AS ScheduledTourCount,
                     @Date AS FullDate,
                     'Inquiry' AS Type,
                     COUNT(LeadEventID) AS InquiryCount,
                     1 AS ISBrand
              FROM InquiryCountCTE CC
              GROUP BY CCTLineOfBusinessCode,
                       CC.CCHierarchyLevel4Name,
                       CC.CCHierarchyLevel5Name,
                       FullDate)
              SELECT *
              FROM CTE
                   OUTER APPLY
              (
                  SELECT CCHierarchyLevel4Name,
                         COUNT(DISTINCT CostCenterNumber) AS RegionCenterCount
                  FROM CostCenterCTE CC
                  WHERE CTE.Region = CC.CCHierarchyLevel4Name
                  GROUP BY CCHierarchyLevel4Name
              ) X
                   OUTER APPLY
              (
                  SELECT CCHierarchyLevel5Name,
                         COUNT(DISTINCT CostCenterNumber) AS DistrictCenterCount
                  FROM CostCenterCTE CC
                  WHERE CTE.District = CC.CCHierarchyLevel5Name
                  GROUP BY CCHierarchyLevel5Name
              ) Y
                   OUTER APPLY
              (
                  SELECT CCTLineOfBusinessCode,
                         COUNT(DISTINCT CCT.CostCenterNumber) AS BrandCenterCount
                  FROM CostCenterCTE CCT
         --INNER JOIN CostCenterCTE CC ON CC.CostCenterTypeID = CCT.CostCenterTypeID 
                  WHERE CTE.Region = CCT.CCTLineOfBusinessCode
                  GROUP BY CCTLineOfBusinessCode
              ) Z
                   OUTER APPLY
              (
                  SELECT SUM(BrandCenterCount) AS TotalCenterCount
                  FROM
                  (
                      SELECT DISTINCT
                             Region
                      FROM CTE
                      WHERE isBrand = 1
                  ) a
                  INNER JOIN
                  (
                      SELECT CCTLineOfBusinessCode,
                             COUNT(DISTINCT CC.CostCenterNumber) AS BrandCenterCount
                      FROM CostCenterCTE CC
         --INNER JOIN CostCenterCTE CC ON CC.CostCenterTypeID = CCT.CostCenterTypeID 
                      GROUP BY CCTLineOfBusinessCode
                  ) abc ON a.Region = abc.CCTLineOfBusinessCode
              ) ZZ;
     END;