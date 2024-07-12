	-- ================================================================================
	-- 
	-- Stored Procedure:	rep.sp_ReportQuery_DailyWebInquiriesAndToursDetail
	--
	-- Purpose:				Extracts Details of Open Web Inquiries from Yesterday;
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
	-- Populates:			 Extracts Details of Open Web Inquiries from Yesterday; 
	--
	-- Usage:				  EXEC [rep].[sp_ReportQuery_DailyWebInquiriesAndToursDetail]
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
	-- 10/08/18        valimineti       Modified - Added Lead Created By ID and Lead Status Filters
	--			 
	-- ================================================================================
CREATE PROCEDURE [rep].[sp_ReportQuery_DailyWebInquiriesAndToursDetail](@Date DATE)
AS
     BEGIN
         WITH Timezone
              AS (
              SELECT LocationNumber,
                     TimeZoneName,
                     UtcDate = SWITCHOFFSET(CONVERT(DATETIMEOFFSET(0), GETUTCDATE()),
                                                                                   CASE
                                                                                       WHEN GETUTCDATE() BETWEEN Salesforce_Staging.dbo.fnGetDSTStart(YEAR(GETUTCDATE())) AND Salesforce_Staging.dbo.fnGetDSTEnd(YEAR(GETUTCDATE()))
                                                                                       THEN COALESCE(UTCDSTOffset, -7)
                                                                                       ELSE COALESCE(UTCOffset, -8)
                                                                                   END * 60),
                     UtcDate2 = SWITCHOFFSET(CONVERT(DATETIMEOFFSET(0), GETUTCDATE()),
                                                                                    CASE
                                                                                        WHEN GETUTCDATE() BETWEEN Salesforce_Staging.dbo.fnGetDSTStart(YEAR(GETUTCDATE())) AND Salesforce_Staging.dbo.fnGetDSTEnd(YEAR(GETUTCDATE()))
                                                                                        THEN  -7
                                                                                         ELSE -8
                                                                                    END * 60),
                     UTCOffset,
                     UTCDSTOffset
              FROM Salesforce_Staging.dbo.vCenterTimeZones),
              CostCenterCTE
              AS (-- List of KCLC and KC@W Cost Centers by Region, District and Line of Business

              SELECT CostCenterKey,
                     OrgRegionName+COALESCE(' - '+NULLIF(OrgRegionLeaderName, 'No Region Leader'), '') AS CCHierarchyLevel4Name,
                     OrgDistrictName+COALESCE(' - '+NULLIF(OrgDistrictLeaderName, ''), '') AS CCHierarchyLevel5Name,
                     [Cost Center Number]+COALESCE(' - '+NULLIF(OrgCenterLeaderName, ''), '') AS CostCenterNumber,
                     [Cost Center Number],
                     CCTLineOfBusinessCode
              FROM model.vCostCenter AS CC
                   JOIN DimOrganization AS ORG ON ORG.CostCenterNumber = CC.[Cost Center Number]
                                                  AND ORG.EDWEndDate IS NULL
                   JOIN DimCostCenterType AS CCT ON CC.[Cost Center Type ID] = CCT.CostCenterTypeID
              WHERE [CC Classification] IN('Same', 'New')
                   AND CCTLineOfBusinessCode IN('KCLC', 'KCAW')
              AND OrgDivisionName = 'KinderCare Field'
              AND OrgTypeName = 'Center'
              AND OrgDistrictName LIKE 'R0%')
              SELECT CC.CCHierarchyLevel4Name AS Region,
                     CC.CCHierarchyLevel5Name AS District,
                     CostCenterNumber AS Center,
                     DL.LeadContact,
                     DL.InquiryType,
                     DL.[LeadPhone],
                     DL.[LeadEmail],
                     CAST(LE.EventDate AS DATE) AS InquiryDate,
                     ROUND(DATEDIFF(ss, SWITCHOFFSET(CAST(LE.EventDate AS DATETIMEOFFSET), '+00:00'), COALESCE(TZ.UtcDate,TZ.UtcDate2))/3600.0, 2) AS TimeInThisStatus
              FROM [BING_EDW].[dbo].[FactLeadEvent] LE
                   INNER JOIN CostCenterCTE CC ON LE.CostCenterKey = CC.CostCenterKey
                   INNER JOIN DimLeadEventType ET ON LE.LeadEventTypeKey = ET.LeadEventTypeKey
                   INNER JOIN DimDate DD ON DD.FullDate = CONVERT(DATE, LE.EventDate)
                   INNER JOIN DimLead DL ON DL.LeadKey = LE.LeadKey
                   LEFT JOIN Timezone TZ ON TZ.LocationNumber = CC.[Cost Center Number]
              WHERE [InquiryType] = 'Web'
                    AND DL.LeadStatus = 'Inquiry'
                    AND LE.LeadCreatedbyId IN('005A0000000RIy4IAG', '005A0000000RKHXIA4') -- SalesForceUsers: FB Admin and White Horse 
                   AND DD.FullDate BETWEEN CASE
                                               WHEN DATEPART(dw, @Date) = 1
                                               THEN DATEADD(dd, -2, @Date)
                                               WHEN DATEPART(dw, @Date) = 2
                                               THEN DATEADD(dd, -3, @Date)
                                               ELSE DATEADD(dd, -1, @Date)
                                           END AND DATEADD(dd, -1, @Date);
     END;