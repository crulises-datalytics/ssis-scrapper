

CREATE PROCEDURE [rep].[sp_DataDrivenSubscription_DailyWebInquiry]
	AS
BEGIN
	-- ================================================================================
	-- 
	-- Stored Procedure:	rep.sp_DataDrivenSubscription_DailyWebInquiry
	--
	-- Purpose:				Outputs the Data Driven Subscription parameters for the 13 Week Trend Report.		 		 					 
	--
	--
	-- Populates:			 N/A
	--
	-- Usage:				  EXEC rep.sp_DataDrivenSubscription_DailyWebInquiry
	--
	-- --------------------------------------------------------------------------------
	--
	-- Change Log:			
	-- ----------
	--
	-- Date			Modified By	  Comments
	-- ----			-----------	  --------
	--
	-- 08/03/18     adevabhakthuni	   BNG-3500  Email Delivery of the Daily Web inquiry Response and Tour Performance Report
    -- 11/27/18     anmorales          Update procedure to reference the report parameter functions
    --
    -- ================================================================================
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    WITH CTEOrganizationOrgHierarchy AS (
       SELECT EmailAddress, OrganizationOrgHierarchy
		  FROM rep.fnOrganizationOrgHierarchy (1) 
		  WHERE CCClassification IN ('New', 'Same')
    ), CTEDateFiscalWeek AS (
       SELECT DateFiscalWeek 
	      FROM rep.fnDateFiscalWeek(0)
    )
    SELECT STUFF((
              SELECT '; ' + rsp.EmailAddress [text()]
                 FROM CTEOrganizationOrgHierarchy AS rsp
                 WHERE rsp.OrganizationOrgHierarchy = org.OrganizationOrgHierarchy
                 GROUP BY '; ' + rsp.EmailAddress
                    FOR XML PATH('')
           ), 1, 2, '') AS EmailTo
          ,NULL AS EmailCC
          ,NULL AS EmailBCC
		   ,'Normal' AS EmailPriority
		   ,' Daily WEB Inquiry Response & Tour Performance Report ' AS EmailSubject
		   ,' Hello, Attached is your Daily WEB Inquiry Response & Tour Performance Report.' AS EmailContent
          ,CAST(1 AS bit) AS IncludeLink
          ,CAST(1 AS bit) AS IncludeReport
          ,'PDF' AS RenderFormatIfIncludeReport
          ,OrganizationOrgHierarchy
          ,DateFiscalWeek
       FROM CTEOrganizationOrgHierarchy AS org
          CROSS JOIN CTEDateFiscalWeek AS wk
       GROUP BY OrganizationOrgHierarchy
          ,DateFiscalWeek;
END