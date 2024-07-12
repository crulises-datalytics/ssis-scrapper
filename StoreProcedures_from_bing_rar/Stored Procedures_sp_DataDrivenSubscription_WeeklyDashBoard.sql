

CREATE PROCEDURE [rep].[sp_DataDrivenSubscription_WeeklyDashboard] (
    @Ancestor tinyint = 0
   ,@PreviousPeriod tinyint = 0
   ,@TestEmail varchar(150) = NULL
)
	AS
BEGIN
	-- ================================================================================
	-- 
	-- Stored Procedure:	rep.sp_DataDrivenSubscription_WeeklyDashboard
	--
	-- Purpose:				Outputs the Data Driven Subscription parameters for the 
    --                      Weekly P&L Report.		 
    -- Parameters:
	--					@Ancestor <tinyint> = Org Hierarchy Ancestor
	--					@PreviousPeriod <tinyint> = Use a previous Date Period
	--					@TestEmail <varchar(150)> = Send all emails to a test email instead of an actual email		 					 
	--
	--
	-- Populates:			 N/A
	--
	-- Usage:				  EXEC rep.sp_DataDrivenSubscription_WeeklyDashBoard @Ancestor = 1, @PreviousPeriod = 1, @TestEmail = 'anquitta@kc-education.com'
	--
	-- --------------------------------------------------------------------------------
	--
	-- Change Log:			
	-- ----------
	--
	-- Date			Modified By	     Comments
	-- ----			-----------	      --------
	--
	-- 08/06/18        adevabhakthuni	BNG-3496 Create original data driven subscription.
   -- 11/27/18        anmorales        Update procedure to reference the report parameter functions
   -- 12/11/18        anquitta         BNG-4439 Update PersonFullName and EmailAddress references
	-- 1/18/19			  anquitta			BNG-4594 standardization of naming conventions
   -- ================================================================================
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    WITH CTEOrganizationOrgHierarchy AS (
       SELECT CASE WHEN NULLIF(PersonFullName, '') IS NOT NULL THEN QUOTENAME(PersonFullName, '"') + QUOTENAME(ISNULL(@TestEmail, EmailAddress), '<') ELSE ISNULL(@TestEmail, EmailAddress) END AS EmailAddress
	         ,OrganizationOrgHierarchy
		  FROM rep.fnOrganizationOrgHierarchy (@Ancestor) 
		  WHERE CCClassification IN ('New', 'Same')
    ), CTEDateFiscalWeek AS (
       SELECT DateFiscalWeek, CONVERT(varchar, FiscalWeekOfYearNumber) + ' of FY' + CONVERT(varchar, FiscalYearNumber) AS FiscalWeek
	      FROM rep.fnDateFiscalWeek(@PreviousPeriod)
    )
    SELECT REPLACE(REPLACE(STUFF((
              SELECT '; ' + rsp.EmailAddress [text()]
                 FROM CTEOrganizationOrgHierarchy AS rsp
                 WHERE rsp.OrganizationOrgHierarchy = org.OrganizationOrgHierarchy
                 GROUP BY '; ' + rsp.EmailAddress
                    FOR XML PATH('')
           ), 1, 2, ''), '&gt;', '>'), '&lt;', '<') AS EmailTo
          ,NULL AS EmailCC
          ,NULL AS EmailBCC
          ,'Normal' AS EmailPriority
		  ,'Weekly Dashboard for Week ' + FiscalWeek AS EmailSubject
		  ,' Hello,Attached is your Weekly Dashboard.' AS EmailContent
          ,CAST(1 AS bit) AS IncludeLink
          ,CAST(1 AS bit) AS IncludeReport
		  ,'PDF' AS RenderFormatIfIncludeReport
          ,org.OrganizationOrgHierarchy
          ,wk.DateFiscalWeek
       FROM CTEOrganizationOrgHierarchy AS org
          CROSS JOIN CTEDateFiscalWeek AS wk
       GROUP BY org.OrganizationOrgHierarchy
          , wk.DateFiscalWeek
		  , wk.FiscalWeek;
END
