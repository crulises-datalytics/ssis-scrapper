CREATE PROCEDURE [rep].[sp_DataDrivenSubscription_MultiUnitPeriodicPandL]
(@Ancestor       TINYINT      = 0,
 @PreviousPeriod TINYINT      = 0,
 @TestEmail      VARCHAR(150) = NULL
)
AS
     BEGIN
	-- ================================================================================
	-- 
	-- Stored Procedure:	rep.sp_DataDrivenSubscription_MultiUnitPeriodicPandL
	--
	-- Purpose:				Outputs the Data Driven Subscription parameters for the 
   --                              MultiUnitPeriodicPandL Report.	
    -- Parameters:
	--					@Ancestor <tinyint> = Org Hierarchy Ancestor
	--					@PreviousPeriod <tinyint> = Use a previous Date Period
	--					@TestEmail <varchar(150)> = Send all emails to a test email instead of an actual email	 		 					 
	--
	--
	-- Populates:			 N/A
	--
	-- Usage:				 EXEC rep.sp_DataDrivenSubscription_MultiUnitPeriodicPandL
	--
	-- --------------------------------------------------------------------------------
	--
	-- Change Log:			
	-- ----------
	--
	-- Date			Modified By	  Comments
	-- ----			-----------	  --------
	--
	-- 08/03/18     Banandesi	  BNG-3504 Create original data driven subscription.
   -- 11/27/18     anmorales     Update procedure to reference the report parameter functions
   -- 12/10/18		 anquitta	   small update on column names in rep.fnOrganizationOrgHierarchy
	-- 12/10/2018   sburke          Adding FiscalPeriod to the final GROUP BY to correct runtime error			 
	-- ================================================================================
         SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
         WITH CTEOrganizationOrgHierarchy
              AS (
              SELECT CASE
                         WHEN NULLIF(PersonFullName, '') IS NOT NULL
                         THEN QUOTENAME(PersonFullName, '"')+QUOTENAME(ISNULL(@TestEmail, EmailAddress), '<')
                         ELSE ISNULL(@TestEmail, EmailAddress)
                     END AS EmailAddress,
                     OrganizationOrgHierarchy
              FROM rep.fnOrganizationOrgHierarchy(@Ancestor)
              WHERE CCClassification IN('New', 'Same')),
              CTEDateFiscalPeriod
              AS (
              SELECT DateFiscalPeriod,
                     'P'+CONVERT(VARCHAR, FiscalPeriodOfYearNumber)+' of YR'+CONVERT(VARCHAR, FiscalYearNumber) AS FiscalPeriod
              FROM rep.fnDateFiscalPeriod(@PreviousPeriod))
              SELECT REPLACE(REPLACE(STUFF(
                                          (
                                              SELECT '; '+rsp.EmailAddress [text()]
                                              FROM CTEOrganizationOrgHierarchy AS rsp
                                              WHERE rsp.OrganizationOrgHierarchy = org.OrganizationOrgHierarchy
                                              GROUP BY '; '+rsp.EmailAddress FOR XML PATH('')
                                          ), 1, 2, ''), '&gt;', '>'), '&lt;', '<') AS EmailTo,
                     NULL AS EmailCC,
                     NULL AS EmailBCC,
                     'Normal' AS EmailPriority,
                     FiscalPeriod+' Multi Unit Periodic P&L' AS EmailSubject,
                     'Hello, Attached is your '+FiscalPeriod+' Multi Unit Periodic P&L Report.' AS EmailContent,
                     CAST(1 AS BIT) AS IncludeLink,
                     CAST(1 AS BIT) AS IncludeReport,
                     'PDF' AS RenderFormatIfIncludeReport,
                     OrganizationOrgHierarchy,
                     DateFiscalPeriod
              FROM CTEOrganizationOrgHierarchy AS org
                   CROSS JOIN CTEDateFiscalPeriod AS pd
              GROUP BY OrganizationOrgHierarchy,
                       DateFiscalPeriod,
				   FiscalPeriod;
     END;
GO

