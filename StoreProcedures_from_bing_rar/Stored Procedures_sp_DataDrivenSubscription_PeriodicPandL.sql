
CREATE PROCEDURE [rep].[sp_DataDrivenSubscription_PeriodicPandL] (
    @Ancestor tinyint = 0
   ,@PreviousPeriod tinyint = 0
   ,@TestEmail varchar(150) = NULL
)
	AS
BEGIN
	-- ================================================================================
	-- 
	-- Stored Procedure:	rep.sp_DataDrivenSubscription_PeriodicPandL
	--
	-- Purpose:				Outputs the Data Driven Subscription parameters for the Periodic P&L Week Trend Report.	
    -- Parameters:
	--					@Ancestor <tinyint> = Org Hierarchy Ancestor
	--					@PreviousPeriod <tinyint> = Use a previous Date Period
	--					@TestEmail <varchar(150)> = Send all emails to a test email instead of an actual email	 		 					 
	--
	--
	-- Populates:			 N/A
	--
	-- Usage:				  EXEC rep.sp_DataDrivenSubscription_PeriodicPandL @Ancestor = 1, @PreviousPeriod = 1, @TestEmail = 'anquitta@kc-education.com'
	--
	-- --------------------------------------------------------------------------------
	--
	-- Change Log:			
	-- ----------
	--
	-- Date			Modified By	  Comments
	-- ----			-----------	  --------
	--
	-- 08/06/18     hhebbalu		 
    -- 11/27/18     anmorales       Update procedure to reference the report parameter functions
	-- 12/11/18		anquitta		BNG-4439, small adjustment to grouping, update PersonFullName and EmailAddress references and names
	-- 1/18/19		anquitta		BNG-4594, standardize procedure naming convention
	-- ================================================================================
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	
    WITH CTEOrganizationOrgHierarchy AS (
		SELECT	CASE WHEN NULLIF(PersonFullName, '') IS NOT NULL THEN QUOTENAME(PersonFullName, '"') + QUOTENAME(ISNULL(@TestEmail, EmailAddress), '<') ELSE ISNULL(@TestEmail, EmailAddress) END AS EmailAddress
				,OrganizationOrgHierarchy
		FROM	rep.fnOrganizationOrgHierarchy (@Ancestor) 
		WHERE	CCClassification IN ('New', 'Same')
    ), CTEDateFiscalPeriod AS (
		SELECT	DateFiscalPeriod 
				, RIGHT('00' + CONVERT(varchar, FiscalPeriodOfYearNumber), 2) + ' of YR' + CONVERT(varchar, FiscalYearNumber) AS FiscalPeriod
		FROM rep.fnDateFiscalPeriod(@PreviousPeriod)
    )
    SELECT	REPLACE(REPLACE(STUFF((
				SELECT	'; ' + rsp.EmailAddress [text()]
				FROM	CTEOrganizationOrgHierarchy AS rsp
				WHERE	rsp.OrganizationOrgHierarchy = org.OrganizationOrgHierarchy
				GROUP BY '; ' + rsp.EmailAddress
                    FOR XML PATH('')
				), 1, 2, ''), '&gt;', '>'), '&lt;', '<') AS EmailTo
			,NULL AS EmailCC
			,NULL AS EmailBCC
			,'Normal' AS EmailPriority
			,'P' + FiscalPeriod + ' Periodic P&L Report' AS EmailSubject
			,'Hello, Attached is your P' + FiscalPeriod + ' Periodic P&L Report.' AS EmailContent
			,CAST(1 AS bit) AS IncludeLink
			,CAST(1 AS bit) AS IncludeReport
			,'PDF' AS RenderFormatIfIncludeReport
			,OrganizationOrgHierarchy
			,DateFiscalPeriod
    FROM	CTEOrganizationOrgHierarchy AS org
		CROSS JOIN CTEDateFiscalPeriod AS pd
	GROUP BY OrganizationOrgHierarchy
			,DateFiscalPeriod
			,FiscalPeriod;
END
GO
