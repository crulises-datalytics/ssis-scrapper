CREATE PROCEDURE [rep].[sp_DataDrivenSubscription_AnnualPeriodicBudget] (
    @Ancestor tinyint = 0
   ,@PreviousPeriod tinyint = 0
   ,@TestEmail varchar(150) = NULL
)
	AS
BEGIN
	-- ================================================================================
	-- 
	-- Stored Procedure:	rep.sp_DataDrivenSubscription_AnnualPeriodicBudget
	--
	-- Purpose:				Outputs the Data Driven Subscription parameters for the 
   --                      Periodic Budgets (Annual Periodic Budgets) Report.	
    -- Parameters:
	--					@Ancestor <tinyint> = Org Hierarchy Ancestor
	--					@PreviousPeriod <tinyint> = Use a previous Date Period
	--					@TestEmail <varchar(150)> = Send all emails to a test email instead of an actual email	 		 					 
	--
	--
	-- Populates:			 N/A
	--
	-- Usage:				  EXEC rep.sp_DataDrivenSubscription_AnnualPeriodicBudget
	--
	-- --------------------------------------------------------------------------------
	--
	-- Change Log:			
	-- ----------
	--
	-- Date			Modified By	  Comments
	-- ----			-----------	  --------
	--
	-- 08/03/18     anmorales			BNG-3502 Create original data driven subscription.
    -- 11/27/18    anmorales        Update procedure to reference the report parameter functions
	 -- 12/10/18	 anquitta			small update on column names in rep.fnOrganizationOrgHierarchy
	--			 
	-- ================================================================================
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	
    WITH CTEOrganizationOrgHierarchy AS (
       SELECT CASE WHEN NULLIF(PersonFullName, '') IS NOT NULL THEN QUOTENAME(PersonFullName, '"') + QUOTENAME(ISNULL(@TestEmail, EmailAddress), '<') ELSE ISNULL(@TestEmail, EmailAddress) END AS EmailAddress
	         ,OrganizationOrgHierarchy
		  FROM rep.fnOrganizationOrgHierarchy (@Ancestor) 
		  WHERE CCClassification IN ('New', 'Same')
    ), CTEDateFiscalPeriod AS (
       SELECT DateFiscalPeriod 
	      FROM rep.fnDateFiscalPeriod(@PreviousPeriod)
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
		  ,'Periodic Budgets (Annual Periodic Budgets)' AS EmailSubject
		  ,'Hello, Attached is your Periodic Budgets (Annual Periodic Budgets) Report.' AS EmailContent
		  ,CAST(1 AS bit) AS IncludeLink
          ,CAST(1 AS bit) AS IncludeReport
          ,'PDF' AS RenderFormatIfIncludeReport
          ,OrganizationOrgHierarchy
		  ,DateFiscalPeriod
       FROM CTEOrganizationOrgHierarchy AS org
          CROSS JOIN CTEDateFiscalPeriod AS pd
		GROUP BY OrganizationOrgHierarchy
          ,DateFiscalPeriod;
END
GO


