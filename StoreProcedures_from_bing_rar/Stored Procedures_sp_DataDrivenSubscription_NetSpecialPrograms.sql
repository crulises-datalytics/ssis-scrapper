
CREATE PROCEDURE [rep].[sp_DataDrivenSubscription_NetSpecialPrograms] (
    @Ancestor tinyint = 1
   ,@PreviousPeriod tinyint = 0
   ,@TestEmail varchar(150) = NULL
)
	AS
BEGIN
--	-- ================================================================================
	-- 
	-- Stored Procedure:	rep.sp_DataDrivenSubscription_NetSpecialPrograms
	--
	-- Purpose:				Outputs the Data Driven Subscription parameters for the Net Special Programs Report.
	--							The table in question is almost static - that is, we don't
	--								 expect the data to change often.  However, we have
	--								 the population process encapsulated in a proc if we  
	--								 need to update or [re]deploy the entire database solution
	--								 from scratch.	  
	--
	--							The logic putting in sproc
	--								 makes it easier to locate what's actually populating the table	
    -- Parameters:
	--					@Ancestor <tinyint> = Org Hierarchy Ancestor
	--					@PreviousPeriod <tinyint> = Use a previous Date Period
	--					@TestEmail <varchar(150)> = Send all emails to a test email instead of an actual email			 		 					 
	--
	--
	-- Populates:			 Extracts Data Driven Subscription Parameters
	--
	-- Usage:				  EXEC rep.sp_DataDrivenSubscription_NetSpecialPrograms  @Ancestor = 1, @PreviousPeriod = 1, @TestEmail = '"anquitta@kc-education.com'
	--
	-- --------------------------------------------------------------------------------
	--
	-- Change Log:			
	-- ----------
	--
	-- Date			Modified By		Comments
	-- ----			-----------		--------
	--
	-- 08/06/18		valimineti	    BNG-3501  
   -- 11/27/18     anmorales		Update procedure to reference the report parameter functions
	-- 12/11/18     anquitta		BNG-4439 multiple email addresses, fix grouping
	-- ================================================================================
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	
    WITH CTEOrganizationOrgHierarchy AS (
       SELECT CASE WHEN NULLIF(PersonFullName, '') IS NOT NULL THEN QUOTENAME(PersonFullName, '"') + QUOTENAME(ISNULL(@TestEmail, EmailAddress), '<') ELSE ISNULL(@TestEmail, EmailAddress) END AS EmailAddress
	         ,OrganizationOrgHierarchy
		  FROM rep.fnOrganizationOrgHierarchy (@Ancestor) 
		  WHERE CCClassification IN ('New', 'Same')
    ), CTEDateFiscalWeek AS (
       SELECT DateFiscalWeek 
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
			  ,'Net Special Programs' AS EmailSubject
			  ,'Hello, Attached is your Net Special Programs Report.' AS EmailContent
			  ,CAST(1 AS bit) AS IncludeLink
			  ,CAST(1 AS bit) AS IncludeReport
			  ,'PDF' AS RenderFormatIfIncludeReport
			  ,OrganizationOrgHierarchy
			  ,DateFiscalWeek
       FROM CTEOrganizationOrgHierarchy AS org
          CROSS JOIN CTEDateFiscalWeek AS wk
       GROUP BY org.OrganizationOrgHierarchy,
			wk.DateFiscalWeek
END

