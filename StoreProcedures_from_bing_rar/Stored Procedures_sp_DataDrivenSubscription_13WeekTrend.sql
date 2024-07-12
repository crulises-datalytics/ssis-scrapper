
CREATE PROCEDURE [rep].[sp_DataDrivenSubscription_13WeekTrend] (
    @Ancestor tinyint = 1
   ,@PreviousPeriod tinyint = 0
   ,@ReportFilterName varchar(50) = null
   ,@TestEmail varchar(150) = NULL
)
    AS
BEGIN
    -- ================================================================================
    --
    -- Stored Procedure:    rep.sp_DataDrivenSubscription_13WeekTrend
    --
    -- Purpose:             Outputs the Data Driven Subscription parameters for the 13 Week Trend Report.
    -- Parameters:
	--					          @Ancestor <tinyint> = Org Hierarchy Ancestor
	--					          @PreviousPeriod <tinyint> = Use a previous Date Period
	--                       @ReportFilterName <varchar(50)> = to filter report list, e.g. "Center Only"
	--					          @TestEmail <varchar(150)> = Send all emails to a test email instead of an actual email
    --
    -- Populates:           N/A
    --
    -- Usage:               EXEC rep.sp_DataDrivenSubscription_13WeekTrend  @Ancestor = 0, @PreviousPeriod = 1, @ReportFilterName = 'District Region', @TestEmail = 'anquitta@kc-education.com'
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:
    -- ----------
    --
    -- Date            Modified By      Comments
    -- ----            -----------      --------
    --
    -- 08/01/18        anmorales        Created original procedure
    -- 08/09/18        anmorales        Update procedure to use correct Excel reference, change int to bit for Include columns
    -- 11/27/18        anmorales        Update procedure to reference the report parameter functions
    -- 12/10/18		  anquitta			 small update on column names in rep.fnOrganizationOrgHierarchy
    -- 02/13/19        anquitta         added columns OrgTypeName, ol.HierarchyLevelNumber to CTEOrganizationOrgHierarchy
	 --                                  new parameter @ReportFilterName, output initial results to temp table, dynamic WHERE clause to create final output
    -- ================================================================================
		
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;


    WITH CTEOrganizationOrgHierarchy AS (
       SELECT CASE WHEN NULLIF(PersonFullName, '') IS NOT NULL THEN QUOTENAME(PersonFullName, '"') + QUOTENAME(ISNULL(@TestEmail, EmailAddress), '<') ELSE ISNULL(@TestEmail, EmailAddress) END AS EmailAddress
	         ,OrganizationOrgHierarchy
			 ,OrgTypeName 
			 ,HierarchyLevelNumber
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
          ,'13 Week Trend Report ' AS EmailSubject
          ,'Hello, Attached is your 13 Week Trend Report.' AS EmailContent
          ,CAST(1 AS bit) AS IncludeLink
          ,CAST(1 AS bit) AS IncludeReport
          ,'EXCELOPENXML' AS RenderFormatIfIncludeReport
          ,OrganizationOrgHierarchy
          ,min(OrgTypeName) AS OrgTypeName
          ,min(HierarchyLevelNumber) AS HierarchyLevelNumber
          ,DateFiscalWeek
	   INTO #ReportFilter
       FROM CTEOrganizationOrgHierarchy AS org
          CROSS JOIN CTEDateFiscalWeek AS wk
       GROUP BY OrganizationOrgHierarchy
          ,DateFiscalWeek;

	IF COALESCE(@ReportFilterName, '') = ''
		SELECT	*
		FROM	#ReportFilter
	ELSE
	BEGIN
		DECLARE @FilterDescription varchar(max)
		DECLARE @Sql nvarchar(max)

		SELECT	@FilterDescription = FilterDescription
		FROM	rep.ReportFilter
		WHERE	ReportFilterName = @ReportFilterName

		SET @Sql = '
			SELECT	*
			FROM	#ReportFilter
			WHERE	1 = 1
				AND	( '+@FilterDescription+' )
			'
		EXEC sp_executeSQL @Sql
	END	-- IF COALESCE(@ReportFilterName, '') = ''
    
END
