CREATE PROCEDURE [dbo].[spCSS_StagingTransform_DimSponsor](@EDWRunDateTime DATETIME2 = NULL)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCSS_StagingTransform_DimSponsor
    --
    -- Purpose:            Performs the transformation logic with the source database
    --                         for a given Fact or Dimension table, and returns the
    --                         results set to the caller (usually for populating a
    --                         temporary table).
    --
    -- Parameters:         @EDWRunDateTime - Used to populate any 'current date' fields, rather than
    --                         making numerous GETDATE() calls  
    --				   
    -- Returns:            Results set containing the transformed data ready for
    --                         consumption by the ETL process for load into BING_EDW
    --
    -- Usage:              INSERT #TemplateUpsert -- (Temporary table)
    --                     EXEC dbo.spCSS_StagingTransform_DimSponsor; @EDWRunDateTime
    -- 
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date          Modified By        Comments
    -- ----          -----------        --------
    --
    -- 12/12/17      Adevabhakthuni     Initial version of proc, converted from SSIS logic
    --  3/01/18      sburke             BNG-	1246 - Use Fb_id from FamilyBuilderMapping to 
    --                                      populate SponsorLeadManagementId field.
    --                                  Also do some code tidy-up.  The file for this stored
    --                                      procedure in Source Control is spCSS_StagingTransform_DimSponsor_1.sql
    --                                      which will be corrected, by removing the '_1.sql' file
    --                                      from the Database project and adding the correctly named object.					           
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;
	    --
	    -- Housekeeping Variables
	    -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
	    --
	    -- If we do not get an @EDWRunDateTime input, set to current date
	    --
         IF @EDWRunDateTime IS NULL
             SET @EDWRunDateTime = GETDATE();
	    --
	    -- Execute the extract / transform from the Staging database source
	    --
         BEGIN TRY
             WITH Contacts_CTE
                  AS (
                  SELECT cntcts.ctr_no,
                         cntcts.fam_no,
                         cntcts.fname,
                         cntcts.mid_init,
                         cntcts.lname,
                         cntcts.w_areacd,
                         cntcts.w_phone,
                         cntcts.city,
                         cntcts.state,
                         cntcts.zip,
                         cntcts.relationship
                  FROM dbo.Csecnctd cntcts
                  --
                  -- Determine list of primary contacts.  There may be several such contacts.  Merely choose the most recent one.
                  --
                       INNER JOIN
                  (
                      SELECT ctr_no,
                             fam_no,
                             MAX(cont_no) AS cont_no
                      FROM dbo.Csecnctd
                      WHERE cont_ar = 'Y'
                      GROUP BY ctr_no,
                               fam_no
                  ) prmy_cntcts ON cntcts.ctr_no = prmy_cntcts.ctr_no
                                   AND cntcts.fam_no = prmy_cntcts.fam_no
                                   AND cntcts.cont_no = prmy_cntcts.cont_no
                  WHERE cntcts.fam_no NOT LIKE '%[A-z]%'),
                  --
                  -- Customers, joining to FamilyBuilderMapping for the FamilyBuilderId (used for SponsorLeadManagementID)
                  --
                  Customers_CTE
                  AS (
                  SELECT cust.ctr_no,
                         cust.cust_code,
                         cust.contact,
                         cust.address1,
                         cust.address2,
                         cust.phone,
                         fm_bldr_map.fb_id
                  FROM dbo.cspcustr cust
                       LEFT JOIN FamilyBuilderMapping fm_bldr_map ON cust.ctr_no = fm_bldr_map.ctr_no
                                                                     AND cust.cust_code = fm_bldr_map.fam_no
                                                                     AND fm_bldr_map.stu_no IS NULL
                  WHERE cust.cust_code NOT LIKE '%[A-z]%'),
                  --
                  -- There are duplicate entries for a given family in this table, and we are using a distinct here to eliminate that issue.
                  --
                  Families_CTE
                  AS (
                  SELECT DISTINCT
                         ctr_no,
                         fam_no,
                         kclc_emp
                  FROM dbo.Csxfamr
                  WHERE fam_no IS NOT NULL)
                  -- 
                  -- Main SELECT statement
                  --
                  SELECT-2 AS SponsorID,
                        COALESCE(NULLIF(cte_cntct.fname, ''), 'Unknown Sponsor Name') AS SponsorFirstName,
                        COALESCE(NULLIF(cte_cntct.mid_init, ''), 'Unknown Sponsor Name') AS SponsorMiddleName,
                        COALESCE(NULLIF(cte_cntct.lname, ''), 'Unknown Sponsor Name') AS SponsorLastName,
                        COALESCE(NULLIF(cust_fmly.contact, ''), 'Unknown Sponsor Name') AS SponsorFullName,
                        COALESCE(NULLIF(cust_fmly.phone, ''), 'Unknown Phone Number') AS SponsorPhonePrimary,
                        COALESCE(NULLIF(cte_cntct.w_areacd+'-'+cte_cntct.w_phone, ''), 'Unknown Phone Number') AS SponsorPhoneSecondary,
                        'Unknown Phone Number' AS SponsorPhoneTertiary,
                        'Unknown Email' AS SponsorEmailPrimary,
                        'Unknown Email' AS SponsorEmailSecondary,
                        COALESCE(NULLIF(cust_fmly.address1, ''), 'Unknown Address') AS SponsorAddress1,
                        COALESCE(NULLIF(cust_fmly.address2, ''), 'Unknown Address') AS SponsorAddress2,
                        COALESCE(NULLIF(cte_cntct.city, ''), 'Unknown City') AS SponsorCity,
                        COALESCE(NULLIF(cte_cntct.state, ''), 'XX') AS SponsorState,
                        COALESCE(NULLIF(cte_cntct.zip, ''), 'Unknown') AS SponsorZIP,
                        COALESCE(NULLIF(cte_cntct.relationship, ''), 'Unknown') AS SponsorStudentRelationship,
                        CASE
                            WHEN cte_cntct.relationship IN('FA', 'GF', 'UN', 'BR')
                            THEN 'Male'
                            WHEN cte_cntct.relationship IN('MO', 'GM', 'AU', 'SI')
                            THEN 'Female'
                            ELSE 'Unknown'
                        END AS SponsorGender,
                        CASE
                            WHEN cust_fmly.kclc_emp = 'Y'
                            THEN 'KCLC Employee'
                            ELSE 'Not KCLC Employee'
                        END AS SponsorInternalEmployee,
                        'Unknown Sponsor Status' AS SponsorStatus,
                        'Unknown Email Sponsor' AS SponsorDoNotEmail,
                        COALESCE(cust_fmly.fb_id, '-1') AS SponsorLeadManagementID,
                        COALESCE(cust_fmly.combined_ctr_no, cte_cntct.ctr_no, '-1') AS CSSCenterNumber,
                        COALESCE(cust_fmly.combined_fam_no, cte_cntct.fam_no, '-1') AS CSSFamilyNumber,
                        'CSS' AS SourceSystem,
                        '1900-01-01' AS EDWEffectiveDate,
                        GETDATE() AS EDWCreatedDate,
                        CAST(SUSER_SNAME() AS VARCHAR(50)) AS EDWCreatedBy,
                        NULL AS Deleted
                  FROM
                  (
                      SELECT combined_ctr_no = COALESCE(cte_cust.ctr_no, cte_fmly.ctr_no),
                             combined_fam_no = COALESCE(cte_cust.cust_code, cte_fmly.fam_no),
                             cust_ctr_no = cte_cust.ctr_no,
                             cte_cust.cust_code,
                             cte_cust.fb_id,
                             fam_ctr_no = cte_fmly.ctr_no,
                             cte_fmly.fam_no,
                             cte_fmly.kclc_emp,
                             cte_cust.contact,
                             phone,
                             cte_cust.address1,
                             cte_cust.address2
                      FROM Customers_CTE cte_cust
                           FULL OUTER JOIN Families_CTE cte_fmly ON cte_cust.ctr_no = cte_fmly.ctr_no
                                                                    AND cte_cust.cust_code = cte_fmly.fam_no
                  ) cust_fmly
                  FULL OUTER JOIN Contacts_CTE cte_cntct ON cust_fmly.combined_ctr_no = cte_cntct.ctr_no
                                                            AND cust_fmly.combined_fam_no = cte_cntct.fam_no
                  WHERE COALESCE(cust_fmly.combined_ctr_no, cte_cntct.ctr_no) IS NOT NULL
                        AND COALESCE(cust_fmly.combined_fam_no, cte_cntct.fam_no) IS NOT NULL;
         END TRY
	    --
	    -- Catch, and throw the error back to the calling procedure or client
	    --
         BEGIN CATCH
             DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
             SELECT @ErrMsg = 'Sub-procedure '+@ProcName+' - '+ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;