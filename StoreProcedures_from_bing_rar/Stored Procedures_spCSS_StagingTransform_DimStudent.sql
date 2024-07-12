CREATE PROCEDURE [dbo].[spCSS_StagingTransform_DimStudent] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCSS_StagingTransform_DimStudent
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
    -- Usage:              INSERT #DimStudentUpsert -- (Temporary table)
    --                     EXEC dbo.spCSS_StagingTransform_DimStudent 
    -- 
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By     Comments
    -- ----         -----------     --------
    --
    --  2/26/18     sburke          BNG-1247 - Convert from SSIS DFT to stored proc
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
             DECLARE @LastProcessedDate DATETIME;
             SET @LastProcessedDate =
             (
                 SELECT LastProcessedDate
                 FROM dbo.EDWETLBatchControl(NOLOCK)
                 WHERE EventName = 'DimStudent - CSS'
             );
             IF @LastProcessedDate IS NULL
                 SET @LastProcessedDate = '19000101';   -- If no previous load logged in EDWETLBatchControl, assume we bring in everything
             WITH CTE_Contacts
                  AS (
                  SELECT ctr_no,
                         fam_no,
                         cont_no,
                         h_areacd,
                         h_phone,
                         addr,
                         addl_address,
                         city,
                         state,
                         zip
                  FROM
                  (
                      SELECT ROW_NUMBER() OVER(PARTITION BY ctr_no,
                                                            fam_no ORDER BY cont_no DESC) AS RowNumber,
                             ctr_no,
                             fam_no,
                             cont_no,
                             h_areacd,
                             h_phone,
                             addr,
                             addl_address,
                             city,
                             state,
                             zip
                      FROM dbo.Csecnctd
                      WHERE cont_ar = 'Y'
                  ) a
                  WHERE a.RowNumber = 1
                        AND a.fam_no NOT LIKE '%[A-z]%')
                  --

                  SELECT-2 AS StudentID,
                        COALESCE(fname, 'Unknown First Name') AS StudentFirstName,
                        COALESCE(s.mid_init, '') AS StudentMiddleName,
                        COALESCE(NULLIF(s.lname, ''), 'Unknown Last Name') AS StudentLastName,
                        '' AS StudentSuffixName,
                        COALESCE(s.pref_name, '') AS StudentPreferredName,
                        COALESCE(NULLIF(s.fname, ''), 'Unknown Student Name')+' '+COALESCE(NULLIF(lname, ''), 'Unknown Student Name') AS StudentFullName,
                        COALESCE(NULLIF(s.dob, ''), '9999-12-31') AS StudentBirthDate,
                        CASE
                            WHEN s.sex = 'M'
                            THEN 'Male'
                            WHEN s.sex = 'F'
                            THEN 'Female'
                            ELSE 'Unknown'
                        END AS StudentGender,
                        'Unknown Ethnicity' AS StudentEthnicity,
                        CASE
                            WHEN s.actv_flag = 'A'
                            THEN 'Active'
                            WHEN s.actv_flag = 'I'
                            THEN 'Inactive'
                            WHEN s.actv_flag = 'W'
                            THEN 'Withdrawn'
                            ELSE 'Unknown'
                        END AS StudentStatus,
                        --'Unknown Lifecycle Status' AS StudentLifecycleStatus,
                        'Unknown Lifetime Subsidy' AS StudentLifetimeSubsidy,
                        'Unknown Category' AS StudentCategory,
                        'Unknown SubCategory' AS StudentSubCategory,
                        COALESCE(c.h_areacd+'-'+c.h_phone, 'Unknown Phone Number') AS StudentPhone,
                        COALESCE(NULLIF(c.addr, ''), 'Unknown Address') AS StudentAddress1,
                        COALESCE(NULLIF(c.addl_address, ''), 'Unknown Address') AS StudentAddress2,
                        COALESCE(NULLIF(c.city, ''), 'Unknown City') AS StudentCity,
                        COALESCE(NULLIF(c.state, ''), 'XX') AS StudentState,
                        COALESCE(NULLIF(c.zip, ''), 'Unknown') AS StudentZIP,
                        COALESCE(NULLIF(s.prim_lang, ''), 'Unknown Primary Language') AS StudentPrimaryLanguage,
                        s.enroll_date AS StudentFirstEnrollmentDate,
                        'Unknown' AS StudentCareSelectStatus,
                        COALESCE(s.ctr_no, '-2') AS CSSCenterNumber,
                        COALESCE(s.fam_no, '-2') AS CSSFamilyNumber,
                        COALESCE(s.stu_no, '-2') AS CSSStudentNumber,
                        'CSS' AS SourceSystem,
                        @EDWRunDateTime AS EDWEffectiveDate,
                        NULL AS EDWEndDate,
                        @EDWRunDateTime AS EDWCreatedDate,
                        CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWCreatedBy
                  FROM dbo.Csxstudr s
                       LEFT JOIN CTE_Contacts c ON s.ctr_no = c.ctr_no
                                                   AND s.fam_no = c.fam_no
                  WHERE s.StgModifiedDate >= @LastProcessedDate;
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
GO