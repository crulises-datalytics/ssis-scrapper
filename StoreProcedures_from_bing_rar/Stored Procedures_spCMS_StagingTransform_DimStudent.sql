CREATE PROCEDURE [dbo].[spCMS_StagingTransform_DimStudent] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingTransform_DimStudent
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
    --                     EXEC dbo.spCMS_StagingTransform_DimStudent @EDWRunDateTime
    -- 
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date        Modified By         Comments
    -- ----        -----------         --------
    --
    -- 12/11/17    sburke              BNG-527 - Refactoring of DimStudent load to move away 
    --                                   from SSIS building temp tables in EDW to using stored
    --                                   proc
    --  8/13/18    sburke              BNG-3587 - Populate StudentFirstEnrollmentDate for DimStudent			 
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
                 FROM CMS_Staging..EDWETLBatchControl(NOLOCK)
                 WHERE EventName = 'DimStudent'
             );
             IF @LastProcessedDate IS NULL
                 SET @LastProcessedDate = '19000101';   -- If no previous load logged in EDWETLBatchControl, assume we bring in everything


             WITH StudentCategoryCTE
                  AS (
                  SELECT a.*
                  FROM
                  (
                      SELECT ROW_NUMBER() OVER(PARTITION BY a.CreatedUTC ORDER BY a.idStudentCategory) AS id,
                             a.idStudent,
                             a.CreatedUTC,
                             a.idStudentCategory,
                             CAST(c.name AS VARCHAR(100)) StudentSubCategory,
                             a.StgModifiedDate
                      FROM stdStudentStudentCategory(NOLOCK) a
                           INNER JOIN sbsdyStudentCategory(NOLOCK) c ON a.idStudentCategory = c.idStudentCategory
                           INNER JOIN
                      (
                          SELECT idStudent,
                                 MAX(CreatedUTC) CreatedUTC
                          FROM stdStudentStudentCategory(NOLOCK)
                          GROUP BY idStudent
                      ) b ON a.idStudent = b.idStudent
                             AND a.CreatedUTC = b.CreatedUTC
                  ) a
                  WHERE id = 1),
                  FirstEnrollmentDate -- Get the earliest EffectiveDate as our StudentFirstEnrollmentDate
                  AS (
                  SELECT idStudent,
                         MIN(EffectiveDate) StudentFirstEnrollmentDate
                  FROM vEnrollment
                  GROUP BY idStudent)
                  SELECT s.idStudent AS StudentID,
                         COALESCE(NULLIF(s.FirstName, ''), 'Unknown First Name') AS StudentFirstName,
                         COALESCE(s.MiddleName, '') AS StudentMiddleName,
                         COALESCE(NULLIF(s.LastName, ''), 'Unknown Last Name') AS StudentLastName,
                         COALESCE(s.SuffixName, '') AS StudentSuffixName,
                         COALESCE(s.PreferredName, '') AS StudentPreferredName,
                         COALESCE(NULLIF(s.FirstName, ''), 'Unknown Student Name')+' '+COALESCE(NULLIF(LastName, ''), 'Unknown Student Name')+''+COALESCE(SuffixName, '') AS StudentFullName,
                         COALESCE(NULLIF(s.BirthDate, ''), '9999-12-31') AS StudentBirthDate,
                         CASE
                             WHEN s.Gender = 'M'
                             THEN 'Male'
                             WHEN s.Gender = 'F'
                             THEN 'Female'
                             ELSE 'Unknown'
                         END AS StudentGender,
                         COALESCE(NULLIF(s.Ethnicity, ''), 'Unknown Ethnicity') AS StudentEthnicity,
                         COALESCE(NULLIF(ss.StudentStatus, ''), 'Unknown Status') AS StudentStatus,
                         ('No Data') AS StudentLifetimeSubsidy,
                         COALESCE(NULLIF(ls.StudentCategory, ''), 'Unknown Category') AS StudentCategory,
                         COALESCE(NULLIF(sc.StudentSubCategory, ''), 'Unknown SubCategory') AS StudentSubCategory,
                         COALESCE(NULLIF(s.PhoneNumber, ''), 'Unknown Phone Number') AS StudentPhone,
                         COALESCE(NULLIF(a.Address1, ''), 'Unknown Address') AS StudentAddress1,
                         COALESCE(NULLIF(a.Address2, ''), 'Unknown Address') AS StudentAddress2,
                         COALESCE(NULLIF(a.City, ''), 'Unknown City') AS StudentCity,
                         COALESCE(NULLIF(a.idState, ''), 'XX') AS StudentState,
                         COALESCE(NULLIF(a.ZipCode, ''), 'Unknown') AS StudentZIP,
                         COALESCE(NULLIF(s.PrimaryLanguage, ''), 'Unknown Primary Language') AS StudentPrimaryLanguage,
                         ISNULL(frst.StudentFirstEnrollmentDate, '1900-01-02') AS StudentFirstEnrollmentDate, -- If no Enrollment Date (which means no enrollment), default to the N/A date
                         'Unknown' AS StudentCareSelectStatus,
                         '-2' AS CSSCenterNumber,
                         '-2' AS CSSFamilyNumber,
                         '-2' AS CSSStudentNumber,
                         'CMS' AS SourceSystem,
                         @EDWRunDateTime AS EDWEffectiveDate,
                         NULL AS EDWEndDate,
                         GETDATE() AS EDWCreatedDate,
                         CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWCreatedBy,
                         s.Deleted
                  FROM stdStudent(NOLOCK) s
                       LEFT JOIN stdStudentStatus(NOLOCK) ss ON s.idStudentStatus = ss.idStudentStatus
                       LEFT JOIN Address(NOLOCK) a ON a.idaddress = s.idaddress
                       LEFT JOIN StudentCategoryCTE sc ON s.idStudent = sc.idStudent
                       LEFT JOIN LookupStudentSubcategoryCategory(NOLOCK) ls ON sc.idStudentCategory = ls.StudentSubCategoryID
                       LEFT JOIN FirstEnrollmentDate frst ON frst.idStudent = s.idStudent -- Note: not all Students have an enrollment record, so left join here...
                  WHERE(s.StgModifiedDate >= @LastProcessedDate
                        OR sc.StgModifiedDate >= @LastProcessedDate
                        OR ls.StgModifiedDate >= @LastProcessedDate
                        OR a.StgModifiedDate >= @LastProcessedDate
                        OR ss.StgModifiedDate >= @LastProcessedDate);
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


