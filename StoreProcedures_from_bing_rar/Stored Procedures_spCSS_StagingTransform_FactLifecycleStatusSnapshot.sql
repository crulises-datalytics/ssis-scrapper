CREATE PROCEDURE [dbo].[spCSS_StagingTransform_FactLifecycleStatusSnapshot]
(@EDWRunDateTime DATETIME2 = NULL,
 @FiscalWeek     INT
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCSS_StagingTransform_FactLifecycleStatusSnapshot
    --
    -- Purpose:            Performs the transformation logic with the source database
    --                         for a given Fact or Dimension table, and returns the
    --                         results set to the caller (usually for populating a
    --                         temporary table).
    --
    -- Parameters:         @EDWRunDateTime
    --
    -- Returns:            Results set containing the transformed data ready for
    --                         consumption by the ETL process for load into BING_EDW
    --
    -- Usage:              INSERT #FactLifecycleStatusSnapshotUpsert -- (Temporary table)
    --                     EXEC dbo.spCSS_StagingTransform_FactLifecycleStatusSnapshot @EDWRunDateTime
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date          Modified By        Comments
    -- ----          -----------        --------
    --
    --  7/17/18      sburke             BNG-3350 - Initial version of proc
    --  8/17/18      sburke             BNG-3582 - Performance improvements, plus adding of Enrollment and Withdrawal details
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;
	    --
	    -- Housekeeping Variables
	    -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceSystem NVARCHAR(3)= 'CSS'; -- C S S   V E R S I O N   O F   E T L 
	    --
         DECLARE @CurrentFiscalWeekSequenceNumber INT;
         DECLARE @PreviousFiscalWeekSequenceNumber INT;
	    --
	    -- If we do not get an @EDWRunDateTime input, set to current date
	    --
         IF @EDWRunDateTime IS NULL
             SET @EDWRunDateTime = GETDATE();
	    --
	    -- Execute the extract / transform from the Staging database source
	    --
         BEGIN TRY
             SELECT @CurrentFiscalWeekSequenceNumber = FiscalWeekSequenceNumber,
                    @PreviousFiscalWeekSequenceNumber = FiscalWeekSequenceNumber - 1
             FROM BING_EDW..DimDate dt
             WHERE dt.FiscalWeekNumber = @FiscalWeek;
		   -- -------------------------------------------------------------------------------- 
		   -- For Students with a Lifecycle Status of 'Withdrawn', we only want to show those 
		   --   that are withdrawing the Fiscal Week we are running the proc for.  In the  
		   --   sourcedata, however, once a Student has a status of Withdrawn, it remains 
		   --   in the Lifecycle table, repeated each week forever.
		   --
		   -- Therefore, we check to see if each student has a status of Withdrawn from the 
		   --   previous Fiscal Week, and if it has, we exclude it from the Fact table load,
		   --   thus ensuring that the last time a Student appears in the Fact table is when
		   --   they completely Withdraw from KinderCare.
		   --
		   -- If a Student Withdraw and then returns ata later date, this process will pick 
		   --   them back up, and the FactLifecycleStatusSnapshot table will show the history
		   --   of them Withdrawing and Re-Enrolling correctly
		   -- --------------------------------------------------------------------------------
		   --
		   -- Get previous week's Lifecycle data (we do this performance - source LifecycleStatusSnapshot table is huge)
		   --

             SELECT tLflw.*
             INTO #LifecycleStatusSnapshotPreviousWeek
             FROM dbo.LifecycleStatusSnapshot tLflw
                  INNER JOIN BING_EDW..DimDate dm_dt ON tLflw.AsOfFiscalWeek = dm_dt.FiscalWeekNumber
                                                        AND dm_dt.FiscalWeekEndDate = dm_dt.FullDate
             WHERE dm_dt.FiscalWeekSequenceNumber = @PreviousFiscalWeekSequenceNumber;
		   --
             CREATE NONCLUSTERED INDEX [CIX_LifecycleStatusSnapshotPreviousWeek] ON #LifecycleStatusSnapshotPreviousWeek
             ([ctr_no] ASC, [fam_no] ASC, [stu_no] ASC, [LifecycleStatusKey] ASC
             );
		   --
		   -- Get current week's Lifecycle data also (again, for performance)
		   --

             SELECT tLflw.*
             INTO #LifecycleStatusSnapshotCurrentWeek
             FROM dbo.LifecycleStatusSnapshot tLflw
                  INNER JOIN BING_EDW..DimDate dm_dt ON tLflw.AsOfFiscalWeek = dm_dt.FiscalWeekNumber
                                                        AND dm_dt.FiscalWeekEndDate = dm_dt.FullDate
             WHERE dm_dt.FiscalWeekSequenceNumber = @CurrentFiscalWeekSequenceNumber;
		   --
             CREATE NONCLUSTERED INDEX [CIX_LifecycleStatusSnapshotCurrentWeek] ON #LifecycleStatusSnapshotCurrentWeek
             ([ctr_no] ASC, [fam_no] ASC, [stu_no] ASC, [LifecycleStatusKey] ASC
             );
		   --
		   -- Delete records from the current week that are Withdrawn, where there is a corresponding Withdrawn last week...
		   --
		   -- (This might seem like a long-winded way of doing it, but it is copied from the CMS pattern which is a bit
		   -- more involved and tricky, and wanted to have the two processes be a variation on the same theme).  Performance
		   -- and space-wise it is fine, and it is simpler and more readable this way.
		   --
             DELETE #LifecycleStatusSnapshotCurrentWeek
             FROM #LifecycleStatusSnapshotCurrentWeek curr
                  INNER JOIN #LifecycleStatusSnapshotPreviousWeek prev ON curr.ctr_no = prev.ctr_no
                                                                          AND curr.fam_no = prev.fam_no
                                                                          AND curr.stu_no = prev.stu_no
             WHERE curr.LifecycleStatusKey = 12
                   AND prev.LifecycleStatusKey = 12;
		   --
		   -- Return the results set from the current week
		   --
             WITH CTE_TransactionCodes
                  AS (
                  SELECT vTrn.ctr_no,
                         vTrn.fam_no,
                         vTrn.stu_no,
                         vTrn.tx_type,
                         vTrn.tx_code
                  FROM vTransaction vTrn
                       LEFT JOIN BING_EDW..DimDate dm_dt ON vTrn.yr_week = dm_dt.FiscalWeekNumber
                                                            AND dm_dt.FiscalWeekEndDate = dm_dt.FullDate
                  WHERE dm_dt.FiscalWeekNumber = @FiscalWeek)
                  SELECT DISTINCT
                         COALESCE(dm_dt.DateKey, -1) AS DateKey,
                         COALESCE(dm_dt.AcademicYearNumber, 1900) AS CurrentAcademicYearNumber, -- The current Academic Year for the as-of date (i.e. the date of the snapshot)
                         COALESCE(dm_std.StudentKey, -1) AS StudentKey,
                         COALESCE(CostCenterKey, -1) AS CostCenterKey,
                         COALESCE(CostCenterTypeKey, -1) AS CostCenterTypeKey,
                         COALESCE(OrgKey, -1) AS OrgKey,
                         COALESCE(LocationKey, -1) AS LocationKey,
                         COALESCE(SponsorKey, -1) AS SponsorKey,
                         -2 AS TransactionCodeKey,
                         COALESCE(dm_tLf.LifecycleStatusKey, -1) AS LifecycleStatusKey,
                         COALESCE(dm_enr_dt.DateKey, -1) AS StudentFirstEnrollmentDateKey, -- The date the student first enrolled at KCE
                         COALESCE(dm_enr_dt.AcademicYearNumber, 1900) AS StudentFirstEnrollmentAcademicYearNumber, -- The Academic Year the student first enrolled
                         CASE
                             WHEN dm_dt.AcademicYearNumber = dm_enr_dt.AcademicYearNumber
                             THEN 1
                             ELSE 0
                         END AS StudentEnrolledCurrentAcademicYear, -- Was the student enrolled during the current academic year for the as-of date (the date of the snapshot)
                         CASE
                             WHEN dm_tLf.LifecycleStatusName = 'Withdrawn'
                                  AND dm_dt.AcademicYearNumber = dm_enr_dt.AcademicYearNumber
                             THEN 1
                             ELSE 0
                         END AS StudentWithdrewCurrentAcademicYear, -- Did the student Withdraw during the same Academic Year as they enrolled
                         CASE
                             WHEN dm_tLf.LifecycleStatusName = 'Withdrawn'
                                  AND dm_dt.BTSYearNumber = dm_enr_dt.BTSYearNumber
                             THEN 1
                             ELSE 0
                         END AS StudentWithdrewCurrentBTSYear, -- Did the student Withdraw during the same Back-to-School Year as they enrolled
                         CASE
                             WHEN dm_dt.AcademicYearNumber - 1 = dm_enr_dt.AcademicYearNumber
                             THEN 1
                             ELSE 0
                         END AS StudentEnrolledPreviousAcademicYear, -- Was the Student enrolled the previous year to the Current (as-of) Year
                         @SourceSystem AS SourceSystem,
                         @EDWRunDateTime AS EDWCreatedDate
                  FROM #LifecycleStatusSnapshotCurrentWeek tLf
                       LEFT JOIN BING_EDW..DimDate dm_dt ON tLf.AsOfFiscalWeek = dm_dt.FiscalWeekNumber
                                                            AND dm_dt.FiscalWeekEndDate = dm_dt.FullDate
                       LEFT JOIN BING_EDW.dbo.DimStudent dm_std ON tLf.ctr_no = dm_std.CSSCenterNumber
                                                                   AND tLF.fam_no = dm_std.CSSFamilyNumber
                                                                   AND tLf.stu_no = dm_std.CSSStudentNumber
                                                                   AND dm_std.EDWEndDate IS NULL -- DimStudent is SCD2, so get the latest version
                                                                   AND dm_std.SourceSystem = @SourceSystem
                       LEFT JOIN BING_EDW..DimDate dm_enr_dt ON dm_std.StudentFirstEnrollmentDate = dm_enr_dt.FullDate
                       LEFT JOIN BING_EDW.dbo.DimCostCenter dm_cctr ON tLf.ctr_no = dm_cctr.CenterCSSID
                                                                       AND dm_cctr.EDWEndDate IS NULL -- DimCostCenter is SCD2, so get the latest version
                       LEFT JOIN BING_EDW.dbo.DimCostCenterType dm_cctyp ON dm_cctr.CostCenterTypeID = dm_cctyp.CostCenterTypeID
                       LEFT JOIN BING_EDW.dbo.DimOrganization dm_org ON dm_cctr.CostCenterNumber = dm_org.CostCenterNumber
                                                                        AND dm_org.EDWEndDate IS NULL -- DimOrganization is SCD2, so get the latest version
                       LEFT JOIN BING_EDW.dbo.DimLocation dm_loc ON dm_org.DefaultLocationID = dm_loc.LocationID
                                                                    AND dm_loc.EDWEndDate IS NULL -- DimLocation is SCD2, so get the latest version
                       LEFT JOIN BING_EDW.dbo.DimSponsor dm_spr ON tLf.ctr_no = dm_spr.CSSCenterNumber
                                                                   AND tLf.fam_no = dm_spr.CSSFamilyNumber
                                                                   AND dm_spr.EDWEndDate IS NULL
                                                                   AND dm_spr.SourceSystem = @SourceSystem
                       LEFT JOIN CTE_TransactionCodes cte_trn ON tLf.ctr_no = cte_trn.ctr_no
                                                                 AND tLf.fam_no = cte_trn.fam_no
                                                                 AND tLf.stu_no = cte_trn.stu_no
                       LEFT JOIN BING_EDW.dbo.DimTransactionCode dm_trn_code ON cte_trn.tx_type = dm_trn_code.TransactionTypeCode
                                                                                AND cte_trn.tx_code = dm_trn_code.TransactionCode
                       LEFT JOIN BING_EDW..DimLifecycleStatus dm_tLf ON tLf.LifecycleStatusKey = dm_tLf.LifecycleStatusKey;
             DROP TABLE #LifecycleStatusSnapshotCurrentWeek;  -- I'm old fashioned...
             DROP TABLE #LifecycleStatusSnapshotPreviousWeek;
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