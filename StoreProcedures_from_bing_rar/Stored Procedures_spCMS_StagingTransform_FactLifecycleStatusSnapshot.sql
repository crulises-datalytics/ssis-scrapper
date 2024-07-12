CREATE PROCEDURE [dbo].[spCMS_StagingTransform_FactLifecycleStatusSnapshot]
(@EDWRunDateTime DATETIME2 = NULL,
 @FiscalDate     DATE
)
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingTransform_FactLifecycleStatusSnapshot
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
    --                     EXEC dbo.spCMS_StagingTransform_FactLifecycleStatusSnapshot @EDWRunDateTime
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date          Modified By        Comments
    -- ----          -----------        --------
    --
    --  7/17/18      sburke             BNG-3434 - Initial version of proc
    --  8/9/2018	 tschwenger		BNG-3527 - Modify ETL Process to Get Lst Cost Center For Withdrawn Students - CMS			 
    --  8/17/2018	 sburke             BNG-3582 - Addition of Enrollment columns to denote Withdrawals in the same year
    --  12/27/2018   adevabhakthuni     BNG-4505 - Modified to get Last costcenter number and sponsor ID 
	--  03/01/2021	 ty					BI-4552  - Needed some performance juicing. Should be lots faster. 
    -- ================================================================================

BEGIN
	SET NOCOUNT ON;
	--
	-- Housekeeping Variables
	-- 
	DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
	DECLARE @DebugMsg NVARCHAR(500);
	DECLARE @ROWCOUNT INT;
	DECLARE @SourceSystem NVARCHAR(3)= 'CMS'; -- C M S   V E R S I O N   O F   E T L 
	DECLARE @CurrentFiscalWeekSequenceNumber INT;
	DECLARE @PreviousFiscalWeekSequenceNumber INT;
	DECLARE @PrevMonthFiscalWeekSequenceNumber INT;

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
			@PreviousFiscalWeekSequenceNumber = FiscalWeekSequenceNumber - 1,
			@PrevMonthFiscalWeekSequenceNumber = FiscalWeekSequenceNumber - 52
		FROM BING_EDW..DimDate dt
		WHERE dt.FullDate = @FiscalDate;

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

	-- don't use vEnrollment, replace it with this temp table instead for performance
	SELECT
		e.idStudent,
		e.idSite,
		sp.idSponsor,
		sd.EffectiveDate,
		COALESCE(sd.EndDate, '12/31/9999') as EndDate
	INTO #LifecycleTempEnrollment
	FROM 
		enrlEnrollment e
		inner join enrlProgram p on e.idEnrollment = p.idEnrollment and e.Deleted IS NULL and p.Deleted IS NULL 
		inner join enrlSession ss on p.idProgramEnrollment = ss.idProgramEnrollment and ss.Deleted IS NULL
		inner join enrlScheduleDay sd on ss.idSessionEnrollment = sd.idSessionEnrollment and sd.Deleted IS NULL
		left join 
			(select idStudent, min(idSponsor) as idSponsor, min(StgModifiedDate) as StgModifiedDate 
			from stdStudentSponsor 
			where idSponsorType = 1 and Deleted IS NULL 
			group by idStudent) sp on e.idStudent = sp.idStudent
	where 
		sd.idScheduleStatus in (2,15)
	group by
		   e.idStudent,
		   e.idSite,
		   sp.idSponsor,
		   sd.EffectiveDate,
		   COALESCE(sd.EndDate, '12/31/9999');

	-----------------	

	WITH CTE_EnrollmentSummaryPreviousWeek AS (
        SELECT idStudent,
                idSite,
                idSponsor
        FROM #LifecycleTempEnrollment
        WHERE DATEADD(dd, -7, @FiscalDate) BETWEEN EffectiveDate AND EndDate
        GROUP BY idStudent,
                idSite,
                idSponsor)

    SELECT 
		tLflw.AsOfFiscalWeekEndDate,
		tLflw.idStudent,
		tLflw.LifecycleStatusKey,
		COALESCE(Enr.idSite, sq.idsite) AS idsite,
		COALESCE(Enr.idSponsor, sq.idSponsor) AS idSponsor
	INTO #LifecycleStatusSnapshotPreviousWeek
    FROM 
		dbo.LifecycleStatusSnapshot tLflw
        INNER JOIN BING_EDW..DimDate dm_dt ON tLflw.AsOfFiscalWeekEndDate = dm_dt.FullDate
        LEFT JOIN CTE_EnrollmentSummaryPreviousWeek Enr ON tLflw.idStudent = Enr.idStudent
        --To get last Cost center NUmber and Sponsor ID for Withdrawn students
        OUTER APPLY
    (
        SELECT TOP 1 EffectiveDate,
                    EndDate,
					idStudent, 
                    idSite,
                    idSponsor
        FROM #LifecycleTempEnrollment enr
        WHERE tLflw.[AsOfFiscalWeekEndDate] >= enr.EffectiveDate --AND enr.EndDate
            AND tLflw.idStudent = enr.idStudent
        ORDER BY enr.EndDate DESC
    ) AS sq
    WHERE 
		dm_dt.FiscalWeekSequenceNumber = @PreviousFiscalWeekSequenceNumber;

    SELECT @ROWCOUNT = @@ROWCOUNT;
    PRINT @ProcName+' : '+'Inserted '+CONVERT(VARCHAR(10), @ROWCOUNT)+' rows into #LifecycleStatusSnapshotPreviousWeek';

	-- add an index for performance
	CREATE NONCLUSTERED INDEX [CIX_LifecycleStatusSnapshotPreviousWeek] ON #LifecycleStatusSnapshotPreviousWeek
		([AsOfFiscalWeekEndDate] ASC, [idStudent] ASC
	);
	--
	-- Get current week's Lifecycle data also (again, for performance)
	--

	WITH CTE_EnrollmentSummaryCurrentWeek AS (
		SELECT idStudent,
				idSite,
				idSponsor
		FROM #LifecycleTempEnrollment enr
		WHERE @FiscalDate BETWEEN EffectiveDate AND EndDate
		GROUP BY idStudent,
				idSite,
				idSponsor)

	SELECT tLf.AsOfFiscalWeekEndDate,
			tLf.idStudent,
			tLf.LifecycleStatusKey,
			COALESCE(Enr.idSite, sq.idsite) AS idsite,
			COALESCE(Enr.idSponsor, sq.idSponsor) AS idSponsor
	INTO #LifecycleStatusSnapshotCurrentWeek
	FROM dbo.LifecycleStatusSnapshot tLf
		INNER JOIN BING_EDW..DimDate dm_dt ON tLf.AsOfFiscalWeekEndDate = dm_dt.FullDate
		LEFT JOIN CTE_EnrollmentSummaryCurrentWeek Enr ON tLf.idStudent = Enr.idStudent
	--To get last Cost center NUmber and Sponsor ID for Withdrawn students
		OUTER APPLY
	(
		SELECT TOP 1 EffectiveDate,
					EndDate,
					idStudent, 
					idSite,
					idSponsor
		FROM #LifecycleTempEnrollment enr
		WHERE tLf.[AsOfFiscalWeekEndDate] >= enr.EffectiveDate --AND enr.EndDate
			AND tLf.idStudent = enr.idStudent
		ORDER BY enr.EndDate DESC
	) AS sq
	WHERE dm_dt.FiscalWeekSequenceNumber = @CurrentFiscalWeekSequenceNumber;

	-- Save the row count in the variable
    SELECT @ROWCOUNT = @@ROWCOUNT;
    PRINT @ProcName+' : '+'Inserted '+CONVERT(VARCHAR(10), @ROWCOUNT)+' rows into #LifecycleStatusSnapshotCurrentWeek';

	-- We're done with the temp table, might as well drop it 
	DROP TABLE #LifeCycleTempEnrollment

	-- An index for this one too
    CREATE NONCLUSTERED INDEX [CIX_LifecycleStatusSnapshotCurrentWeek] ON #LifecycleStatusSnapshotCurrentWeek
    ([AsOfFiscalWeekEndDate] ASC, [idStudent] ASC
    );

    --
    -- Delete records from the current week that are Withdrawn, where there is a corresponding Withdrawn last week...
    --
    DELETE #LifecycleStatusSnapshotCurrentWeek
    FROM #LifecycleStatusSnapshotCurrentWeek curr
        INNER JOIN #LifecycleStatusSnapshotPreviousWeek prev ON curr.idStudent = prev.idStudent
    WHERE curr.LifecycleStatusKey = 12
        AND prev.LifecycleStatusKey = 12;
    SELECT @ROWCOUNT = @@ROWCOUNT;
    PRINT @ProcName+' : '+'Deleted '+CONVERT(VARCHAR(10), @ROWCOUNT)+' rows from #LifecycleStatusSnapshotCurrentWeek due to Student Withdrawal previous week';

    --
    -- Update records from the current week that are Withdrawn, where there is a corresponding NOT-Withdrawn last week.
    -- 
    -- We do this because once a Student is withdrawn in CMS, all their CostCenter and Org details are wiped, so we
    --     take that data from the previous week so we have a picture of what Center the Student was in when they
    --     withdrew...
    --

    UPDATE #LifecycleStatusSnapshotCurrentWeek
    SET
        idSite = prev.idSite,
        idSponsor = prev.idSponsor
    FROM #LifecycleStatusSnapshotPreviousWeek prev
        INNER JOIN #LifecycleStatusSnapshotCurrentWeek curr ON curr.idStudent = prev.idStudent
    WHERE curr.LifecycleStatusKey = 12
        AND prev.LifecycleStatusKey <> 12;
    SELECT @ROWCOUNT = @@ROWCOUNT;
    PRINT @ProcName+' : '+'Updated '+CONVERT(VARCHAR(10), @ROWCOUNT)+' rows in #LifecycleStatusSnapshotCurrentWeek due to Student Withdrawal this week';
             
    --

    SELECT 
		COALESCE(dm_dt.DateKey, -1) AS DateKey,
        COALESCE(dm_dt.AcademicYearNumber, 1900) AS CurrentAcademicYearNumber,
        COALESCE(dm_std.StudentKey, -1) AS StudentKey,
        COALESCE(dm_cctr.CostCenterKey, -1) AS CostCenterKey,
        COALESCE(dm_cctyp.CostCenterTypeKey, -1) AS CostCenterTypeKey,
        COALESCE(dm_org.OrgKey, -1) AS OrgKey,
        COALESCE(dm_loc.LocationKey, -1) AS LocationKey,
        COALESCE(dm_spr.SponsorKey, -1) AS SponsorKey,
        -2 AS TransactionCodeKey,
        COALESCE(dm_tLf.LifecycleStatusKey, -1) AS LifecycleStatusKey,
        COALESCE(dm_enr_dt.DateKey, -1) AS StudentFirstEnrollmentDateKey,
        COALESCE(dm_enr_dt.AcademicYearNumber, 1900) AS StudentFirstEnrollmentAcademicYearNumber,
        CASE
            WHEN dm_dt.AcademicYearNumber = dm_enr_dt.AcademicYearNumber
            THEN 1
            ELSE 0
        END AS StudentEnrolledCurrentAcademicYear,
        CASE
            WHEN dm_tLf.LifecycleStatusName = 'Withdrawn'
                    AND dm_dt.AcademicYearNumber = dm_enr_dt.AcademicYearNumber
            THEN 1
            ELSE 0
        END AS StudentWithdrewCurrentAcademicYear,
        CASE
            WHEN dm_tLf.LifecycleStatusName = 'Withdrawn'
                    AND dm_dt.BTSYearNumber = dm_enr_dt.BTSYearNumber
            THEN 1
            ELSE 0
        END AS StudentWithdrewCurrentBTSYear,
        CASE
            WHEN dm_dt.AcademicYearNumber - 1 = dm_enr_dt.AcademicYearNumber
            THEN 1
            ELSE 0
        END AS StudentEnrolledPreviousAcademicYear,
        @SourceSystem AS SourceSystem,
        @EDWRunDateTime AS EDWCreatedDate
    FROM #LifecycleStatusSnapshotCurrentWeek AS tLf
        LEFT JOIN BING_EDW..DimDate AS dm_dt ON tLf.AsOfFiscalWeekEndDate = dm_dt.FullDate
        LEFT JOIN BING_EDW.dbo.DimStudent AS dm_std ON tLf.idStudent = dm_std.StudentID
            AND dm_std.EDWEndDate IS NULL -- DimStudent is SCD2, so get the latest version
            AND dm_std.SourceSystem = @SourceSystem
        LEFT JOIN BING_EDW..DimDate dm_enr_dt ON dm_std.StudentFirstEnrollmentDate = dm_enr_dt.FullDate
        LEFT JOIN BING_EDW.dbo.DimCostCenter AS dm_cctr ON tLf.idSite = dm_cctr.CenterCMSID
			AND dm_cctr.EDWEndDate IS NULL -- DimCostCenter is SCD2, so get the latest version
        LEFT JOIN BING_EDW.dbo.DimCostCenterType AS dm_cctyp ON dm_cctr.CostCenterTypeID = dm_cctyp.CostCenterTypeID
        LEFT JOIN BING_EDW.dbo.DimOrganization AS dm_org ON dm_cctr.CostCenterNumber = dm_org.CostCenterNumber
			AND dm_org.EDWEndDate IS NULL -- DimOrganization is SCD2, so get the latest version
        LEFT JOIN BING_EDW.dbo.DimSponsor AS dm_spr ON tLf.idSponsor = dm_spr.SponsorID
            AND dm_spr.SourceSystem = @SourceSystem
			AND dm_spr.EDWEndDate IS NULL
        LEFT JOIN BING_EDW.dbo.DimLocation AS dm_loc ON dm_org.DefaultLocationID = dm_loc.LocationID
			AND dm_loc.EDWEndDate IS NULL -- DimLocation is SCD2, so get the latest version
        LEFT JOIN BING_EDW..DimLifecycleStatus AS dm_tLf ON tLf.LifecycleStatusKey = dm_tLf.LifecycleStatusKey
    WHERE 1 = 1;

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

