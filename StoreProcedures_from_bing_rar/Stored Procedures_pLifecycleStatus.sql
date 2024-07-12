
--select * from BING_EDW.dbo.DimLifecycleStatus
--insert into LifecycleStatusSnapshot
--exec pLifecycleStatus 201703

CREATE   PROCEDURE [dbo].[pLifecycleStatus](@FiscalWeek int, @SourceCount int Output)
as
begin

	declare @LifecycleStatuses TABLE              (ctr_no varchar(4), fam_no varchar(4), stu_no varchar(4), LifecycleStatusKey int)
	create table #EnrollmentsLast12Weeks          (ctr_no varchar(4), fam_no varchar(4), stu_no varchar(4))
	create table #EnrollmentsLast3WeeksContiguous (ctr_no varchar(4), fam_no varchar(4), stu_no varchar(4))
	create table #EnrollmentsCurrentWeek          (ctr_no varchar(4), fam_no varchar(4), stu_no varchar(4))
	create table #EnrollmentsPriorWeek            (ctr_no varchar(4), fam_no varchar(4), stu_no varchar(4))
	create table #Registrations                   (ctr_no varchar(4), fam_no varchar(4))

	-------------

	declare @Year            int =  left(cast(@FiscalWeek as varchar), 4)
	declare @Week            int = right(cast(@FiscalWeek as varchar), 2)
	declare @CurrentDataWeek int = (select max(cast(yr as varchar) + right('00' + cast(last_week_ar as varchar), 2)) from TransactionYearlySnapshot);
	
	declare @FiscalWeekEndDateKey        int = (select FiscalWeekEndDateKey from BING_EDW.dbo.vDimFiscalWeek where FiscalWeekNumber = @FiscalWeek)
	declare @FiscalWeekShortTermStart    int = dbo.fnFiscalWeekAdd(@FiscalWeek, 1)
	declare @FiscalWeekLongTermStart     int = dbo.fnFiscalWeekAdd(@FiscalWeek, 12)
	declare @FiscalWeekARStart           int = dbo.fnFiscalWeekAdd(@FiscalWeek, 3  - 1)
	declare @FiscalWeekRegistrationStart int = dbo.fnFiscalWeekAdd(@FiscalWeek, 20 - 1);
	--select @FiscalWeek, @FiscalWeekARStart


	
	--All Students enrolled in the current week
	insert into #EnrollmentsCurrentWeek
	select distinct ctr_no, fam_no, stu_no
	from Csptrand
	where tx_type = 'TU' 
	and yr = @Year and [week] = @Week

	--All Students ever enrolled, with start and end weeks
	select *,
	PastEnrollment    = case when @FiscalWeek > first_week_tuition then 1 else 0 end,
	CurrentEnrollment = case when exists(select 1 from #EnrollmentsCurrentWeek as b where b.ctr_no = a.ctr_no and b.fam_no = a.fam_no and b.stu_no = a.stu_no) then 1 else 0 end,
	FutureEnrollment  = case when @FiscalWeek < last_week_tuition then 1 else 0 end
	into #EnrollmentsAll
	from (
		select ctr_no, fam_no, stu_no, first_week_tuition = min(first_week_tuition), last_week_tuition = max(last_week_tuition)
		from (
			select ctr_no, fam_no, stu_no, 
			first_week_tuition = cast(yr as varchar) + right('00' + cast(first_week_tuition as varchar), 2), 
			last_week_tuition  = cast(yr as varchar) + right('00' + cast(last_week_tuition  as varchar), 2)
			from TransactionYearlySnapshot
			where stu_no is not null
			and first_week_tuition is not null
		  -- remove year filter because we need to get all enrollemnts across time
			union all
			select ctr_no, fam_no, stu_no,
			first_week_tuition = cast(yr as varchar) + right('00' + cast(min(week) as varchar), 2),
			last_week_tuition = cast(yr as varchar) + right('00' + cast(max(week) as varchar), 2)
			from vTransaction
			where yr > (select max(yr) from TransactionYearlySnapshot)
			 AND tx_type = 'TU'
			 and stu_no is not null
			 group by ctr_no, fam_no, stu_no, yr
			 -- add in recent data that isn't in the TransactionYearlySnapshot
		) a

		group by ctr_no, fam_no, stu_no
	) a

	--All Families registered in the past 20 weeks, through the current week
	if left(@FiscalWeekRegistrationStart, 4) = @Year
		insert into #Registrations
		select distinct ctr_no, fam_no
		from csptrand
		where tx_code = 'RGPREPAY' and amount < 0
		and yr = @Year and [week] between right(@FiscalWeekRegistrationStart, 2) and @Week
	else
		insert into #Registrations
		select distinct ctr_no, fam_no
		from csptrand
		where tx_code = 'RGPREPAY' and amount < 0
		and yr = left(@FiscalWeekRegistrationStart, 4) and [week] >= right(@FiscalWeekRegistrationStart, 2) -- corrected logic to get the acurate time frame
		union
		select distinct ctr_no, fam_no
		from csptrand
		where tx_code = 'RGPREPAY' and amount < 0
		and yr = @Year and [week] <= @Week

	--All Students enrolled in the previous 12 weeks
	if left(@FiscalWeekLongTermStart, 4) = left(@FiscalWeekShortTermStart, 4)
		insert into #EnrollmentsLast12Weeks
		select distinct ctr_no, fam_no, stu_no 
		from csptrand
		where tx_type = 'TU'
		and yr = left(@FiscalWeekLongTermStart, 4) and week between right(@FiscalWeekLongTermStart, 2) and right(@FiscalWeekShortTermStart, 2)
	else
		insert into #EnrollmentsLast12Weeks
		select distinct ctr_no, fam_no, stu_no 
		from csptrand
		where tx_type = 'TU'
		  and yr = left(@FiscalWeekLongTermStart, 4) and [week] >= right(@FiscalWeekLongTermStart, 2) 
		union
		select distinct ctr_no, fam_no, stu_no 
		from csptrand
		where tx_type = 'TU'
		and yr = left(@FiscalWeekShortTermStart, 4) and [week] <= right(@FiscalWeekShortTermStart, 2) -- corrected logic to get the acurate time frame

	--All Students enrolled contiguously for three weeks, through this week
	if left(@FiscalWeekARStart, 4) = @Year
		insert into #EnrollmentsLast3WeeksContiguous
		select ctr_no, fam_no, stu_no--, tuition_week_count = count(distinct [week]) 
		from csptrand
		where tx_type = 'TU'
		and yr = @Year and week between right(@FiscalWeekARStart, 2) and @Week
		group by ctr_no, fam_no, stu_no
		having count(distinct [week]) = 3
	else
		insert into #EnrollmentsLast3WeeksContiguous
		select ctr_no, fam_no, stu_no
		from (
		      select ctr_no, fam_no, stu_no, [week]
		      from csptrand
		      where tx_type = 'TU'
			 and yr = left(@FiscalWeekARStart, 4) and [week] >= right(@FiscalWeekARStart, 2)
			 union
		      select ctr_no, fam_no, stu_no, [week]
		      from csptrand
		      where tx_type = 'TU'
			 and yr = @Year and [week] <= @Week
		) as a
		group by ctr_no, fam_no, stu_no
		having count(distinct [week]) = 3


	--All Students enrolled in the previous week
	insert into #EnrollmentsPriorWeek
	select distinct ctr_no, fam_no, stu_no
	from Csptrand
	where tx_type = 'TU' 
	and yr = left(@FiscalWeekShortTermStart, 4) and [week] = right(@FiscalWeekShortTermStart, 2)

	--All students reserved in the current week
	select distinct ctr_no, fam_no, stu_no
	into #Reservations
	from csptrand
	where tx_code = 'RES'
	and yr = @Year and [week] = @Week

	select ctr_no, fam_no
	into #AtRiskNoPayment
	from ARBalanceSnapshot
	where ARBalanceType = 1
	and ARAgingDays >= 14
	and AsOfFiscalWeek = @FiscalWeek

	--Create clustered indexes on the preceding datasets.
	CREATE CLUSTERED INDEX IX_EnrollmentsLast12Weeks          ON #EnrollmentsLast12Weeks (ctr_no, fam_no, stu_no);
	CREATE CLUSTERED INDEX IX_EnrollmentsLast3WeeksContiguous ON #EnrollmentsLast3WeeksContiguous (ctr_no, fam_no, stu_no);
	CREATE CLUSTERED INDEX IX_Registrations                   ON #Registrations (ctr_no, fam_no);
	CREATE CLUSTERED INDEX IX_Reservations                    ON #Reservations (ctr_no, fam_no);
	CREATE CLUSTERED INDEX IX_EnrollmentsCurrentWeek          ON #EnrollmentsCurrentWeek (ctr_no, fam_no, stu_no);
	CREATE CLUSTERED INDEX IX_EnrollmentsPriorWeek            ON #EnrollmentsPriorWeek (ctr_no, fam_no, stu_no);
	CREATE CLUSTERED INDEX IX_EnrollmentsAll                  ON #EnrollmentsAll (ctr_no, fam_no, stu_no);
	CREATE CLUSTERED INDEX IX_AtRiskNoPayment                 ON #AtRiskNoPayment (ctr_no, fam_no);

	select a.ctr_no, a.fam_no, a.stu_no, 
	PastEnrollmentPriorWeek   = case when b.stu_no is not null then 1 else 0 end,
	PastEnrollmentLast12Weeks = case when c.stu_no is not null then 1 else 0 end,
	PastEnrollment, CurrentEnrollment, FutureEnrollment
	into #Enrollments
	from #EnrollmentsAll a
	left join #EnrollmentsPriorWeek   b on a.ctr_no = b.ctr_no and a.fam_no = b.fam_no and a.stu_no = b.stu_no
	left join #EnrollmentsLast12Weeks c on a.ctr_no = c.ctr_no and a.fam_no = c.fam_no and a.stu_no = c.stu_no
	left join #EnrollmentsCurrentWeek d on a.ctr_no = d.ctr_no and a.fam_no = d.fam_no and a.stu_no = d.stu_no

	CREATE CLUSTERED INDEX IX_Enrollments                     ON #Enrollments (ctr_no, fam_no, stu_no);

	with LifecycleStatuses as (
		--Pre-Enrolled (New and Existing)
		select 
		a.ctr_no, a.fam_no, a.stu_no, LifecycleStatusKey = case when PastEnrollment = 1 then 2 else 1 end
		from #EnrollmentsAll a
		inner join #Registrations b on a.ctr_no = b.ctr_no and a.fam_no = b.fam_no
		left join #EnrollmentsCurrentWeek c on a.ctr_no = c.ctr_no and a.fam_no = c.fam_no and a.stu_no = c.stu_no
		where c.stu_no is null
	union
		--New
		select a.ctr_no, a.fam_no, a.stu_no, LifecycleStatusKey = 3
		from #EnrollmentsCurrentWeek a
		inner join #EnrollmentsAll b on a.ctr_no = b.ctr_no and a.fam_no = b.fam_no and a.stu_no = b.stu_no
		where PastEnrollment = 0
	union
		--Enrolled
		select ctr_no, fam_no, stu_no, LifecycleStatusKey = 5
		from #EnrollmentsCurrentWeek
	union
		--Reserved
		select ctr_no, fam_no, stu_no, LifecycleStatusKey = 6
		from #Reservations
	union
		--At Risk (A/R)
		select a.ctr_no, a.fam_no, stu_no, LifecycleStatusKey = 8
		from #EnrollmentsLast3WeeksContiguous a
		inner join #AtRiskNoPayment b on a.ctr_no = b.ctr_no and a.fam_no = b.fam_no
	union
		--Re-Enrolled (Short-Term)
		select ctr_no, fam_no, stu_no, LifecycleStatusKey = 9 
		from #Enrollments
		where CurrentEnrollment       = 1
		  and PastEnrollmentPriorWeek = 0
		  and PastEnrollment          = 1
	union
		--Re-Enrolled (Long-Term)
		select ctr_no, fam_no, stu_no, LifecycleStatusKey = 10
		from #Enrollments
		where CurrentEnrollment         = 1
		  and PastEnrollmentLast12Weeks = 0
		  and PastEnrollment            = 1
	union
		--Withdrawing
		select ctr_no, fam_no, stu_no, LifecycleStatusKey = 11
		from #Enrollments
		where CurrentEnrollment = 1
		  and FutureEnrollment  = 0
		  and @FiscalWeek <> @CurrentDataWeek --if we're looking at data for current data week, then there can be no Withdrawals
	union
		--Withdrawn
		select ctr_no, fam_no, stu_no, LifecycleStatusKey = 12
		from #Enrollments
		where CurrentEnrollment = 0
		  and PastEnrollment    = 1
	)
	insert into @LifecycleStatuses
	select * from LifecycleStatuses

	--select * from @LifecycleStatuses
	--order by 4, 1, 2, 3

	--Determine a single Lifecycle Status for each Student, as determined by the Importance order.
	select AsOfFiscalWeek = @FiscalWeek, a.ctr_no, a.fam_no, a.stu_no, b.LifecycleStatusKey
	from @LifecycleStatuses a
	inner join BING_EDW.dbo.DimLifecycleStatus b on a.LifecycleStatusKey = b.LifecycleStatusKey
	inner join (
		select ctr_no, fam_no, stu_no, min(LifecycleStatusImportance) as LifecycleStatusImportance 
		from @LifecycleStatuses a
		inner join BING_EDW.dbo.DimLifecycleStatus b on a.LifecycleStatusKey = b.LifecycleStatusKey
		group by ctr_no, fam_no, stu_no
	) c on a.ctr_no = c.ctr_no and a.fam_no = c.fam_no and a.stu_no = c.stu_no and b.LifecycleStatusImportance = c.LifecycleStatusImportance

set @SourceCount =  @@Rowcount;	
end