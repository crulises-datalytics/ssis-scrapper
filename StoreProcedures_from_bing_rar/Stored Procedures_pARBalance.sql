
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date         Modified By     Comments
    -- ----         -----------     --------
    -- 1/23/18		Valimineti		BNG-832 Performance tune AR Balance Snapshot - Corrected the stored proc to only fetch data for that week, 
	--																				instead of whole year or all years
	--
    -- 1/25/18		Valimineti      BNG-832 Performance tune AR Balance Snapshot - Corrected the stored proc to write only year 
	--										in AsOfFiscalWeek column for Transaction Yearly Snapshot dataset 
	--
	--02/27/18		hhebbalu		BNG-1271 Correct ETL logic for FactARBalanceSnapshot for CSS source - Reverted back the above mentioned changes to the oiginal condition.
	--										The fact will have snapshot of all the previous balances until today for every week with it's aging. 
	--03/06/18		hhebbalu		BNG-1271 Correct ETL logic for FactARBalanceSnapshot for CSS source - Changed the inner join to left join when joined with FiscalWeeks
	--								Also added code to age the balances from 2001 if the AR is from years before 2001 as the DimDate doesn't hold the dates beyond 2001
	--
	--03/08/18		hhebbalu		BNG-1271 Correct ETL logic for FactARBalanceSnapshot for CSS source - ARAgingDate was mapped as ASofDate. Changed it to show 
	--								Aging dates(which  are used to calculate ARAging days)
    -- ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE   PROCEDURE [dbo].[pARBalance] (@AsOfFiscalWeek int, @SourceCount int Output)
AS
begin

	--declare @AsOfFiscalWeek        int = 201101;

	--Balance Data through the end of the previous year
	select ctr_no, tap_id, fam_no, cust_code, 
	amount_ar        = cast(sum(amount_ar) as numeric(19,2)), 
	amount_prepay    = cast(sum(amount_prepay) as numeric(19,2)),
	last_week_ar     = max(cast(yr as varchar) + right('00' + cast(last_week_ar      as varchar), 2)),
	last_week_prepay = max(cast(yr as varchar) + right('00' + cast(last_week_prepay  as varchar), 2)),
	last_week_paid   = max(cast(yr as varchar) + right('00' + cast(last_week_payment as varchar), 2))
	into #BalancePreviousYears
	from vTransactionYearlySnapshot
	where yr < left(cast(@AsOfFiscalWeek as varchar), 4) -- Get all previous years for the group by
	--and ctr_no in ('0044', '4490')
	group by ctr_no, tap_id, fam_no, cust_code
	having cast(sum(amount_ar) as numeric(19,2)) <> 0 or cast(sum(amount_prepay) as numeric(19,2)) <> 0
	order by 1, 2

	select ctr_no, tap_id, fam_no, cust_code,
	amount_ar        = cast(sum(amount_ar) as numeric(19,2)), 
	amount_prepay    = cast(sum(amount_prepay) as numeric(19,2)),
	last_week_ar     = max(case when amount_ar     <> 0 then yr_week else null end),
	last_week_prepay = max(case when amount_prepay <> 0 then yr_week else null end),
	last_week_paid   = max(case when amount_paid   <> 0 then yr_week else null end)
	into #BalanceCurrentYear
	from vTransaction
	where yr = left(cast(@AsOfFiscalWeek as varchar), 4) -- Get all information for this years
	and week <= right(cast(@AsOfFiscalWeek as varchar), 2) -- only through the existing week
	--and ctr_no in ('0044', '4490')
	group by ctr_no, tap_id, fam_no, cust_code
	having cast(sum(amount_ar) as numeric(19,2)) <> 0 or cast(sum(amount_prepay) as numeric(19,2)) <> 0
	--order by 1, 2

	--select * from #BalanceCurrentYear
	--Create clustered indexes on the preceding datasets.
	CREATE CLUSTERED INDEX IX_BalancePreviousYears          ON  #BalancePreviousYears (ctr_no, tap_id);
	CREATE CLUSTERED INDEX IX_BalanceCurrentYear ON  #BalanceCurrentYear   (ctr_no, tap_id);

	with AsOfFiscalWeekEndDate as (
		select FiscalWeekEndDateKey, FiscalWeekNumber, FiscalWeekEndDate from BING_EDW.dbo.vDimFiscalWeek where FiscalWeekNumber = @AsOfFiscalWeek
	), FiscalWeeks as (
		select FiscalWeekEndDateKey, FiscalWeekNumber, FiscalWeekEndDate from BING_EDW.dbo.vDimFiscalWeek
	), Balance as (
		select 
		AsOfFiscalWeek = case when left((@AsOfFiscalWeek),4) < 2011 then left((@AsOfFiscalWeek),4) else @AsOfFiscalWeek end,
		--AsOfFiscalWeekEndDate = e.FiscalWeekEndDate,
		--as_of_week  = e.FiscalWeekNumber,
		aging_weeks_ar      = case when amount_ar      <> 0 then DATEDIFF(dd, b.FiscalWeekEndDate, e.FiscalWeekEndDate) / 7 else null end, 
		aging_weeks_prepay  = case when amount_prepay  <> 0 then DATEDIFF(dd, c.FiscalWeekEndDate, e.FiscalWeekEndDate) / 7 else null end, 
		aging_weeks_payment = DATEDIFF(dd, d.FiscalWeekEndDate, e.FiscalWeekEndDate) / 7, 
		d.FiscalWeekEndDate as AgingPaymentDate,c.FiscalWeekEndDate AS AgingPrepayDate, b.FiscalWeekEndDate AS AgingARDate,
		a.* 
		from (
			select 
			ctr_no        = coalesce(a.ctr_no, b.ctr_no), 
			fam_no        = coalesce(a.fam_no, b.fam_no),
			cust_code     = coalesce(a.cust_code, b.cust_code),
			amount_ar     = coalesce(a.amount_ar, 0) + coalesce(b.amount_ar, 0),
			amount_prepay = coalesce(a.amount_prepay, 0) + coalesce(b.amount_prepay, 0),
			last_week_ar     = case when oa.last_week_ar	  <   200101  then  200101   else oa.last_week_ar end,		
			--DimDate holds the data only from 2001. The AR/PP/PY with year<2001 will have null weeks when joined with vDimFiscalWeek. So any AR/PP/PY which has year<2001 will be forcefully aged from 2001
			last_week_prepay = case when oa.last_week_prepay  <   200101  then  200101   else oa.last_week_prepay end,
			last_week_paid   = case when oa.last_week_paid	  <   200101  then  200101   else oa.last_week_paid end
			from #BalanceCurrentYear a
			full outer join #BalancePreviousYears b on a.ctr_no = b.ctr_no and a.tap_id = b.tap_id
			outer apply(values(
				case when coalesce(a.last_week_ar, 0)     > coalesce(b.last_week_ar, 0)     then a.last_week_ar     else b.last_week_ar end,
				case when coalesce(a.last_week_prepay, 0) > coalesce(b.last_week_prepay, 0) then a.last_week_prepay else b.last_week_prepay end,
				case when coalesce(a.last_week_paid, 0)   > coalesce(b.last_week_paid, 0)   then a.last_week_paid   else b.last_week_paid end
			) ) oa (last_week_ar, last_week_prepay, last_week_paid)
			--where coalesce(a.fam_no, b.fam_no) = '1398'
		) a
		left join FiscalWeeks b on a.last_week_ar     = b.FiscalWeekNumber --the records were dropped when the dates were null. So changed the inner join to Left	--BNG-1271
		left join FiscalWeeks c on a.last_week_prepay = c.FiscalWeekNumber --the records were dropped when the dates were null. So changed the inner join to Left	--BNG-1271
		left join FiscalWeeks d on a.last_week_paid   = d.FiscalWeekNumber --the records were dropped when the dates were null. So changed the inner join to Left	--BNG-1271
		cross join AsOfFiscalWeekEndDate e
		where amount_ar <> 0 or amount_prepay <> 0 or last_week_paid is not null
	)
	select AsOfFiscalWeek, ctr_no, fam_no, cust_code, 
	ARBalanceType = 1, ARAgingDate = isnull(AgingPaymentDate,AgingARDate),
	ARAgingDays = 7 * isnull(aging_weeks_payment,aging_weeks_ar), ARBalanceAmount = amount_ar
 -- defaulting the date to latest aging_weeks_ar if there is null in Aging_Weeks_Payment(There are some cases of null)
	from Balance
	where amount_ar <> 0
	union
	select AsOfFiscalWeek, ctr_no, fam_no, cust_code,
	--swap sign on amount_prepay because we want the dataset to be additive with amount_ar
	ARBalanceType = 2, 
	ARAgingDate = isnull(AgingPrepayDate,AgingARDate),
	ARAgingDays = 7 * isnull(aging_weeks_prepay,aging_weeks_ar), 0 - amount_prepay		-- defaulting the date to latest aging_weeks_ar if there is null in Aging_Weeks_Payment(There are some cases of null)
	from Balance
	where amount_prepay <> 0

set @SourceCount =  @@Rowcount;

end