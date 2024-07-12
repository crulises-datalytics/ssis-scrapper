 CREATE PROCEDURE [dbo].[spADP_Update_PS_JOB_B10]
	@LastSuccessfulLoadTime DATETIME,
	@ProcessingYear			INT,
	@DebugMode				INT       = NULL
AS
	-- ================================================================================
	-- 
	-- Stored Procedure:   spADP_Update_PS_JOB_B10
	--
	-- Purpose:            Fetches records from [dbo].[ADP_PS_JOB_B0] for Update into
	--						[dbo].[ADP_PS_JOB_B10]
	--
	-- Parameters:         @LastSuccessfulLoadTime
	--                     @ProcessingYear
	--                     @DebugMode - Used just for development & debug purposes,
	--                         outputting helpful info back to the caller.  Not
	--                         required for Production, and does not affect any
	--                         core logic.
	--
	-- --------------------------------------------------------------------------------
	--
	-- Change Log:		   
	-- ----------
	--
	-- Date				Modified By			Comments
	-- ----				-----------			--------
	-- 07/26/2023		Aniket				BI-9131: ADP_GL Loading Logic Change		 
	-- ================================================================================
	BEGIN
		SET NOCOUNT ON;

		--
		-- Housekeeping Variables
		--
		DECLARE @ProcName NVARCHAR(500) = OBJECT_NAME(@@PROCID);
		DECLARE @DebugMsg NVARCHAR(500) = '';
		DECLARE @UpdateCount INT=0;
		
		--
		IF @DebugMode = 1
			BEGIN
				SELECT @DebugMsg = @ProcName + N' : ' + CONVERT(NVARCHAR(25), GETDATE(), 121) + N' - Starting.';
				RAISERROR (@DebugMsg, 0, 0) WITH NOWAIT;
			END;

		BEGIN TRY;
			--
			-- Create Temporary Table to get data from Source
			--
			IF NOT EXISTS (SELECT 1 FROM [sys].[tables] WHERE [name] = 'ADP_PS_JOB_B0_TMP' AND [schema_id] = SCHEMA_ID('dbo'))
				BEGIN
					CREATE TABLE [dbo].[ADP_PS_JOB_B0_TMP] (
						[emplid] [varchar](16) NOT NULL,
						[empl_rcd_nbr] [int] NULL,
						[effdt] [date] NOT NULL,
						[effseq] [tinyint] NOT NULL,
						[deptid] [varchar](6) NOT NULL,
						[jobcode] [varchar](3) NOT NULL,
						[position_nbr] [varchar](20) NULL,
						[empl_status] [varchar](1) NULL,
						[action] [varchar](3) NULL,
						[action_dt] [date] NULL,
						[action_reason] [nvarchar](3) NULL,
						[location] [varchar](5) NULL,
						[job_entry_dt] [date] NULL,
						[dept_entry_dt] [date] NULL,
						[position_entry_dt] [date] NULL,
						[shift] [varchar](20) NULL,
						[reg_temp] [varchar](1) NULL,
						[full_part_time] [varchar](1) NULL,
						[flsa_status] [varchar](1) NULL,
						[officer_cd] [varchar](1) NULL,
						[company] [varchar](3) NULL,
						[paygroup] [nvarchar](3) NULL,
						[empl_type] [varchar](1) NULL,
						[holiday_schedule] [varchar](3) NULL,
						[std_hours] [numeric](4, 2) NULL,
						[eeo_class] [varchar](1) NULL,
						[empl_class] [varchar](20) NULL,
						[sal_admin_plan] [varchar](20) NULL,
						[grade] [varchar](20) NULL,
						[grade_entry_dt] [date] NULL,
						[step] [int] NULL,
						[step_entry_dt] [date] NULL,
						[gl_pay_type] [varchar](20) NULL,
						[acct_cd] [varchar](20) NULL,
						[earns_dist_type] [varchar](1) NULL,
						[comp_frequency] [varchar](1) NULL,
						[comprate] [numeric](12, 4) NULL,
						[change_amt] [numeric](12, 4) NULL,
						[change_pct] [numeric](6, 3) NULL,
						[annual_rt] [numeric](10, 2) NULL,
						[monthly_rt] [numeric](9, 2) NULL,
						[hourly_rt] [numeric](8, 4) NULL,
						[annl_benef_base_rt] [numeric](10, 2) NULL,
						[shift_rt] [numeric](7, 4) NULL,
						[shift_factor] [numeric](4, 3) NULL,
						[currency_cd] [varchar](3) NULL,
						[xfer_accum] [varchar](1) NULL,
						[xfer_deductions] [varchar](1) NULL,
						[xfer_sui_sdi] [varchar](1) NULL,
						[xfer_tax] [varchar](1) NULL,
						[al_empl_status] [varchar](1) NULL,
						[al_pay_frequency] [varchar](1) NULL,
						[al_std_hours] [numeric](5, 2) NULL,
						[clock_nbr] [nvarchar](5) NULL,
						[data_control] [varchar](20) NULL,
						[file_nbr] [varchar](16) NULL,
						[file_nbr_status] [varchar](1) NULL,
						[gross_calc] [varchar](1) NULL,
						[home_department] [varchar](6) NULL,
						[home_jobcost_nbr] [varchar](20) NULL,
						[hourly_rt_2] [numeric](7, 4) NULL,
						[hourly_rt_3] [numeric](7, 4) NULL,
						[rate_1] [numeric](9, 4) NULL,
						[rate_type] [varchar](1) NULL,
						[reporting_location] [varchar](5) NULL,
						[source_file_nbr] [varchar](6) NULL,
						[title] [varchar](20) NULL,
						[variable_plan_cd] [varchar](20) NULL,
						[var_plan_entry_dt] [date] NULL,
						[allow_draw] [varchar](1) NULL,
						[split_base] [int] NULL,
						[split_variable] [int] NULL,
						[override_position] [varchar](1) NULL,
						[updated_by_posn] [varchar](1) NULL,
						[tf_file_id] [int] NULL,
						[paygroup_2] [varchar](1) NULL,
						[alt_lang_chk] [varchar](1) NULL,
						[workers_comp_cd] [varchar](4) NULL,
						[from_merit_wrksht] [varchar](1) NULL,
						[overtime_eligible] [varchar](1) NULL,
						[primary_job] [varchar](1) NULL,
						[rate_code] [varchar](20) NULL,
						[retro_pay_eligible] [varchar](1) NULL,
						[retro_pay_start_dt] [date] NULL,
						[leaveplan_eligible] [varchar](1) NULL,
						[flsa_ot_ind] [varchar](1) NULL,
						[reports_to_id] [varchar](6) NULL,
						[geog_diff_id] [varchar](20) NULL,
						[cost_num] [varchar](9) NULL,
						[comp_entry_dt] [date] NULL,
						[union_cd] [varchar](1) NULL,
						[union_seniority_dt] [date] NULL,
						[barg_unit] [varchar](20) NULL,
						[barg_seniority_dt] [date] NULL,
						[next_step_dt] [date] NULL,
						[contract_job] [varchar](1) NULL,
						[retro_job_status] [varchar](1) NULL,
						[excld_tot_hrs_wrk] [varchar](1) NULL,
						[pay_stmt_msg_id] [varchar](20) NULL,
						[datalakeinserttime] [datetime2](7) NOT NULL,
						[RowHash] [binary](16) NOT NULL
					);
				END;

			IF NOT EXISTS (SELECT 1 FROM [dbo].[ADP_PS_JOB_B0_TMP])
				BEGIN
					INSERT INTO [dbo].[ADP_PS_JOB_B0_TMP] (
						[emplid],[empl_rcd_nbr],[effdt],[effseq],[deptid],[jobcode],[position_nbr],[empl_status],[action],[action_dt],[action_reason]
						,[location],[job_entry_dt],[dept_entry_dt],[position_entry_dt],[shift],[reg_temp],[full_part_time],[flsa_status],[officer_cd]
						,[company],[paygroup],[empl_type],[holiday_schedule],[std_hours],[eeo_class],[empl_class],[sal_admin_plan],[grade],[grade_entry_dt]
						,[step],[step_entry_dt],[gl_pay_type],[acct_cd],[earns_dist_type],[comp_frequency],[comprate],[change_amt],[change_pct],[annual_rt]
						,[monthly_rt],[hourly_rt],[annl_benef_base_rt],[shift_rt],[shift_factor],[currency_cd],[xfer_accum],[xfer_deductions],[xfer_sui_sdi]
						,[xfer_tax],[al_empl_status],[al_pay_frequency],[al_std_hours],[clock_nbr],[data_control],[file_nbr],[file_nbr_status],[gross_calc]
						,[home_department],[home_jobcost_nbr],[hourly_rt_2],[hourly_rt_3],[rate_1],[rate_type],[reporting_location],[source_file_nbr],[title]
						,[variable_plan_cd],[var_plan_entry_dt],[allow_draw],[split_base],[split_variable],[override_position],[updated_by_posn],[tf_file_id]
						,[paygroup_2],[alt_lang_chk],[workers_comp_cd],[from_merit_wrksht],[overtime_eligible],[primary_job],[rate_code],[retro_pay_eligible]
						,[retro_pay_start_dt],[leaveplan_eligible],[flsa_ot_ind],[reports_to_id],[geog_diff_id],[cost_num],[comp_entry_dt],[union_cd]
						,[union_seniority_dt],[barg_unit],[barg_seniority_dt],[next_step_dt],[contract_job],[retro_job_status],[excld_tot_hrs_wrk],[pay_stmt_msg_id]
						,[datalakeinserttime],[RowHash]
					)
					SELECT
						CASE 
							WHEN ISNUMERIC( [emplid] ) = 0 AND ISNUMERIC( [file_nbr] ) = 1 AND  RIGHT( [emplid], 4 ) = RIGHT( [file_nbr], 4 ) 
								THEN [file_nbr] ELSE [emplid] 
						END AS [emplid]
						,[empl_rcd_nbr],[effdt],[effseq],[deptid],[jobcode],[position_nbr],[empl_status],[action],[action_dt],[action_reason]
						,[location],[job_entry_dt],[dept_entry_dt],[position_entry_dt],[shift],[reg_temp],[full_part_time],[flsa_status],[officer_cd]
						,[company],[paygroup],[empl_type],[holiday_schedule],[std_hours],[eeo_class],[empl_class],[sal_admin_plan],[grade],[grade_entry_dt]
						,[step],[step_entry_dt],[gl_pay_type],[acct_cd],[earns_dist_type],[comp_frequency],[comprate],[change_amt],[change_pct],[annual_rt]
						,[monthly_rt],[hourly_rt],[annl_benef_base_rt],[shift_rt],[shift_factor],[currency_cd],[xfer_accum],[xfer_deductions],[xfer_sui_sdi]
						,[xfer_tax],[al_empl_status],[al_pay_frequency],[al_std_hours],[clock_nbr],[data_control]
						,CASE
							WHEN ISNUMERIC( [emplid] ) = 1 AND ( [file_nbr] = '' OR [file_nbr] IS NULL OR ISNUMERIC( [file_nbr] ) = 0)  
								THEN [emplid] ELSE [file_nbr]
						END AS [file_nbr]
						,[file_nbr_status],[gross_calc]
						,[home_department],[home_jobcost_nbr],[hourly_rt_2],[hourly_rt_3],[rate_1],[rate_type],[reporting_location],[source_file_nbr],[title]
						,[variable_plan_cd],[var_plan_entry_dt],[allow_draw],[split_base],[split_variable],[override_position],[updated_by_posn],[tf_file_id]
						,[paygroup_2],[alt_lang_chk],[workers_comp_cd],[from_merit_wrksht],[overtime_eligible],[primary_job],[rate_code],[retro_pay_eligible]
						,[retro_pay_start_dt],[leaveplan_eligible],[flsa_ot_ind],[reports_to_id],[geog_diff_id],[cost_num],[comp_entry_dt],[union_cd]
						,[union_seniority_dt],[barg_unit],[barg_seniority_dt],[next_step_dt],[contract_job],[retro_job_status],[excld_tot_hrs_wrk],[pay_stmt_msg_id]
						,[datalakeinserttime],[RowHash]
					FROM [dbo].[ADP_PS_JOB_B0]
					WHERE YEAR([effdt]) = @ProcessingYear
					AND [BaseCreatedDate] > @LastSuccessfulLoadTime;
				END;
			 
			UPDATE [E]
				SET [E].[empl_rcd_nbr] = [L].[empl_rcd_nbr],
					[E].[position_nbr] = [L].[position_nbr],
					[E].[effdt] = [L].[effdt],
					[E].[empl_status] = [L].[empl_status],
					[E].[action] = [L].[action],
					[E].[action_dt] = [L].[action_dt],
					[E].[action_reason] = [L].[action_reason],
					[E].[location] = [L].[location],
					[E].[job_entry_dt] = [L].[job_entry_dt],
					[E].[dept_entry_dt] = [L].[dept_entry_dt],
					[E].[position_entry_dt] = [L].[position_entry_dt],
					[E].[shift] = [L].[shift],
					[E].[reg_temp] = [L].[reg_temp],
					[E].[full_part_time] = [L].[full_part_time],
					[E].[flsa_status] = [L].[flsa_status],
					[E].[officer_cd] = [L].[officer_cd],
					[E].[company] = [L].[company],
					[E].[paygroup] = [L].[paygroup],
					[E].[empl_type] = [L].[empl_type],
					[E].[holiday_schedule] = [L].[holiday_schedule],
					[E].[std_hours] = [L].[std_hours],
					[E].[eeo_class] = [L].[eeo_class],
					[E].[empl_class] = [L].[empl_class],
					[E].[sal_admin_plan] = [L].[sal_admin_plan],
					[E].[grade] = [L].[grade],
					[E].[grade_entry_dt] = [L].[grade_entry_dt],
					[E].[step] = [L].[step],
					[E].[step_entry_dt] = [L].[step_entry_dt],
					[E].[gl_pay_type] = [L].[gl_pay_type],
					[E].[acct_cd] = [L].[acct_cd],
					[E].[earns_dist_type] = [L].[earns_dist_type],
					[E].[comp_frequency] = [L].[comp_frequency],
					[E].[comprate] = [L].[comprate],
					[E].[change_amt] = [L].[change_amt],
					[E].[change_pct] = [L].[change_pct],
					[E].[annual_rt] = [L].[annual_rt],
					[E].[monthly_rt] = [L].[monthly_rt],
					[E].[hourly_rt] = [L].[hourly_rt],
					[E].[annl_benef_Base_rt] = [L].[annl_benef_Base_rt],
					[E].[shift_rt] = [L].[shift_rt],
					[E].[shift_factor] = [L].[shift_factor],
					[E].[currency_cd] = [L].[currency_cd],
					[E].[xfer_accum] = [L].[xfer_accum],
					[E].[xfer_deductions] = [L].[xfer_deductions],
					[E].[xfer_sui_sdi] = [L].[xfer_sui_sdi],
					[E].[xfer_tax] = [L].[xfer_tax],
					[E].[al_empl_status] = [L].[al_empl_status],
					[E].[al_pay_frequency] = [L].[al_pay_frequency],
					[E].[al_std_hours] = [L].[al_std_hours],
					[E].[clock_nbr] = [L].[clock_nbr],
					[E].[data_control] = [L].[data_control],
					[E].[file_nbr] =    [L].[file_nbr]     ,
					[E].[file_nbr_status] = [L].[file_nbr_status],
					[E].[gross_calc] = [L].[gross_calc],
					[E].[home_department] = [L].[home_department],
					[E].[home_jobcost_nbr] = [L].[home_jobcost_nbr],
					[E].[hourly_rt_2] = [L].[hourly_rt_2],
					[E].[hourly_rt_3] = [L].[hourly_rt_3],
					[E].[rate_1] = [L].[rate_1],
					[E].[rate_type] = [L].[rate_type],
					[E].[reporting_location] = [L].[reporting_location],
					[E].[source_file_nbr] = [L].[source_file_nbr],
					[E].[title] = [L].[title],
					[E].[variable_plan_cd] = [L].[variable_plan_cd],
					[E].[var_plan_entry_dt] = [L].[var_plan_entry_dt],
					[E].[allow_draw] = [L].[allow_draw],
					[E].[split_base] = [L].[split_base],
					[E].[split_variable] = [L].[split_variable],
					[E].[override_position] = [L].[override_position],
					[E].[updated_by_posn] = [L].[updated_by_posn],
					[E].[tf_file_id] = [L].[tf_file_id],
					[E].[paygroup_2] = [L].[paygroup_2],
					[E].[alt_lang_chk] = [L].[alt_lang_chk],
					[E].[workers_comp_cd] = [L].[workers_comp_cd],
					[E].[from_merit_wrksht] = [L].[from_merit_wrksht],
					[E].[overtime_eligible] = [L].[overtime_eligible],
					[E].[primary_job] = [L].[primary_job],
					[E].[rate_code] = [L].[rate_code],
					[E].[retro_pay_eligible] = [L].[retro_pay_eligible],
					[E].[retro_pay_start_dt] = [L].[retro_pay_start_dt],
					[E].[leaveplan_eligible] = [L].[leaveplan_eligible],
					[E].[flsa_ot_ind] = [L].[flsa_ot_ind],
					[E].[reports_to_id] = [L].[reports_to_id],
					[E].[geog_diff_id] = [L].[geog_diff_id],
					[E].[cost_num] = [L].[cost_num],
					[E].[comp_entry_dt] = [L].[comp_entry_dt],
					[E].[union_cd] = [L].[union_cd],
					[E].[union_seniority_dt] = [L].[union_seniority_dt],
					[E].[barg_unit] = [L].[barg_unit],
					[E].[barg_seniority_dt] = [L].[barg_seniority_dt],
					[E].[next_step_dt] = [L].[next_step_dt],
					[E].[contract_job] = [L].[contract_job],
					[E].[retro_job_status] = [L].[retro_job_status],
					[E].[excld_tot_hrs_wrk] = [L].[excld_tot_hrs_wrk],
					[E].[pay_stmt_msg_id] = [L].[pay_stmt_msg_id],
					[E].[datalakeinserttime] = [L].[datalakeinserttime],
					[E].[RowHash] = [L].[RowHash],
					[E].[BaseUpdatedDate] = GETDATE(),
					[E].[BaseUpdatedBy] = SUSER_SNAME()
					FROM [HR_Base].[dbo].[ADP_PS_JOB_B10] [E]
					INNER JOIN [dbo].[ADP_PS_JOB_B0_TMP] [L]
						ON [E].[effdt] =   [L].[effdt]
						AND [E].[effseq] =  [L].[effseq]
						AND [E].[jobcode] = [L].[jobcode]
						AND [E].[deptid] =  [L].[deptid]
						AND CONVERT(VARCHAR(16), [E].[emplid]) = [L].[emplid]
					WHERE 
						[E].[RowHash] != [L].[RowHash];
   
			SELECT @UpdateCount = @@ROWCOUNT;
			SELECT @UpdateCount AS [UpdateCount];
		
		END TRY
		BEGIN CATCH
			IF (@@TRANCOUNT > 0)
				ROLLBACK TRAN;

		END CATCH;
   	 
	END;