CREATE PROCEDURE [dbo].[spADP_GetNew_PS_AL_CHK_DATA_B10]
	@LastSuccessfulLoadTime DATETIME,
	@ProcessingYear			INT,
	@DebugMode				INT       = NULL
AS
	-- ================================================================================
	-- 
	-- Stored Procedure:   spADP_GetNew_PS_AL_CHK_DATA_B10
	--
	-- Purpose:            Fetches records from [dbo].[ADP_PS_AL_CHK_DATA_B0] for insert into
	--					   [dbo].[ADP_PS_AL_CHK_DATA_B10]
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
	-- 07/26/2023		Suhas				BI-9604: B0 to B10 - Transform data to B10 Tables (Part 2/2)		 
	-- ================================================================================
	BEGIN
		SET NOCOUNT ON;

		--
		-- Housekeeping Variables
		--
		DECLARE @ProcName NVARCHAR(500) = OBJECT_NAME(@@PROCID);
		DECLARE @DebugMsg NVARCHAR(500) = '';

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
			IF NOT EXISTS (SELECT 1 FROM [sys].[tables] WHERE [name] = 'ADP_PS_AL_CHK_DATA_B0_TMP' AND [schema_id] = SCHEMA_ID('dbo'))
				BEGIN
					CREATE TABLE [dbo].[ADP_PS_AL_CHK_DATA_B0_TMP] (
						[paygroup] [varchar](3) NOT NULL,
						[file_nbr] [int] NOT NULL,
						[check_dt] [date] NOT NULL,
						[week_nbr] [tinyint] NOT NULL,
						[payroll_nbr] [tinyint] NOT NULL,
						[check_nbr] [bigint] NOT NULL,
						[entry_nbr] [tinyint] NOT NULL,
						[emplid] [int] NOT NULL,
						[empl_rcd_nbr] [tinyint] NULL,
						[pay_end_dt] [date] NOT NULL,
						[wgps_adv_pay_dt] [date] NULL,
						[ck_gross] [numeric](11, 2) NULL,
						[al_net_pay] [numeric](10, 2) NULL,
						[temp_deptid] [varchar](6) NULL,
						[home_department] [varchar](6) NULL,
						[check_ind] [varchar](1) NULL,
						[void_ind] [varchar](1) NULL,
						[tax_frequency] [varchar](1) NULL,
						[fed_tax_amt] [numeric](10, 2) NULL,
						[socsec_amt] [numeric](10, 2) NULL,
						[medicare_amt] [numeric](10, 2) NULL,
						[state_tax_cd] [varchar](4) NULL,
						[state_tax_amt] [numeric](10, 2) NULL,
						[state2_tax_cd] [varchar](4) NULL,
						[state2_tax_amt] [numeric](10, 2) NULL,
						[local_tax_cd] [varchar](4) NULL,
						[local_tax_amt] [decimal](10, 2) NULL,
						[local2_tax_cd] [varchar](4) NULL,
						[local2_tax_amt] [numeric](10, 2) NULL,
						[school_dist_amt] [numeric](10, 2) NULL,
						[sui_tax_cd] [varchar](4) NULL,
						[suisdi_amt] [numeric](10, 2) NULL,
						[reversed] [varchar](1) NULL,
						[suppress_voucher] [varchar](1) NULL,
						[chk_rec_type] [varchar](1) NULL,
						[cost_num] [varchar](9) NULL,
						[temp_cost_num] [nvarchar](10) NULL,
						[local4_tax_cd] [varchar](4) NULL,
						[local4_tax_amt] [numeric](10, 2) NULL,
						[local5_tax_cd] [varchar](4) NULL,
						[local5_tax_amt] [numeric](10, 2) NULL,
						[pay_begin_dt] [date] NULL,
						[sched_year] [varchar](4) NULL,
						[quarter_nbr] [varchar](1) NULL,
						[qtr_adjstmnt_ind] [varchar](1) NULL,
						[al_batch_nbr] [varchar](4) NULL,
						[othr_period_beg_dt] [date] NULL,
						[othr_period_end_dt] [date] NULL,
						[total_hrs_worked] [numeric](6, 2) NULL,
						[paycheck_tax_freq] [varchar](1) NULL,
						[adjustment_dt] [date] NULL,
						[adjustment_seq] [varchar](3) NULL,
						[fli_tax_amt] [numeric](10, 2) NULL,
						[mli_tax_amt] [numeric](10, 2) NULL,
						[sui_amt] [numeric](10, 2) NULL,
						[sdi_amt] [numeric](10, 2) NULL,
						[local9_tax_cd] [varchar](4) NULL,
						[local9_tax_amt] [decimal](10, 2) NULL,
						[local10_tax_cd] [varchar](4) NULL,
						[local10_tax_amt] [numeric](10, 2) NULL,
						[datalakeinserttime] [datetime2](7) NOT NULL,
						[RowHash] [binary](16) NOT NULL,
						[IsUpdated] [bit] NOT NULL
					);
				END;

			TRUNCATE TABLE [dbo].[ADP_PS_AL_CHK_DATA_B0_TMP];
			INSERT INTO [dbo].[ADP_PS_AL_CHK_DATA_B0_TMP] (
				[paygroup],[file_nbr],[check_dt],[week_nbr],[payroll_nbr],[check_nbr],[entry_nbr],[emplid],[empl_rcd_nbr],[pay_end_dt],[wgps_adv_pay_dt]
				,[ck_gross],[al_net_pay],[temp_deptid],[home_department],[check_ind],[void_ind],[tax_frequency],[fed_tax_amt],[socsec_amt],[medicare_amt]
				,[state_tax_cd],[state_tax_amt],[state2_tax_cd],[state2_tax_amt],[local_tax_cd],[local_tax_amt],[local2_tax_cd],[local2_tax_amt],[school_dist_amt]
				,[sui_tax_cd],[suisdi_amt],[reversed],[suppress_voucher],[chk_rec_type],[cost_num],[temp_cost_num],[local4_tax_cd],[local4_tax_amt],[local5_tax_cd]
				,[local5_tax_amt],[pay_begin_dt],[sched_year],[quarter_nbr],[qtr_adjstmnt_ind],[al_batch_nbr],[othr_period_beg_dt],[othr_period_end_dt]
				,[total_hrs_worked],[paycheck_tax_freq],[adjustment_dt],[adjustment_seq],[fli_tax_amt],[mli_tax_amt],[sui_amt],[sdi_amt],[local9_tax_cd]
				,[local9_tax_amt],[local10_tax_cd],[local10_tax_amt],[datalakeinserttime],[RowHash],[IsUpdated]
			)
			SELECT
				[paygroup]
				,CASE
					WHEN ISNUMERIC( [emplid] ) = 1 AND ( [file_nbr] = '' OR [file_nbr] IS NULL OR ISNUMERIC( [file_nbr] ) = 0)  
						THEN [emplid] ELSE [file_nbr]
				END AS [file_nbr]
				,[check_dt],[week_nbr],[payroll_nbr],[check_nbr],[entry_nbr]
				,CASE 
					WHEN ISNUMERIC( [emplid] ) = 0 AND ISNUMERIC( [file_nbr] ) = 1 AND  RIGHT( [emplid], 6 ) = [file_nbr] 
						THEN [file_nbr] ELSE [emplid] 
				END AS [emplid]
				,[empl_rcd_nbr],[pay_end_dt],[wgps_adv_pay_dt]
				,[ck_gross],[al_net_pay],[temp_deptid],[home_department],[check_ind],[void_ind],[tax_frequency],[fed_tax_amt],[socsec_amt],[medicare_amt]
				,[state_tax_cd],[state_tax_amt],[state2_tax_cd],[state2_tax_amt],[local_tax_cd],[local_tax_amt],[local2_tax_cd],[local2_tax_amt],[school_dist_amt]
				,[sui_tax_cd],[suisdi_amt],[reversed],[suppress_voucher],[chk_rec_type],[cost_num],[temp_cost_num],[local4_tax_cd],[local4_tax_amt],[local5_tax_cd]
				,[local5_tax_amt],[pay_begin_dt],[sched_year],[quarter_nbr],[qtr_adjstmnt_ind],[al_batch_nbr],[othr_period_beg_dt],[othr_period_end_dt]
				,[total_hrs_worked],[paycheck_tax_freq],[adjustment_dt],[adjustment_seq],[fli_tax_amt],[mli_tax_amt],[sui_amt],[sdi_amt],[local9_tax_cd]
				,[local9_tax_amt],[local10_tax_cd],[local10_tax_amt],[datalakeinserttime],[RowHash],0
			FROM [dbo].[ADP_PS_AL_CHK_DATA_B0]
			WHERE YEAR([pay_end_dt]) = @ProcessingYear
			AND [BaseCreatedDate] > @LastSuccessfulLoadTime;

			UPDATE [B]
				SET [home_department] = [C].[home_department],
					[IsUpdated] = 1
			FROM [dbo].[ADP_PS_AL_CHK_DATA_B0_TMP] [B]
			JOIN (
				SELECT
					[X].[paygroup], [X].[file_nbr], [X].[check_dt], [X].[week_nbr], [X].[payroll_nbr], [X].[check_nbr], [X].[entry_nbr], [X].[home_department],
					ROW_NUMBER() OVER (PARTITION BY [X].[paygroup], [X].[file_nbr], [X].[check_dt], [X].[week_nbr], [X].[payroll_nbr], [X].[check_nbr] ORDER BY [X].[entry_nbr]) AS [RowID]
				FROM [dbo].[ADP_PS_AL_CHK_DATA_B0_TMP] [X]
				JOIN (
					SELECT
						[paygroup], [file_nbr], [check_dt], [week_nbr], [payroll_nbr], [check_nbr], [entry_nbr], [home_department]
					FROM [dbo].[ADP_PS_AL_CHK_DATA_B0_TMP]
					WHERE [entry_nbr] = 0 AND (ISNULL(LTRIM(RTRIM([home_department])), '') = '')
				) AS [Y]
					ON [X].[paygroup] = [Y].[paygroup]
					AND [X].[file_nbr] = [Y].[file_nbr]
					AND [X].[check_dt] = [Y].[check_dt]
					AND [X].[week_nbr] = [Y].[week_nbr]
					AND [X].[payroll_nbr] = [Y].[payroll_nbr]
					AND [X].[check_nbr] = [Y].[check_nbr]
				WHERE [X].[entry_nbr] > 0 AND (ISNULL(LTRIM(RTRIM([X].[home_department])), '') <> '')
			) AS [C]
				ON [B].[paygroup] = [C].[paygroup]
				AND [B].[file_nbr] = [C].[file_nbr]
				AND [B].[check_dt] = [C].[check_dt]
				AND [B].[week_nbr] = [C].[week_nbr]
				AND [B].[payroll_nbr] = [C].[payroll_nbr]
				AND [B].[check_nbr] = [C].[check_nbr]
			WHERE [B].[entry_nbr] = 0
			AND (ISNULL(LTRIM(RTRIM([B].[home_department])), '') = '')
			AND [C].[RowID] = 1;

			UPDATE [B]
				SET [cost_num] = [C].[cost_num],
					[IsUpdated] = 1
			FROM [dbo].[ADP_PS_AL_CHK_DATA_B0_TMP] [B]
			JOIN (
				SELECT
					[X].[paygroup], [X].[file_nbr], [X].[check_dt], [X].[week_nbr], [X].[payroll_nbr], [X].[check_nbr], [X].[entry_nbr], [X].[cost_num],
					ROW_NUMBER() OVER (PARTITION BY [X].[paygroup], [X].[file_nbr], [X].[check_dt], [X].[week_nbr], [X].[payroll_nbr], [X].[check_nbr] ORDER BY [X].[entry_nbr]) AS [RowID]
				FROM [dbo].[ADP_PS_AL_CHK_DATA_B0_TMP] [X]
				JOIN (
					SELECT
						[paygroup], [file_nbr], [check_dt], [week_nbr], [payroll_nbr], [check_nbr], [entry_nbr], [cost_num]
					FROM [dbo].[ADP_PS_AL_CHK_DATA_B0_TMP]
					WHERE [entry_nbr] = 0 AND (ISNULL(LTRIM(RTRIM([cost_num])), '') = '')
				) AS [Y]
					ON [X].[paygroup] = [Y].[paygroup]
					AND [X].[file_nbr] = [Y].[file_nbr]
					AND [X].[check_dt] = [Y].[check_dt]
					AND [X].[week_nbr] = [Y].[week_nbr]
					AND [X].[payroll_nbr] = [Y].[payroll_nbr]
					AND [X].[check_nbr] = [Y].[check_nbr]
				WHERE [X].[entry_nbr] > 0 AND (ISNULL(LTRIM(RTRIM([X].[cost_num])), '') <> '')
			) AS [C]
				ON [B].[paygroup] = [C].[paygroup]
				AND [B].[file_nbr] = [C].[file_nbr]
				AND [B].[check_dt] = [C].[check_dt]
				AND [B].[week_nbr] = [C].[week_nbr]
				AND [B].[payroll_nbr] = [C].[payroll_nbr]
				AND [B].[check_nbr] = [C].[check_nbr]
			WHERE [B].[entry_nbr] = 0
			AND (ISNULL(LTRIM(RTRIM([B].[cost_num])), '') = '')
			AND [C].[RowID] = 1;

			UPDATE [Target]
			SET [Target].[RowHash] = [Source].[RowHash]
			FROM 
			(SELECT [paygroup]
				,[file_nbr]
				,[check_dt]
				,[week_nbr]
				,[payroll_nbr]
				,[check_nbr]
				,[entry_nbr]
				,[emplid]
				,HASHBYTES('MD5',
			(SELECT
				[paygroup]
				,[file_nbr]
				,[check_dt]
				,[week_nbr]
				,[payroll_nbr]
				,[check_nbr]
				,[entry_nbr]
				,[emplid]
				,[empl_rcd_nbr]
				,[pay_end_dt]
				,[wgps_adv_pay_dt]
				,[ck_gross]
				,[al_net_pay]
				,[temp_deptid]
				,[home_department]
				,[check_ind]
				,[void_ind]
				,[tax_frequency]
				,[fed_tax_amt]
				,[socsec_amt]
				,[medicare_amt]
				,[state_tax_cd]
				,[state_tax_amt]
				,[state2_tax_cd]
				,[state2_tax_amt]
				,[local_tax_cd]
				,[local_tax_amt]
				,[local2_tax_cd]
				,[local2_tax_amt]
				,[school_dist_amt]
				,[sui_tax_cd]
				,[suisdi_amt]
				,[reversed]
				,[suppress_voucher]
				,[chk_rec_type]
				,[cost_num]
				,[temp_cost_num]
				,[local4_tax_cd]
				,[local4_tax_amt]
				,[local5_tax_cd]
				,[local5_tax_amt]
				,[pay_begin_dt]
				,[sched_year]
				,[quarter_nbr]
				,[qtr_adjstmnt_ind]
				,[al_batch_nbr]
				,[othr_period_beg_dt]
				,[othr_period_end_dt]
				,[total_hrs_worked]
				,[paycheck_tax_freq]
				,[adjustment_dt]
				,[adjustment_seq]
				,[fli_tax_amt]
				,[mli_tax_amt]
				,[sui_amt]
				,[sdi_amt]
				,[local9_tax_cd]
				,[local9_tax_amt]
				,[local10_tax_cd]
				,[local10_tax_amt]
				FOR XML RAW)) AS [RowHash]
			FROM [dbo].[ADP_PS_AL_CHK_DATA_B0_TMP]) [Source]
			JOIN [dbo].[ADP_PS_AL_CHK_DATA_B0_TMP] [Target]
				ON [Source].[paygroup] = [Target].[paygroup]
				AND [Source].[file_nbr] = [Target].[file_nbr]
				AND [Source].[check_dt] = [Target].[check_dt]
				AND [Source].[week_nbr] = [Target].[week_nbr]
				AND [Source].[payroll_nbr] = [Target].[payroll_nbr]
				AND [Source].[check_nbr] = [Target].[check_nbr]
				AND [Source].[entry_nbr] = [Target].[entry_nbr]
				AND [Source].[emplid] = [Target].[emplid]
			WHERE [Target].[IsUpdated] = 1;

			IF NOT EXISTS (SELECT 1 FROM [sys].[tables] WHERE [name] = 'ADP_PS_AL_CHK_DATA_B10_TMP' AND [schema_id] = SCHEMA_ID('dbo'))
				BEGIN
					CREATE TABLE [dbo].[ADP_PS_AL_CHK_DATA_B10_TMP] (
						[paygroup] [varchar](3) NOT NULL,
						[file_nbr] [int] NOT NULL,
						[check_dt] [date] NOT NULL,
						[week_nbr] [tinyint] NOT NULL,
						[payroll_nbr] [tinyint] NOT NULL,
						[check_nbr] [bigint] NOT NULL,
						[entry_nbr] [tinyint] NOT NULL,
						[emplid] [int] NOT NULL,
						[empl_rcd_nbr] [tinyint] NULL,
						[pay_end_dt] [date] NOT NULL,
						[wgps_adv_pay_dt] [date] NULL,
						[ck_gross] [numeric](11, 2) NULL,
						[al_net_pay] [numeric](10, 2) NULL,
						[temp_deptid] [varchar](6) NULL,
						[home_department] [varchar](6) NULL,
						[check_ind] [varchar](1) NULL,
						[void_ind] [varchar](1) NULL,
						[tax_frequency] [varchar](1) NULL,
						[fed_tax_amt] [numeric](10, 2) NULL,
						[socsec_amt] [numeric](10, 2) NULL,
						[medicare_amt] [numeric](10, 2) NULL,
						[state_tax_cd] [varchar](4) NULL,
						[state_tax_amt] [numeric](10, 2) NULL,
						[state2_tax_cd] [varchar](4) NULL,
						[state2_tax_amt] [numeric](10, 2) NULL,
						[local_tax_cd] [varchar](4) NULL,
						[local_tax_amt] [decimal](10, 2) NULL,
						[local2_tax_cd] [varchar](4) NULL,
						[local2_tax_amt] [numeric](10, 2) NULL,
						[school_dist_amt] [numeric](10, 2) NULL,
						[sui_tax_cd] [varchar](4) NULL,
						[suisdi_amt] [numeric](10, 2) NULL,
						[reversed] [varchar](1) NULL,
						[suppress_voucher] [varchar](1) NULL,
						[chk_rec_type] [varchar](1) NULL,
						[cost_num] [varchar](9) NULL,
						[temp_cost_num] [nvarchar](10) NULL,
						[local4_tax_cd] [varchar](4) NULL,
						[local4_tax_amt] [numeric](10, 2) NULL,
						[local5_tax_cd] [varchar](4) NULL,
						[local5_tax_amt] [numeric](10, 2) NULL,
						[pay_begin_dt] [date] NULL,
						[sched_year] [varchar](4) NULL,
						[quarter_nbr] [varchar](1) NULL,
						[qtr_adjstmnt_ind] [varchar](1) NULL,
						[al_batch_nbr] [varchar](4) NULL,
						[othr_period_beg_dt] [date] NULL,
						[othr_period_end_dt] [date] NULL,
						[total_hrs_worked] [numeric](6, 2) NULL,
						[paycheck_tax_freq] [varchar](1) NULL,
						[adjustment_dt] [date] NULL,
						[adjustment_seq] [varchar](3) NULL,
						[fli_tax_amt] [numeric](10, 2) NULL,
						[mli_tax_amt] [numeric](10, 2) NULL,
						[sui_amt] [numeric](10, 2) NULL,
						[sdi_amt] [numeric](10, 2) NULL,
						[local9_tax_cd] [varchar](4) NULL,
						[local9_tax_amt] [decimal](10, 2) NULL,
						[local10_tax_cd] [varchar](4) NULL,
						[local10_tax_amt] [numeric](10, 2) NULL,
						[datalakeinserttime] [datetime2](7) NOT NULL,
						[RowHash] [binary](16) NOT NULL
					);
				END;

			TRUNCATE TABLE [dbo].[ADP_PS_AL_CHK_DATA_B10_TMP];
			INSERT INTO [dbo].[ADP_PS_AL_CHK_DATA_B10_TMP] (
				[paygroup],[file_nbr],[check_dt],[week_nbr],[payroll_nbr],[check_nbr],[entry_nbr],[emplid],[empl_rcd_nbr],[pay_end_dt],[wgps_adv_pay_dt]
				,[ck_gross],[al_net_pay],[temp_deptid],[home_department],[check_ind],[void_ind],[tax_frequency],[fed_tax_amt],[socsec_amt],[medicare_amt]
				,[state_tax_cd],[state_tax_amt],[state2_tax_cd],[state2_tax_amt],[local_tax_cd],[local_tax_amt],[local2_tax_cd],[local2_tax_amt],[school_dist_amt]
				,[sui_tax_cd],[suisdi_amt],[reversed],[suppress_voucher],[chk_rec_type],[cost_num],[temp_cost_num],[local4_tax_cd],[local4_tax_amt],[local5_tax_cd]
				,[local5_tax_amt],[pay_begin_dt],[sched_year],[quarter_nbr],[qtr_adjstmnt_ind],[al_batch_nbr],[othr_period_beg_dt],[othr_period_end_dt]
				,[total_hrs_worked],[paycheck_tax_freq],[adjustment_dt],[adjustment_seq],[fli_tax_amt],[mli_tax_amt],[sui_amt],[sdi_amt],[local9_tax_cd]
				,[local9_tax_amt],[local10_tax_cd],[local10_tax_amt],[datalakeinserttime],[RowHash]
			)
			SELECT
				[paygroup],[file_nbr],[check_dt],[week_nbr],[payroll_nbr],[check_nbr],[entry_nbr],[emplid],[empl_rcd_nbr],[pay_end_dt],[wgps_adv_pay_dt]
				,[ck_gross],[al_net_pay],[temp_deptid],[home_department],[check_ind],[void_ind],[tax_frequency],[fed_tax_amt],[socsec_amt],[medicare_amt]
				,[state_tax_cd],[state_tax_amt],[state2_tax_cd],[state2_tax_amt],[local_tax_cd],[local_tax_amt],[local2_tax_cd],[local2_tax_amt],[school_dist_amt]
				,[sui_tax_cd],[suisdi_amt],[reversed],[suppress_voucher],[chk_rec_type],[cost_num],[temp_cost_num],[local4_tax_cd],[local4_tax_amt],[local5_tax_cd]
				,[local5_tax_amt],[pay_begin_dt],[sched_year],[quarter_nbr],[qtr_adjstmnt_ind],[al_batch_nbr],[othr_period_beg_dt],[othr_period_end_dt]
				,[total_hrs_worked],[paycheck_tax_freq],[adjustment_dt],[adjustment_seq],[fli_tax_amt],[mli_tax_amt],[sui_amt],[sdi_amt],[local9_tax_cd]
				,[local9_tax_amt],[local10_tax_cd],[local10_tax_amt],[datalakeinserttime],[RowHash]
			FROM [dbo].[ADP_PS_AL_CHK_DATA_B10]
			WHERE YEAR([pay_end_dt]) = @ProcessingYear;

			SELECT
				[paygroup],[file_nbr],[check_dt],[week_nbr],[payroll_nbr],[check_nbr],[entry_nbr],[emplid],[empl_rcd_nbr],[pay_end_dt],[wgps_adv_pay_dt]
				,[ck_gross],[al_net_pay],[temp_deptid],[home_department],[check_ind],[void_ind],[tax_frequency],[fed_tax_amt],[socsec_amt],[medicare_amt]
				,[state_tax_cd],[state_tax_amt],[state2_tax_cd],[state2_tax_amt],[local_tax_cd],[local_tax_amt],[local2_tax_cd],[local2_tax_amt],[school_dist_amt]
				,[sui_tax_cd],[suisdi_amt],[reversed],[suppress_voucher],[chk_rec_type],[cost_num],[temp_cost_num],[local4_tax_cd],[local4_tax_amt],[local5_tax_cd]
				,[local5_tax_amt],[pay_begin_dt],[sched_year],[quarter_nbr],[qtr_adjstmnt_ind],[al_batch_nbr],[othr_period_beg_dt],[othr_period_end_dt]
				,[total_hrs_worked],[paycheck_tax_freq],[adjustment_dt],[adjustment_seq],[fli_tax_amt],[mli_tax_amt],[sui_amt],[sdi_amt],[local9_tax_cd]
				,[local9_tax_amt],[local10_tax_cd],[local10_tax_amt],[datalakeinserttime],[RowHash]
			FROM [dbo].[ADP_PS_AL_CHK_DATA_B0_TMP] AS [B0]
			WHERE NOT EXISTS (
				SELECT 1
				FROM [dbo].[ADP_PS_AL_CHK_DATA_B10_TMP] AS [B10]
				WHERE [B0].[paygroup] = [B10].[paygroup]
				AND [B0].[file_nbr] = [B10].[file_nbr]
				AND [B0].[check_dt] = [B10].[check_dt]
				AND [B0].[week_nbr] = [B10].[week_nbr]
				AND [B0].[payroll_nbr] = [B10].[payroll_nbr]
				AND [B0].[check_nbr] = [B10].[check_nbr]
				AND [B0].[entry_nbr] = [B10].[entry_nbr]
				AND [B0].[emplid] = [B10].[emplid]
			);
		
		END TRY
		BEGIN CATCH
			IF (@@TRANCOUNT > 0)
				ROLLBACK TRAN;

		END CATCH;
   	 
	END;