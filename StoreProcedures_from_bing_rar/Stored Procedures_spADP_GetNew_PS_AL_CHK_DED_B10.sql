CREATE PROCEDURE [dbo].[spADP_GetNew_PS_AL_CHK_DED_B10]
	@LastSuccessfulLoadTime DATETIME,
	@ProcessingYear			INT,
	@DebugMode				INT       = NULL
AS
	-- ================================================================================
	-- 
	-- Stored Procedure:   spADP_GetNew_PS_AL_CHK_DED_B10
	--
	-- Purpose:            Fetches records from [dbo].[ADP_PS_AL_CHK_DED_B0] for insert into
	--						[dbo].[ADP_PS_AL_CHK_DED_B10]
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
	-- 07/26/2023		Aniket				BI-9131: ADP_PS_AL_CHK_DED_B10 Loading Logic Change		 
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
			IF NOT EXISTS (SELECT 1 FROM [sys].[tables] WHERE [name] = 'ADP_PS_AL_CHK_DED_B0_TMP' AND [schema_id] = SCHEMA_ID('dbo'))
				BEGIN
					CREATE TABLE [dbo].[ADP_PS_AL_CHK_DED_B0_TMP] (
						[paygroup] [varchar](3) NOT NULL,
						[file_nbr] [varchar](16) NOT NULL,
						[check_dt] [date] NOT NULL,
						[week_nbr] [tinyint] NOT NULL,
						[payroll_nbr] [tinyint] NOT NULL,
						[check_nbr] [bigint] NOT NULL,
						[entry_nbr] [tinyint] NOT NULL,
						[row_nbr] [tinyint] NOT NULL,
						[emplid] [varchar](16) NOT NULL,
						[empl_rcd_nbr] [tinyint] NULL,
						[plan_type] [varchar](2) NULL,
						[dedcd] [varchar](3) NULL,
						[ded_class] [varchar](1) NULL,
						[al_amount] [numeric](10, 2) NULL,
						[al_dedcd] [varchar](3) NULL,
						[al_descr] [nvarchar](30) NULL,
						[datalakeinserttime] [datetime2](7) NOT NULL,
						[RowHash] [binary](16) NOT NULL
					);
				END;

			TRUNCATE TABLE [dbo].[ADP_PS_AL_CHK_DED_B0_TMP];
			INSERT INTO [dbo].[ADP_PS_AL_CHK_DED_B0_TMP] (
				[paygroup]
			   ,[file_nbr]
			   ,[check_dt]
			   ,[week_nbr]
			   ,[payroll_nbr]
			   ,[check_nbr]
			   ,[entry_nbr]
			   ,[row_nbr]
			   ,[emplid]
			   ,[empl_rcd_nbr]
			   ,[plan_type]
			   ,[dedcd]
			   ,[ded_class]
			   ,[al_amount]
			   ,[al_dedcd]
			   ,[al_descr]
			   ,[datalakeinserttime]
			   ,[RowHash]	 
			)
			SELECT
				[paygroup]
			   ,CASE
					WHEN ISNUMERIC( [emplid] ) = 1 AND ( [file_nbr] = '' OR [file_nbr] IS NULL OR ISNUMERIC( [file_nbr] ) = 0)  
						THEN [emplid]
					ELSE [file_nbr]
				END AS [file_nbr]
			   ,[check_dt]
			   ,[week_nbr]
			   ,[payroll_nbr]
			   ,[check_nbr]
			   ,[entry_nbr]
			   ,[row_nbr]
			   ,CASE 
					WHEN ISNUMERIC( [emplid] ) = 0 AND ISNUMERIC( [file_nbr] ) = 1 AND  RIGHT( [emplid], 4 ) = RIGHT( [file_nbr], 4 ) 
						THEN [file_nbr]
					ELSE [emplid] 
				END AS [emplid]
			   ,[empl_rcd_nbr]
			   ,[plan_type]
			   ,[dedcd]
			   ,[ded_class]
			   ,[al_amount]
			   ,[al_dedcd]
			   ,[al_descr]
			   ,[datalakeinserttime]
			   ,[RowHash]	 
			FROM [dbo].[ADP_PS_AL_CHK_DED_B0]
			WHERE YEAR([check_dt]) = @ProcessingYear
			AND [BaseCreatedDate] > @LastSuccessfulLoadTime;

			--
			-- Create Temporary Table to get data from Target
			--
			IF NOT EXISTS (SELECT 1 FROM [sys].[tables] WHERE [name] = 'ADP_PS_AL_CHK_DED_B10_TMP' AND [schema_id] = SCHEMA_ID('dbo'))
				BEGIN
					CREATE TABLE [dbo].[ADP_PS_AL_CHK_DED_B10_TMP] (
						[paygroup] [varchar](3) NOT NULL,
						[file_nbr] [varchar](16) NOT NULL,
						[check_dt] [date] NOT NULL,
						[week_nbr] [tinyint] NOT NULL,
						[payroll_nbr] [tinyint] NOT NULL,
						[check_nbr] [bigint] NOT NULL,
						[entry_nbr] [tinyint] NOT NULL,
						[row_nbr] [tinyint] NOT NULL,
						[emplid] [varchar](16) NOT NULL,
						[empl_rcd_nbr] [tinyint] NULL,
						[plan_type] [varchar](2) NULL,
						[dedcd] [varchar](3) NULL,
						[ded_class] [varchar](1) NULL,
						[al_amount] [numeric](10, 2) NULL,
						[al_dedcd] [varchar](3) NULL,
						[al_descr] [nvarchar](30) NULL,
						[datalakeinserttime] [datetime2](7) NOT NULL,
						[RowHash] [binary](16) NOT NULL	 
					);
				END;

			TRUNCATE TABLE [dbo].[ADP_PS_AL_CHK_DED_B10_TMP];
			INSERT INTO [dbo].[ADP_PS_AL_CHK_DED_B10_TMP] (
				[paygroup]
			   ,[file_nbr]
			   ,[check_dt]
			   ,[week_nbr]
			   ,[payroll_nbr]
			   ,[check_nbr]
			   ,[entry_nbr]
			   ,[row_nbr]
			   ,[emplid]
			   ,[empl_rcd_nbr]
			   ,[plan_type]
			   ,[dedcd]
			   ,[ded_class]
			   ,[al_amount]
			   ,[al_dedcd]
			   ,[al_descr]
			   ,[datalakeinserttime]
			   ,[RowHash]	 
			)
			SELECT
				[paygroup]
			   ,[file_nbr]
			   ,[check_dt]
			   ,[week_nbr]
			   ,[payroll_nbr]
			   ,[check_nbr]
			   ,[entry_nbr]
			   ,[row_nbr]
			   ,[emplid]
			   ,[empl_rcd_nbr]
			   ,[plan_type]
			   ,[dedcd]
			   ,[ded_class]
			   ,[al_amount]
			   ,[al_dedcd]
			   ,[al_descr]
			   ,[datalakeinserttime]
			   ,[RowHash]	 
			FROM [dbo].[ADP_PS_AL_CHK_DED_B10]
			WHERE YEAR([check_dt]) = @ProcessingYear;

			SELECT
				[paygroup]
			   ,[file_nbr]
			   ,[check_dt]
			   ,[week_nbr]
			   ,[payroll_nbr]
			   ,[check_nbr]
			   ,[entry_nbr]
			   ,[row_nbr]
			   ,[emplid]
			   ,[empl_rcd_nbr]
			   ,[plan_type]
			   ,[dedcd]
			   ,[ded_class]
			   ,[al_amount]
			   ,[al_dedcd]
			   ,[al_descr]
			   ,[datalakeinserttime]
			   ,[RowHash]	 
			FROM [dbo].[ADP_PS_AL_CHK_DED_B0_TMP] AS [B0]
			WHERE NOT EXISTS (
				SELECT 1
				FROM [dbo].[ADP_PS_AL_CHK_DED_B10_TMP] AS [B10]
				WHERE [B0].[paygroup] = [B10].[paygroup]
				AND [B0].[emplid]  =   [B10].[emplid] 
				AND [B0].[check_dt] = [B10].[check_dt]
				AND [B0].[week_nbr] = [B10].[week_nbr]
				AND [B0].[payroll_nbr] = [B10].[payroll_nbr]
				AND [B0].[check_nbr] = [B10].[check_nbr]
				AND [B0].[entry_nbr] = [B10].[entry_nbr]
				AND [B0].[row_nbr] = [B10].[row_nbr]
			);
		
		END TRY
		BEGIN CATCH
			IF (@@TRANCOUNT > 0)
				ROLLBACK TRAN;

		END CATCH;
   	 
	END;