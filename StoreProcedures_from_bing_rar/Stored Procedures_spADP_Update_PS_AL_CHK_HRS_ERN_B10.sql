CREATE PROCEDURE [dbo].[spADP_Update_PS_AL_CHK_HRS_ERN_B10]
	@LastSuccessfulLoadTime DATETIME,
	@ProcessingYear			INT,
	@DebugMode				INT       = NULL
AS
	-- ================================================================================
	-- 
	-- Stored Procedure:   spADP_Update_PS_AL_CHK_HRS_ERN_B10
	--
	-- Purpose:            Updates [dbo].[ADP_PS_AL_CHK_HRS_ERN_B10] based on changes from
	--					   [dbo].[ADP_PS_AL_CHK_HRS_ERN_B0]
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
	-- 07/26/2023		Praveen				BI-9604: ADP_PS_AL_CHK_HRS_ERN_B10 Loading Logic Change		 
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
			IF NOT EXISTS (SELECT 1 FROM [sys].[tables] WHERE [name] = 'ADP_PS_AL_CHK_HRS_ERN_B0_TMP' AND [schema_id] = SCHEMA_ID('dbo'))
				BEGIN
					CREATE TABLE [dbo].[ADP_PS_AL_CHK_HRS_ERN_B0_TMP] (
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
						[erncd] [varchar](10) NULL,
						[al_hours] [numeric](14, 2) NULL,
						[earnings] [numeric](15, 2) NULL,
						[datalakeinserttime] [datetime2](7) NOT NULL,
						[RowHash] [binary](16) NOT NULL
					);
				END;

			IF NOT EXISTS (SELECT 1 FROM [dbo].[ADP_PS_AL_CHK_HRS_ERN_B0_TMP])
				BEGIN
					INSERT INTO [dbo].[ADP_PS_AL_CHK_HRS_ERN_B0_TMP] (
						[paygroup],
						[file_nbr],
						[check_dt],
						[week_nbr],
						[payroll_nbr],
						[check_nbr],
						[entry_nbr],
						[row_nbr],
						[emplid],
						[empl_rcd_nbr],
						[erncd],
						[al_hours],
						[earnings],
						[datalakeinserttime],
						[RowHash]
					)
					SELECT
						[paygroup],			
						CASE
							WHEN ISNUMERIC( [emplid] ) = 1 AND ( [file_nbr] = '' OR [file_nbr] IS NULL OR ISNUMERIC( [file_nbr] ) = 0)  
								THEN [emplid]
							ELSE [file_nbr]
						END AS [file_nbr],
						[check_dt],
						[week_nbr],
						[payroll_nbr],
						[check_nbr],
						[entry_nbr],
						[row_nbr],
						CASE 
							WHEN ISNUMERIC( [emplid] ) = 0 AND ISNUMERIC( [file_nbr] ) = 1 AND RIGHT( [emplid], 6 ) = [file_nbr]
								THEN [file_nbr]
							ELSE [emplid] 
						END AS [emplid],
						[empl_rcd_nbr],
						[erncd],
						[al_hours],
						[earnings],
						[datalakeinserttime],
						[RowHash]
					FROM [dbo].[ADP_PS_AL_CHK_HRS_ERN_B0]
					WHERE YEAR([check_dt]) = @ProcessingYear
					AND [BaseCreatedDate] > @LastSuccessfulLoadTime;
				END;
			 
			UPDATE [E]
				SET [E].[empl_rcd_nbr] = [L].[empl_rcd_nbr],
					[E].[erncd] = [L].[erncd],
					[E].[al_hours] = [L].[al_hours],
					[E].[earnings] = [L].[earnings],
					[E].[datalakeinserttime] = [L].[datalakeinserttime],
					[E].[RowHash] = [L].[RowHash],
					[E].[BaseUpdatedDate] = GETDATE(),
					[E].[BaseUpdatedBy] = SUSER_SNAME()
			FROM [HR_Base].[dbo].[ADP_PS_AL_CHK_HRS_ERN_B10] [E]
			INNER JOIN [dbo].[ADP_PS_AL_CHK_HRS_ERN_B0_TMP] [L]
				ON  [E].[paygroup] = [L].[paygroup]
				AND CONVERT(VARCHAR(16), [E].[file_nbr]) = [L].[file_nbr]
				AND [E].[check_dt] = [L].[check_dt]
				AND [E].[week_nbr] = [L].[week_nbr]
				AND [E].[payroll_nbr] = [L].[payroll_nbr]
				AND [E].[check_nbr] = [L].[check_nbr]
				AND [E].[entry_nbr] = [L].[entry_nbr]
				AND [E].[row_nbr] = [L].[row_nbr]
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