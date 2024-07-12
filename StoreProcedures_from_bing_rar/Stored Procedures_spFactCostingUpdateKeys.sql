CREATE PROCEDURE [dbo].[spFactCostingUpdateKeys]
	@SourceName		VARCHAR(100),
	@ExecutionID	VARCHAR(300)
AS
	BEGIN
		/*
		================================================================================
		Stored Procedure:   spFactCostingUpdateKeys
		Purpose:            Update [PersonKey] since [PersonKey] is expected to change in [dbo].[DimPerson]
		Parameters:			@SourceName
							@ExecutionID
		Usage:              EXEC [dbo].[spFactCostingUpdateKeys] 'FactCosting', ''
		
		--------------------------------------------------------------------------------
		Change Log:		   
		----------
		Date				Modified By			Comments
		----------			-----------			--------
		01/12/2024			Suhas				DFTP-1047 - Initial version
		================================================================================
		*/

		DECLARE @AuditID BIGINT = 0,
				@TaskName VARCHAR(100),
				@CurrentYear INT,
				@MinYear INT,
				@YearDate DATE,
				@SourceCount INT,
				@UpdateCount INT;

		SELECT @CurrentYear = YEAR(GETDATE());
		SELECT @MinYear = @CurrentYear - 7;
		
		BEGIN TRY
			
			WHILE (@MinYear <= @CurrentYear)
				BEGIN
					SELECT @YearDate = CONVERT(DATE, CONVERT(VARCHAR, @MinYear) + '-01-01');
					
					SELECT @TaskName = @SourceName + ' - PersonKey';
					EXEC [dbo].[spBeginAuditLog]
						@AuditId = @AuditID OUTPUT,
						@SourceName = @TaskName,
						@ExecutionID = @ExecutionID,
						@BatchSplitByName = 'CalendarYear',
						@BatchSplitByValue = @YearDate;
					
					DROP TABLE IF EXISTS #CostingKeys;

					SELECT DISTINCT
						[F].[PersonID] [CostingPersonID], [F].[PersonKey] [CostingPersonKey], [F].[PayPeriodEndDateKey],
						[P].[PersonID] [PreviousPersonID],
						[C].[PersonKey] [CurrentPersonKey], [C].[EffectiveFrom], [C].[EffectiveTo]
					INTO #CostingKeys
					FROM [dbo].[FactCosting] [F]
					INNER JOIN [dbo].[DimDate] [D]
						ON [F].[PayPeriodEndDateKey] = [D].[DateKey]
					LEFT JOIN [dbo].[DimPerson] [P]
						ON [F].[PersonID] = [P].[PersonID]
						AND [F].[PersonKey] = [P].[PersonKey]
						AND [D].[FullDate] >= [P].[EffectiveFrom]
						AND [D].[FullDate] <= [P].[EffectiveTo]
					INNER JOIN [dbo].[DimPerson] [C]
						ON [F].[PersonID] = [C].[PersonID]
						AND [D].[FullDate] >= [C].[EffectiveFrom]
						AND [D].[FullDate] <= [C].[EffectiveTo]
					WHERE [F].[PayPeriodEndDateKey] >= CONVERT(INT, CONVERT(VARCHAR, @MinYear) + '0101') AND [F].[PayPeriodEndDateKey] < CONVERT(INT, CONVERT(VARCHAR, @MinYear + 1) + '0101')
					AND [P].[PersonID] IS NULL;

					SELECT @SourceCount = COUNT(*) FROM #CostingKeys;

					BEGIN TRAN;

					UPDATE [F]
						SET [PersonKey] = [A].[CurrentPersonKey]
					FROM [dbo].[FactCosting] [F]
					INNER JOIN (
						SELECT
							[CostingPersonID], [CostingPersonKey], [PayPeriodEndDateKey], [CurrentPersonKey],
							ROW_NUMBER() OVER (PARTITION BY [CostingPersonID], [CostingPersonKey], [PayPeriodEndDateKey] ORDER BY [EffectiveTo] DESC, [EffectiveFrom] DESC) AS [RW]
						FROM #CostingKeys
					) AS [A]
						ON [F].[PersonID] = [A].[CostingPersonID]
						AND [F].[PersonKey] = [A].[CostingPersonKey]
						AND [F].[PayPeriodEndDateKey] = [A].[PayPeriodEndDateKey]
						AND [A].[RW] = 1
					WHERE [F].[PayPeriodEndDateKey] >= CONVERT(INT, CONVERT(VARCHAR, @MinYear) + '0101') AND [F].[PayPeriodEndDateKey] < CONVERT(INT, CONVERT(VARCHAR, @MinYear + 1) + '0101');

					SELECT @UpdateCount = @@ROWCOUNT;

					COMMIT TRAN;

					DROP TABLE IF EXISTS #CostingKeys;

					EXEC [dbo].[spEndAuditLog] 
						@InsertCount = 0,
						@UpdateCount = @UpdateCount,
						@DeleteCount = 0,
						@SourceCount = @SourceCount,
						@AuditId = @AuditID;

					SELECT @MinYear = @MinYear + 1;
				END;

		END TRY

		BEGIN CATCH
			IF (@@TRANCOUNT > 0)
				ROLLBACK TRAN;

			IF (@AuditID > 0)
				EXEC [dbo].[spErrorAuditLog]    
					@AuditId = @AuditID;
		END CATCH;
	END;
GO