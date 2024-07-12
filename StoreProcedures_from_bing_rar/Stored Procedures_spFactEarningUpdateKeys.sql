CREATE PROCEDURE [dbo].[spFactEarningUpdateKeys]
	@SourceName		VARCHAR(100),
	@ExecutionID	VARCHAR(300)
AS
	BEGIN
		/*
		================================================================================
		Stored Procedure:   spFactEarningUpdateKeys
		Purpose:            Update Keys:
							a. [PersonKey] since [PersonKey] is expected to change in [dbo].[DimPerson]
							b. [AssignmentKey] since [AssignmentKey] is expected to change in [dbo].[DimAssignment]
							b. [PositionKey] since [PositionKey] is expected to change in [dbo].[DimPosition]
		Parameters:			@SourceName
							@ExecutionID
		Usage:              EXEC [dbo].[spFactEarningUpdateKeys] 'FactEarning', ''
		
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

					/*
					**	Section 1: Update PersonKey
					*/
					SELECT @TaskName = @SourceName + ' - PersonKey';
					EXEC [dbo].[spBeginAuditLog]
						@AuditId = @AuditID OUTPUT,
						@SourceName = @TaskName,
						@ExecutionID = @ExecutionID,
						@BatchSplitByName = 'CalendarYear',
						@BatchSplitByValue = @YearDate;
					
					DROP TABLE IF EXISTS #EarningPersonKeys;

					SELECT DISTINCT
						[F].[PersonID] [EarningPersonID], [F].[PersonKey] [EarningPersonKey], [F].[PayPeriodEndDateKey],
						[P].[PersonID] [PreviousPersonID],
						[C].[PersonKey] [CurrentPersonKey], [C].[EffectiveFrom], [C].[EffectiveTo]
					INTO #EarningPersonKeys
					FROM [dbo].[FactEarning] [F]
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

					SELECT @SourceCount = COUNT(*) FROM #EarningPersonKeys;

					BEGIN TRAN;

					UPDATE [F]
						SET [PersonKey] = [A].[CurrentPersonKey]
					FROM [dbo].[FactEarning] [F]
					INNER JOIN (
						SELECT
							[EarningPersonID], [EarningPersonKey], [PayPeriodEndDateKey], [CurrentPersonKey],
							ROW_NUMBER() OVER (PARTITION BY [EarningPersonID], [EarningPersonKey], [PayPeriodEndDateKey] ORDER BY [EffectiveTo] DESC, [EffectiveFrom] DESC) AS [RW]
						FROM #EarningPersonKeys
					) AS [A]
						ON [F].[PersonID] = [A].[EarningPersonID]
						AND [F].[PersonKey] = [A].[EarningPersonKey]
						AND [F].[PayPeriodEndDateKey] = [A].[PayPeriodEndDateKey]
						AND [A].[RW] = 1
					WHERE [F].[PayPeriodEndDateKey] >= CONVERT(INT, CONVERT(VARCHAR, @MinYear) + '0101') AND [F].[PayPeriodEndDateKey] < CONVERT(INT, CONVERT(VARCHAR, @MinYear + 1) + '0101');

					SELECT @UpdateCount = @@ROWCOUNT;

					COMMIT TRAN;

					DROP TABLE IF EXISTS #EarningPersonKeys;

					EXEC [dbo].[spEndAuditLog] 
						@InsertCount = 0,
						@UpdateCount = @UpdateCount,
						@DeleteCount = 0,
						@SourceCount = @SourceCount,
						@AuditId = @AuditID;

					/*
					**	Section 2: Update AssignmentKey
					*/
					SELECT @TaskName = @SourceName + ' - AssignmentKey';
					EXEC [dbo].[spBeginAuditLog]
						@AuditId = @AuditID OUTPUT,
						@SourceName = @TaskName,
						@ExecutionID = @ExecutionID,
						@BatchSplitByName = 'CalendarYear',
						@BatchSplitByValue = @YearDate;
					
					DROP TABLE IF EXISTS #EarningAssignmentKeys;

					SELECT DISTINCT
						[F].[AssignmentID] [EarningAssignmentID], [F].[AssignmentKey] [EarningAssignmentKey], [F].[PayPeriodEndDateKey],
						[P].[AssignmentID] [PreviousAssignmentID],
						[C].[AssignmentKey] [CurrentAssignmentKey], [C].[EffectiveFrom], [C].[EffectiveTo]
					INTO #EarningAssignmentKeys
					FROM [dbo].[FactEarning] [F]
					INNER JOIN [dbo].[DimDate] [D]
						ON [F].[PayPeriodEndDateKey] = [D].[DateKey]
					LEFT JOIN [dbo].[DimAssignment] [P]
						ON [F].[AssignmentID] = [P].[AssignmentID]
						AND [F].[AssignmentKey] = [P].[AssignmentKey]
						AND [D].[FullDate] >= [P].[EffectiveFrom]
						AND [D].[FullDate] <= [P].[EffectiveTo]
					INNER JOIN [dbo].[DimAssignment] [C]
						ON [F].[AssignmentID] = [C].[AssignmentID]
						AND [D].[FullDate] >= [C].[EffectiveFrom]
						AND [D].[FullDate] <= [C].[EffectiveTo]
					WHERE [F].[PayPeriodEndDateKey] >= CONVERT(INT, CONVERT(VARCHAR, @MinYear) + '0101') AND [F].[PayPeriodEndDateKey] < CONVERT(INT, CONVERT(VARCHAR, @MinYear + 1) + '0101')
					AND [P].[AssignmentID] IS NULL;

					SELECT @SourceCount = COUNT(*) FROM #EarningAssignmentKeys;

					BEGIN TRAN;

					UPDATE [F]
						SET [AssignmentKey] = [A].[CurrentAssignmentKey]
					FROM [dbo].[FactEarning] [F]
					INNER JOIN (
						SELECT
							[EarningAssignmentID], [EarningAssignmentKey], [PayPeriodEndDateKey], [CurrentAssignmentKey],
							ROW_NUMBER() OVER (PARTITION BY [EarningAssignmentID], [EarningAssignmentKey], [PayPeriodEndDateKey] ORDER BY [EffectiveTo] DESC, [EffectiveFrom] DESC) AS [RW]
						FROM #EarningAssignmentKeys
					) AS [A]
						ON [F].[AssignmentID] = [A].[EarningAssignmentID]
						AND [F].[AssignmentKey] = [A].[EarningAssignmentKey]
						AND [F].[PayPeriodEndDateKey] = [A].[PayPeriodEndDateKey]
						AND [A].[RW] = 1
					WHERE [F].[PayPeriodEndDateKey] >= CONVERT(INT, CONVERT(VARCHAR, @MinYear) + '0101') AND [F].[PayPeriodEndDateKey] < CONVERT(INT, CONVERT(VARCHAR, @MinYear + 1) + '0101');

					SELECT @UpdateCount = @@ROWCOUNT;

					COMMIT TRAN;

					DROP TABLE IF EXISTS #EarningAssignmentKeys;

					EXEC [dbo].[spEndAuditLog] 
						@InsertCount = 0,
						@UpdateCount = @UpdateCount,
						@DeleteCount = 0,
						@SourceCount = @SourceCount,
						@AuditId = @AuditID;

					/*
					**	Section 3: Update PositionKey
					*/
					SELECT @TaskName = @SourceName + ' - PositionKey';
					EXEC [dbo].[spBeginAuditLog]
						@AuditId = @AuditID OUTPUT,
						@SourceName = @TaskName,
						@ExecutionID = @ExecutionID,
						@BatchSplitByName = 'CalendarYear',
						@BatchSplitByValue = @YearDate;
					
					DROP TABLE IF EXISTS #EarningPositionKeys;

					SELECT DISTINCT
						[F].[PositionID] [EarningPositionID], [F].[PositionKey] [EarningPositionKey], [F].[PayPeriodEndDateKey],
						[P].[PositionID] [PreviousAssignmentID],
						[C].[PositionKey] [CurrentAssignmentKey], [C].[EffectiveFrom], [C].[EffectiveTo]
					INTO #EarningPositionKeys
					FROM [dbo].[FactEarning] [F]
					INNER JOIN [dbo].[DimDate] [D]
						ON [F].[PayPeriodEndDateKey] = [D].[DateKey]
					LEFT JOIN [dbo].[DimPosition] [P]
						ON [F].[PositionID] = [P].[PositionID]
						AND [F].[PositionKey] = [P].[PositionKey]
						AND [D].[FullDate] >= [P].[EffectiveFrom]
						AND [D].[FullDate] <= [P].[EffectiveTo]
					INNER JOIN [dbo].[DimPosition] [C]
						ON [F].[PositionID] = [C].[PositionID]
						AND [D].[FullDate] >= [C].[EffectiveFrom]
						AND [D].[FullDate] <= [C].[EffectiveTo]
					WHERE [F].[PayPeriodEndDateKey] >= CONVERT(INT, CONVERT(VARCHAR, @MinYear) + '0101') AND [F].[PayPeriodEndDateKey] < CONVERT(INT, CONVERT(VARCHAR, @MinYear + 1) + '0101')
					AND [P].[PositionID] IS NULL;

					SELECT @SourceCount = COUNT(*) FROM #EarningPositionKeys;

					BEGIN TRAN;

					UPDATE [F]
						SET [PositionKey] = [A].[CurrentAssignmentKey]
					FROM [dbo].[FactEarning] [F]
					INNER JOIN (
						SELECT
							[EarningPositionID], [EarningPositionKey], [PayPeriodEndDateKey], [CurrentAssignmentKey],
							ROW_NUMBER() OVER (PARTITION BY [EarningPositionID], [EarningPositionKey], [PayPeriodEndDateKey] ORDER BY [EffectiveTo] DESC, [EffectiveFrom] DESC) AS [RW]
						FROM #EarningPositionKeys
					) AS [A]
						ON [F].[PositionID] = [A].[EarningPositionID]
						AND [F].[PositionKey] = [A].[EarningPositionKey]
						AND [F].[PayPeriodEndDateKey] = [A].[PayPeriodEndDateKey]
						AND [A].[RW] = 1
					WHERE [F].[PayPeriodEndDateKey] >= CONVERT(INT, CONVERT(VARCHAR, @MinYear) + '0101') AND [F].[PayPeriodEndDateKey] < CONVERT(INT, CONVERT(VARCHAR, @MinYear + 1) + '0101');

					SELECT @UpdateCount = @@ROWCOUNT;

					COMMIT TRAN;

					DROP TABLE IF EXISTS #EarningPositionKeys;

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