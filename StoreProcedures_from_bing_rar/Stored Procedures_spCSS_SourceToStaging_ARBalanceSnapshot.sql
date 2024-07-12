CREATE PROCEDURE [dbo].[spCSS_SourceToStaging_ARBalanceSnapshot] (@AsOfFiscalWeek int)
AS
BEGIN
DECLARE @tableVar TABLE (MergeAction VARCHAR(20));
--Declare @AsOfFiscalWeek int;
--SET @AsOfFiscalWeek=201752;
Declare @AuditId BIGINT,
		@SourceCount INT, 
        @InsertCount INT,
		@UpdateCount INT,
		--@DeleteCount INT=null,
		@TaskName VARCHAR(100)= 'ARBalanceSnapshot';;
IF OBJECT_ID('tempdb..#ARBalanceSnapshotUpsert') IS NOT NULL DROP TABLE #ARBalanceSnapshotUpsert;
 CREATE Table #ARBalanceSnapshotUpsert([AsOfFiscalWeek] [int] NOT NULL,
										[ctr_no] [varchar](4) NOT NULL,
										[fam_no] [varchar](4) NULL,
										[cust_code] [varchar](4) NULL,
										[ARBalanceType] [int] NOT NULL,
										[ARAgingDate] [date] NOT NULL,
										[ARAgingDays] [int] NOT NULL,
										[ARBalanceAmount] [numeric](19, 2) NOT NULL,);

					Insert into #ARBalanceSnapshotUpsert
							   ([AsOfFiscalWeek]
							   ,[ctr_no]
							   ,[fam_no]
							   ,[cust_code]
							   ,[ARBalanceType]
							   ,[ARAgingDate]
							   ,[ARAgingDays]
							   ,[ARBalanceAmount])
					exec pARBalance @AsOfFiscalWeek,@SourceCount;


				CREATE CLUSTERED INDEX CIX_TempARBalanceSnapshot ON #ARBalanceSnapshotUpsert
																	([AsOfFiscalWeek] ASC
																   ,[ctr_no] ASC
																   ,[fam_no] ASC
																   ,[cust_code] ASC
																   ,[ARBalanceType] ASC
																   ,[ARAgingDate] ASC);
				Set @SourceCount=(Select count(1) from #ARBalanceSnapshotUpsert);
-------------------------------------------------------
--Pre Execute Event Handler
-------------------------------------------------------				
	EXEC CSS_Staging.[dbo].[spStagingBeginAuditLog] @SourceName = @TaskName, @AuditId = @AuditId OUTPUT;
--============================================================================================================
							
BEGIN TRY
BEGIN TRANSACTION
	MERGE ARBalanceSnapshot AS T
	USING #ARBalanceSnapshotUpsert AS S
	ON (T.[AsOfFiscalWeek] = S.[AsOfFiscalWeek] AND T.[ctr_no]= S.[ctr_no] and (T.[fam_no] =S.[fam_no] OR (T.[fam_no] is null and S.[fam_no] is null)) 
	and (T.[cust_code]=S.[cust_code] OR (T.[cust_code] is null and S.[cust_code] is null)) and T.[ARBalanceType]=S.[ARBalanceType])
	WHEN MATCHED AND 
		(
			   T.ARAgingDate<>S.ARAgingDate
			OR T.ARAgingDays<>S.ARAgingDays
			OR T.ARBalanceAmount<>S.ARBalanceAmount
		) THen
	Update 
		set 
			T.ARAgingDate=S.ARAgingDate,
			T.ARAgingDays=S.ARAgingDays,
			T.ARBalanceAmount=S.ARBalanceAmount,
			T.StgModifiedDate=getdate(),
			T.StgModifiedBy=suser_sname()
	WHEN NOT MATCHED By Target THEN
	INSERT 
	--Target
				(
				 [AsOfFiscalWeek]
				,[ctr_no]
				,[fam_no]
				,[cust_code]
				,[ARBalanceType]
				,[ARAgingDate]
				,[ARAgingDays]
				,[ARBalanceAmount]
				)
	Values
			( 
				 S.[AsOfFiscalWeek]
				,S.[ctr_no]
				,S.[fam_no]
				,S.[cust_code]
				,S.[ARBalanceType]
				,S.[ARAgingDate]
				,S.[ARAgingDays]
				,S.[ARBalanceAmount]
			)

	--SOFT Delete Optional
	--WHEN NOT MATCHED BY SOURCE AND T.Deleted is null
	--		THEN 
	--		UPDATE
	--		Set	Deleted = GetDate()
	------------------------------------------------------------------------------------
	--Audit Log Counts
	------------------------------------------------------------------------------------
	OUTPUT $action INTO @tableVar; 
		 SELECT 
			--@sourcecount ,
			@InsertCount=SUM(Inserted), --as vInsertCount ,
			@UpdateCount=SUM(Updated) --as vUpdateCount 
			--,SUM(Deleted) as Deleted
	FROM  (
			-- Count the number of inserts
			SELECT COUNT(*) as Inserted, 0 as Updated
			FROM @tableVar  
			WHERE MergeAction = 'INSERT'
 
			UNION ALL
			-- Count the number of updates   
			SELECT 0 as Inserted, COUNT(*) as Updated
			FROM @tableVar  
			WHERE MergeAction = 'UPDATE' 
			) as CountTable; 

--===============================================
--Post Execute Event Handler to write the counts 
--===============================================
EXEC CSS_Staging.[dbo].[spStagingEndAuditLog]
                 @InsertCount = @InsertCount,
                 @UpdateCount = @UpdateCount,
                 --@DeleteCount = @DeleteCount,
                 @SourceCount = @SourceCount,
                 @AuditId = @AuditId;
--=============================================
COMMIT TRANSACTION
END TRY

BEGIN CATCH

--=======================================================
--ON Error Event Handler to rollback the transaction
--=======================================================
EXEC CSS_Staging.[dbo].[spstagingErrorAuditLog] @AuditId = @AuditId;
--=========================================================================
DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @ErrorSeverity INT;
    DECLARE @ErrorState INT;

    SELECT @ErrorMessage = ERROR_MESSAGE(),
           @ErrorSeverity = ERROR_SEVERITY(),
           @ErrorState = ERROR_STATE();

    RAISERROR (@ErrorMessage, -- Message text.
               @ErrorSeverity, -- Severity.
               @ErrorState -- State.
               );
END CATCH
END