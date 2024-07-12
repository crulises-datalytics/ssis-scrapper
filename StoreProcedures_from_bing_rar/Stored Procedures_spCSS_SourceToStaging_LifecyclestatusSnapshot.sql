CREATE PROCEDURE [dbo].[spCSS_SourceToStaging_LifecyclestatusSnapshot] (@AsOfFiscalWeek int)
AS
BEGIN
DECLARE @tableVar TABLE (MergeAction VARCHAR(20));
Declare @AuditId BIGINT,
		@SourceCount INT, 
        @InsertCount INT,
		@UpdateCount INT,
		@DeleteCount INT,
		@TaskName VARCHAR(100)= 'LifecyclestatusSnapshot';;
IF OBJECT_ID('tempdb..#SourceCurrentWeek') IS NOT NULL DROP TABLE #SourceCurrentWeek;
CREATE Table #SourceCurrentWeek([AsOfFiscalWeek] [int] NOT NULL,
    [ctr_no] [varchar](4) NOT NULL,
    [fam_no] [varchar](4) NOT NULL,
    [stu_no] [varchar](4) NOT NULL,
    [LifecycleStatusKey] [int] NOT NULL);
    Insert into  #SourceCurrentWeek 
( [AsOfFiscalWeek]
          ,[ctr_no]
          ,[fam_no]
          ,[stu_no]
          ,[LifecycleStatusKey])
exec pLifecycleStatus @ASOfFiscalWeek,@SourceCount;

CREATE CLUSTERED INDEX CIX_TempSourceCurrentWeek ON #SourceCurrentWeek
																	([AsOfFiscalWeek] ASC
																   ,[ctr_no] ASC
																   ,[fam_no] ASC
																   ,[stu_no] ASC
																 );
 Set @sourcecount=(Select count(1) from #SourceCurrentWeek);

 ----------------------Pre Execute Event Handler-------------------------
            EXEC CSS_Staging.[dbo].[spStagingBeginAuditLog]
            @SourceName = @TaskName,
            @AuditId = @AuditId OUTPUT;
--------------------------------------------------------------------------
BEGIN TRY
BEGIN TRANSACTION
    MERGE LifeCycleStatusSnapshot AS T
    USING #SourceCurrentWeek AS S
    ON (T.[AsOfFiscalWeek] =S.[AsOfFiscalWeek] and T.[ctr_no] =S.[ctr_no]
and T.[fam_no] =S.[fam_no] and
T.[stu_no] =S.[stu_no]
)
    WHEN MATCHED and 
     T.[LifecycleStatusKey] <> S.[LifecycleStatusKey]

         THen
    Update 
        set 
         T.[LifecycleStatusKey] =S.[LifecycleStatusKey],
	T.StgModifiedDate=getdate(),
	T.StgModifiedBy=suser_sname()
    WHEN NOT MATCHED By Target THEN
    INSERT 
                    (
                
                                    [AsOfFiscalWeek]
          ,[ctr_no]
          ,[fam_no]
          ,[stu_no]
          ,[LifecycleStatusKey]
                                    
                )
    Values
    ( 
    S.[AsOfFiscalWeek]
, S.[ctr_no]
, S.[fam_no]
, S.[stu_no]
, S.[LifecycleStatusKey]

    )

    --SOFT Delete Optional
    --WHEN NOT MATCHED BY SOURCE AND T.Deleted is null
    --        THEN 
    --        UPDATE
    --        Set    Deleted = GetDate()
    ------------------------------------------------------------------------------------
    --Audit Log Counts
    ------------------------------------------------------------------------------------
    OUTPUT $action INTO @tableVar; 
        SELECT 
            @sourcecount as vSourceCount ,
            SUM(Inserted) as vInsertCount ,
            SUM(Updated) as vUpdateCount 
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

Set @UpdateCount = (select count(*) From @tableVar Where MergeAction='UPDATE')            
Set @InsertCount= (Select Count(*) From @tableVar Where MergeAction='INSERT')
 ------------------PostExecute Event Handler-------------------------------
 EXEC CSS_Staging.[dbo].[spStagingEndAuditLog]
                 @InsertCount = @InsertCount,
                 @UpdateCount = @UpdateCount,
                 @DeleteCount = @DeleteCount,
                 @SourceCount = @SourceCount,
                 @AuditId = @AuditId;
-------------------****************************--------------------------------
COMMIT TRANSACTION
END TRY

BEGIN CATCH

ROLLBACK TRANSACTION
------On ERROR EVENT Handler------------
EXEC CSS_Staging.[dbo].[spstagingErrorAuditLog]
                 @AuditId = @AuditId;
------**************************---------------
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
End