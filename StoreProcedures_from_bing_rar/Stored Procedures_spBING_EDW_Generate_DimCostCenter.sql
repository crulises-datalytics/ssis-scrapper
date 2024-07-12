CREATE PROCEDURE [dbo].[spBING_EDW_Generate_DimCostCenter]
(
 @EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
     BEGIN
         -- ================================================================================
         -- 
         -- Stored Procedure:   spBING_EDW_Generate_DimCostCenter
         --
         -- Purpose:            Populates the DimCostCenter table in BING_EDW.
         --
         -- --------------------------------------------------------------------------------
         --
         -- Change Log:		   
         -- ----------
         --
         -- Date         Modified By     Comments
         -- ----         -----------     --------
         --
         -- 3/07/17     Jimmy Ji    	
		 -- 06/29/22    hhebbalu         BI-6236 EffectiveFrom and EffectiveTo is mapped wrong in 
		 --                              INSERT statement. Fixed it
         --			 
         -- ================================================================================
         SET NOCOUNT ON;
         --
         -- Housekeeping Variables
         -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'DimMartCostCenter';
         DECLARE @AuditId BIGINT;
         --
         -- ETL status Variables
         --
         DECLARE @RowCount INT;
         DECLARE @Error INT;
         DECLARE @SourceCount INT= 0;
         DECLARE @InsertCount INT= 0;
         DECLARE @UpdateCount INT= 0;
         DECLARE @DeleteCount INT= 0;	

		 SELECT @SourceCount=count(1) from [staging].[dbo].[DimMartCostCenterLanding]
         --
         -- If we do not get an @EDWRunDateTime input, set to current date
         --
         IF @EDWRunDateTime IS NULL
             SET @EDWRunDateTime = GETDATE();
         --
         IF @DebugMode = 1
             SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Starting.';
         PRINT @DebugMsg;

         --
         -- Write to EDW AuditLog we are starting
         --
         EXEC [dbo].[spEDWBeginAuditLog]
              @SourceName = @SourceName,
              @AuditId = @AuditId OUTPUT; 		 	 
         --
         BEGIN TRY
            
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Deleted '+CONVERT(NVARCHAR(20), @DeleteCount)+' from Target.';
                     PRINT @DebugMsg;
             END;
             --
             -- Add Seed row
             --
            update a
			set 
			a.CostCenterNumber=b.CostCenterNumber,
			a.EDWEffectiveFrom=b.EDWEffectiveFrom,
			A.EDWEffectiveTo=b.EDWEffectiveTo
			from dbo.DimMartCostCenter as a
			join staging.dbo.DimMartCostcenterLanding as b
			on a.CostCenterKey=b.Costcenterkey
			and
			(a.CostCenterNumber!=b.CostCenterNumber or
			a.EDWEffectiveFrom!=b.EDWEffectiveFrom or
			A.EDWEffectiveTo!=b.EDWEffectiveTo)
			SELECT @UpdateCount = @@ROWCOUNT

			insert into dbo.DimMartCostCenter
			select	a.[CostCenterKey]
					,a.[CostCenterNumber]
					,a.[EDWEffectiveFrom]
					,a.[EDWEffectiveTo]
			from staging.dbo.dimmartcostcenterlanding as a
			left join dbo.DimMartCostCenter as b
			on a.CostCenterKey=b.CostCenterKey
			where b.CostCenterKey is null
			select @InsertCount=@@ROWCOUNT

             
             IF @DebugMode = 1
                 BEGIN
                     SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Inserted '+CONVERT(NVARCHAR(20), @InsertCount)+' into Target.';
                     PRINT @DebugMsg;
             END;

             --
             -- Write our successful run to the EDW AuditLog 
             --
             EXEC [dbo].[spEDWEndAuditLog]
                  @InsertCount = @InsertCount,
                  @UpdateCount = @UpdateCount,
                  @DeleteCount = @DeleteCount,
                  @SourceCount = @SourceCount,
                  @AuditId = @AuditId;

             -- Debug output progress
             IF @DebugMode = 1
                 SELECT @DebugMsg = @ProcName+' : '+CONVERT(NVARCHAR(20), GETDATE())+' - Completing successfully.';
             PRINT @DebugMsg;
         END TRY
	    --
	    -- Catch, and throw the error back to the calling procedure or client
	    --
         BEGIN CATCH
		   --
		   -- Write our failed run to the EDW AuditLog 
		   --
             EXEC [dbo].[spEDWErrorAuditLog]
                  @AuditId = @AuditId;
             DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
             SELECT @ErrMsg = 'Sub-procedure '+@ProcName+' - '+ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;