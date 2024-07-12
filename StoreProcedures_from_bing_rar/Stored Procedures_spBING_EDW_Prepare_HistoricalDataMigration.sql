CREATE PROCEDURE [dbo].[spBING_EDW_Prepare_HistoricalDataMigration] 
(@EDWRunDateTime DATETIME2 = NULL,
 @DebugMode      INT       = NULL
)
AS
     BEGIN
         -- ================================================================================
         -- 
         -- Stored Procedure:   spBING_EDW_Prepare_HistoricalDataMigration
         --
         -- Purpose:            Clears down and prepares BING_EDW Fact & Dimension tables.
         --                     For initial Data Migration of historical data from BING_EDW
         --                         source systems and the Legacy DW, we will execute a full
         --                         historical load into a 'clean' BING EDW.   
         --                     In order for this to be a repeatable process, we want to be able to 
         --                         reload the data from scratch each time.	  
         --
         --                     This stored procedure controls the data clear-dwon of Fact and Dimension 
         --                         tables in BING_EDW, in preperation of a full historical load.	   	    	    	      	    
         --
         -- Usage:              EXEC dbo.spBING_EDW_Prepare_HistoricalDataMigration @DebugMode = 1
         --
         -- --------------------------------------------------------------------------------
         --
         -- Change Log:		   
         -- ----------
         --
         -- Date         Modified By     Comments
         -- ----         -----------     --------
         --
         -- 12/11/17     sburke          BNG-527 - Refactor EDW Historical (Data Migration) load
         --  1/16/18     sburke          BNG-998 - Add call to spBING_EDW_Build_DimensionSeedRows
         --                                  to ensure -1 (unknown) and -2 (Not Applicable) seed 
         --                                  rows are populated.
         --  1/22/18     sburke          BNG-1006 - Add CMS tables
         --  2/02/18     sburke          BNG-250 - Add DimLeadType
         --  2/13/18     sburke          BNG-252 - Add SalesForce EDWETLBatchControl cleardown
         --  4/10/18     sburke          BNG-1589 - Add MISC_Staging and Cambridge_Staging EDWETLBatchControl cleardown
         --                                         Also add cleardowns of remaining HR Dimension & Fact tables
         --  5/01/18     sburke          BNG-1671 - Add initial values to the CSS_Staging..StagingETLBatchControl table,
         --                                  which drives how far back in history we go for the CSS Historical load
         --                                         Also call spBING_EDW_Build_DimensionStaticData sproc from here.
         --			 
         -- ================================================================================
         SET NOCOUNT ON;
         --
         -- Housekeeping Variables
         -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
         DECLARE @SourceName VARCHAR(100)= 'PrepareDataMigration - BING_EDW';
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
		   -- ================================================================================
		   -- BING_EDW FACT TABLES
		   -- ================================================================================
             TRUNCATE TABLE [dbo].[FactAdjustment];
             TRUNCATE TABLE [dbo].[FactARBalanceSnapshot];
             TRUNCATE TABLE [dbo].[FactBilling];
             TRUNCATE TABLE [dbo].[FactCenterStatSnapshot];
             TRUNCATE TABLE [dbo].[FactEmployeeAssessment];
             TRUNCATE TABLE [dbo].[FactEmployeeAssignment];
             TRUNCATE TABLE [dbo].[FactEmployeeCompliance];
             TRUNCATE TABLE [dbo].[FactEmployeeLeave];
             TRUNCATE TABLE [dbo].[FactEmployeePayRate];
             TRUNCATE TABLE [dbo].[FactEmployeePerformance];
             TRUNCATE TABLE [dbo].[FactEmployeeQualification];
             TRUNCATE TABLE [dbo].[FactFTESnapshot];
             TRUNCATE TABLE [dbo].[FactGLBalance];
             TRUNCATE TABLE [dbo].[FactGLBalancePlanAllocation];
             TRUNCATE TABLE [dbo].[FactLeadEvent];
             TRUNCATE TABLE [dbo].[FactLeadPipeline];
             TRUNCATE TABLE [dbo].[FactNetRevenue];
             TRUNCATE TABLE [dbo].[FactPayment];
             TRUNCATE TABLE [dbo].[FactPaymentApplied];
             TRUNCATE TABLE [dbo].[FactPersonSpecialInfo];
             TRUNCATE TABLE [dbo].[FactSessionEnrollment];
             TRUNCATE TABLE [dbo].[FactTierAssignment];

		   -- ================================================================================
		   -- BING_EDW BRIDGE TABLES
		   -- ================================================================================
             TRUNCATE TABLE [dbo].[BridgeCompanyRollup];
             TRUNCATE TABLE [dbo].[BridgeSecurityPersonHRISGroup];
             TRUNCATE TABLE [dbo].[BridgeSecurityPersonOrg];

             -- ================================================================================
		   -- BING_EDW DIMENSION TABLES
		   -- ================================================================================
             TRUNCATE TABLE [dbo].[DimAccountSubaccount];
             TRUNCATE TABLE [dbo].[DimAdjustmentReason];
             TRUNCATE TABLE [dbo].[DimARAgingBucket];
             TRUNCATE TABLE [dbo].[DimARBalanceType];
             TRUNCATE TABLE [dbo].[DimAssessmentType];
             TRUNCATE TABLE [dbo].[DimAssignmentType];
             TRUNCATE TABLE [dbo].[DimClassroom];
             TRUNCATE TABLE [dbo].[DimCompany];
             TRUNCATE TABLE [dbo].[DimCompanyRollup];
             TRUNCATE TABLE [dbo].[DimComplianceItem];
             TRUNCATE TABLE [dbo].[DimComplianceRating];
             TRUNCATE TABLE [dbo].[DimCostCenter];
             TRUNCATE TABLE [dbo].[DimCostCenterType];
             TRUNCATE TABLE [dbo].[DimCreditMemoType];
             TRUNCATE TABLE [dbo].[DimDataScenario];
             TRUNCATE TABLE [dbo].[DimDate];
             TRUNCATE TABLE [dbo].[DimDiscountType];
             TRUNCATE TABLE [dbo].[DimFeeType];
             TRUNCATE TABLE [dbo].[DimGLMetricType];
             TRUNCATE TABLE [dbo].[DimHRUser];
             TRUNCATE TABLE [dbo].[DimInvoiceType];
             TRUNCATE TABLE [dbo].[DimLead];
             TRUNCATE TABLE [dbo].[DimLeadType];
             TRUNCATE TABLE [dbo].[DimLeadEventType];
             TRUNCATE TABLE [dbo].[DimLifecycleStatus];
             TRUNCATE TABLE [dbo].[DimLocation];
             TRUNCATE TABLE [dbo].[DimOrganization];
             TRUNCATE TABLE [dbo].[DimPayBasis];
             TRUNCATE TABLE [dbo].[DimPayGrade];
             TRUNCATE TABLE [dbo].[DimPaymentType];
             TRUNCATE TABLE [dbo].[DimPayRateChangeReason];
             TRUNCATE TABLE [dbo].[DimPeopleGroup];
             TRUNCATE TABLE [dbo].[DimPerformanceRating];
             TRUNCATE TABLE [dbo].[DimPerson];
             TRUNCATE TABLE [dbo].[DimPosition];
             TRUNCATE TABLE [dbo].[DimProgram];
             TRUNCATE TABLE [dbo].[DimQualificationType];
             TRUNCATE TABLE [dbo].[DimReportProfile];
             TRUNCATE TABLE [dbo].[DimScheduleType];
             TRUNCATE TABLE [dbo].[DimScheduleWeek];
             TRUNCATE TABLE [dbo].[DimSession];
             TRUNCATE TABLE [dbo].[DimSpecialInfo];
             TRUNCATE TABLE [dbo].[DimSponsor];
             TRUNCATE TABLE [dbo].[DimStudent];
             TRUNCATE TABLE [dbo].[DimTier];
             TRUNCATE TABLE [dbo].[DimTimeCalculation];
             TRUNCATE TABLE [dbo].[DimTransactionCode];
             TRUNCATE TABLE [dbo].[DimTuitionAssistanceProvider];
             TRUNCATE TABLE [dbo].[DimWebCampaign];

		   -- ================================================================================
		   -- BING_EDW ETL BATCH AUDIT TABLES
		   -- ================================================================================
		   -- 
		   -- Do NOT Clear-down [dbo].[EDWAuditLog], as we want to keep high-level history of previous loads
		   --
             TRUNCATE TABLE [dbo].[EDWBatchLoadLog]; -- OK to delete this

		   -- ================================================================================
		   -- BING_EDW ETL BATCH AUDIT TABLES
		   -- ================================================================================
             DELETE FROM GL_Staging.dbo.EDWETLBatchControl;
             DELETE FROM HR_Staging.dbo.EDWETLBatchControl;
             DELETE FROM CMS_Staging.dbo.EDWETLBatchControl;
             DELETE FROM CSS_Staging.dbo.EDWETLBatchControl;
             DELETE FROM SalesForce_Staging.dbo.EDWETLBatchControl;
             DELETE FROM MISC_Staging.dbo.EDWETLBatchControl;
             DELETE FROM Cambridge_Staging.dbo.EDWETLBatchControl;
             DELETE FROM SalesForce_Staging.dbo.EDWETLBatchControl;

		   -- ================================================================================
		   -- SET HISTORICAL 'START' DATE FOR CSS FULL HISTORY LOAD
		   -- ================================================================================
             DELETE FROM CSS_Staging..StagingETLBatchControl;
             INSERT INTO CSS_Staging..StagingETLBatchControl
             VALUES
             ('Weekly',
              '19930301',
              1993,
              1
             );
             INSERT INTO CSS_Staging..StagingETLBatchControl
             VALUES
             ('Yearly',
              NULL,
              1993,
              NULL
             ); 

		   -- ================================================================================
		   -- POPULATE SEED ROWS IN DIMENSION TABLES
		   -- ================================================================================
             EXEC [dbo].[spBING_EDW_Build_DimensionSeedRows]
                  @DebugMode = @DebugMode;

		   -- ================================================================================
		   -- POPULATE STATIC DIMENSION TABLES
		   -- ================================================================================
             EXEC dbo.spBING_EDW_Build_DimensionStaticData
                  @DebugMode = @DebugMode;
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
GO


