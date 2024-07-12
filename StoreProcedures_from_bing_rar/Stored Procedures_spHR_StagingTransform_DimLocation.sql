
/*
IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'spHR_StagingTransform_DimLocation'
)
    DROP PROCEDURE dbo.spHR_StagingTransform_DimLocation;
GO
*/
CREATE PROCEDURE [dbo].[spHR_StagingTransform_DimLocation] @EDWRunDateTime DATETIME2 = NULL
AS
    -- ================================================================================
    -- 
    -- Stored Procedure:   spHR_StagingTransform_DimLocation
    --
    -- Purpose:            Performs the transformation logic with the source database
    --                         for a given Fact or Dimension table, and returns the
    --                         results set to the caller (usually for populating a
    --                         temporary table).
    --
    -- Parameters:         @EDWRunDateTime - Used to populate any 'current date' fields, rather than
    --                         making numerous GETDATE() calls  
    --				   
    -- Returns:            Results set containing the transformed data ready for
    --                         consumption by the ETL process for load into BING_EDW
    --
    -- Usage:              DECLARE @EDWRunDateTime DATETIME2 = GETDATE();              
    --                     INSERT #DimLocationUpsert -- (Temporary table)
    --                     EXEC dbo.spHR_StagingTransform_DimLocation @EDWRunDateTime
    -- 
    --
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date        Modified By         Comments
    -- ----        -----------         --------
    --
    -- 10/16/17    sburke              BNG-255 - Initial version of proc, loading HR Location
    --                                     data from HR_Staging (replacing the CMS, PRO and
    --                                     Horizon loads)
    --			 
    -- ================================================================================
     BEGIN
         SET NOCOUNT ON;
	    --
	    -- Housekeeping Variables
	    -- 
         DECLARE @ProcName NVARCHAR(500)= OBJECT_NAME(@@PROCID);
         DECLARE @DebugMsg NVARCHAR(500);
	    --
	    -- If we do not get an @EDWRunDateTime input, set to current date
	    --
         IF @EDWRunDateTime IS NULL
             SET @EDWRunDateTime = GETDATE();  
	    --
	    -- Execute the extract / transform from the Staging database source
	    --
         BEGIN TRY
             SELECT LocationID AS LocationID,
                    COALESCE(ShipToLocationID, -1) AS ShipToLocationID,
                    COALESCE(LocationName, 'Unknown Location') AS LocationName,
                    COALESCE(LocationAddressLine1, 'No Address') AS LocationAddressLine1,
                    COALESCE(LocationAddressLine2, 'No Address') AS LocationAddressLine2,
                    COALESCE(LocationAddressLine3, 'No Address') AS LocationAddressLine3,
                    COALESCE(LocationCity, 'Unknown City') AS LocationCity,
                    COALESCE(LocationState, 'Unknown Org') AS LocationState,
                    COALESCE(LocationZip, '-1') AS LocationZip,
                    COALESCE(LocationCounty, 'Unknown County') AS LocationCounty,
                    COALESCE(LocationCountry, 'Unknown Country') AS LocationCountry,
                    COALESCE(LocationPhone, 'Unknown Phone') AS LocationPhone,
                    COALESCE(LocationSecondaryPhone, 'No Secondary Phone') AS LocationSecondaryPhone,
                    COALESCE(LocationFax, 'Unknown Fax') AS LocationFax,
                    COALESCE(LocationEmail, 'Unknown Email') AS LocationEmail,
                    COALESCE(LocationADPCode, '-1') AS LocationADPCode,
                    COALESCE(LocationWarehouseFlag, 'Unknown Warehouse') AS LocationWarehouseFlag,
                    COALESCE(LocationShipToSiteFlag, 'Unknown Ship To Site') AS LocationShipToSiteFlag,
                    COALESCE(LocationReceivingSiteFlag, 'Unknown Receiving Site') AS LocationReceivingSiteFlag,
                    COALESCE(LocationBillToSiteFlag, 'Unknown Bill To Site') AS LocationBillToSiteFlag,
                    COALESCE(LocationInOrganizationFlag, 'Unknown In Organization') AS LocationInOrganizationFlag,
                    COALESCE(LocationOfficeSiteFlag, 'Unknown Office Site') AS LocationOfficeSiteFlag,
                    COALESCE(LocationEEOCostCenterName, NULL) AS LocationEEOCostCenterName,
                    COALESCE(LocationEEOCostCenterNumber, NULL) AS LocationEEOCostCenterNumber,
                    COALESCE(LocationEEOApprenticesEmployedFlag, NULL) AS LocationEEOApprenticesEmployedFlag,
                    COALESCE(LocationEEOGovernmentContractorFlag, NULL) AS LocationEEOGovernmentContractorFlag,
                    COALESCE(LocationEEOMainActivitiesLine1, NULL) AS LocationEEOMainActivitiesLine1,
                    COALESCE(LocationEEOMainActivitiesLine2, NULL) AS LocationEEOMainActivitiesLine2,
                    COALESCE(LocationEEOReportedPreviouslyFlag, NULL) AS LocationEEOReportedPreviouslyFlag,
                    COALESCE(LocationEEOHeadquartersEstablishmentFlag, NULL) AS LocationEEOHeadquartersEstablishmentFlag,
                    COALESCE(LocationDUNSNumber, NULL) AS LocationDUNSNumber,
                    COALESCE(LocationSICNumber, NULL) AS LocationSICNumber,
                    COALESCE(LocationNAICSNumber, NULL) AS LocationNAICSNumber,
                    COALESCE(LocationFEINNumber, NULL) AS LocationFEINNumber,
                    COALESCE(LocationCreatedUser, -1) AS LocationCreatedUser,
                    COALESCE(LocationCreatedDate, '1/1/1900') AS LocationCreatedDate,
                    COALESCE(LocationModifiedUser, -1) AS LocationModifiedUser,
                    COALESCE(LocationModifiedDate, '1/1/1900') AS LocationModifiedDate,
                    @EDWRunDateTime AS EDWEffectiveDate,
                    NULL AS EDWEndDate,
                    @EDWRunDateTime AS EDWCreatedDate,
                    CAST(SYSTEM_USER AS VARCHAR(50)) AS EDWCreatedBy,
                    NULL AS Deleted
             FROM [dbo].[tfnHR_StagingGenerate_Locations_DimLocation]();
         END TRY
	    --
	    -- Catch, and throw the error back to the calling procedure or client
	    --
         BEGIN CATCH
             DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
             SELECT @ErrMsg = 'Sub-procedure '+@ProcName+' - '+ERROR_MESSAGE(),
                    @ErrSeverity = ERROR_SEVERITY();
             RAISERROR(@ErrMsg, @ErrSeverity, 1);
         END CATCH;
     END;
GO
