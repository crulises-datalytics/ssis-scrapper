
Create Procedure [dbo].[spXtmaIndividualEmailResultCMerge] As
 -- ================================================================================
    -- 
    -- Stored Procedure:   spSalesforce_StagingspXtmaIndividualEmailResultCMerge
    --
    -- Purpose:            Inserts and Update the data from XtmaIndividualEmailResultCLAnding in to XtmaIndividualEmailResultC
    --                         
    --                         
    -- --------------------------------------------------------------------------------
    --
    -- Change Log:		   
    -- ----------
    --
    -- Date        Modified By        Comments
    -- ----        -----------        --------
    --
    -- 02/16/18    Adevabhakthuni     Merge XtmaIndividualEmailResultC using XtmaIndividualEmailResultCrLanding
    --			 
    -- ================================================================================

DECLARE @tableVar TABLE (MergeAction VARCHAR(20))
--SYNCHRONIZE THE Targt TABLE WITH REFRESHED DATA FROM SOURCE TABLE
MERGE XtmaIndividualEmailResultC AS T
USING XtmaIndividualEmailResultCLanding AS S
ON (T.id = S.id) 
--WHEN RECORDS ARE MATCHED, UPDATE THE RECORDS IF THERE IS ANY CHANGE
WHEN MATCHED 
THEN 

Update   SET
T.OwnerId = S.OwnerId ,
T.IsDeleted = S.IsDeleted ,
T.Name = S.Name ,
T.CreatedDate = S.CreatedDate ,
T.CreatedById = S.CreatedById ,
T.LastModifiedDate = S.LastModifiedDate ,
T.LastModifiedById = S.LastModifiedById ,
T.SystemModStamp = S.SystemModStamp ,
T.CampaignC = S.CampaignC ,
T.ContactC = S.ContactC ,
T.DateBouncedC = S.DateBouncedC ,
T.DateTimeOpenedC = S.DateTimeOpenedC ,
T.DateTimeSentC = S.DateTimeSentC ,
T.DateUnsubscribedC = S.DateUnsubscribedC ,
T.FromAddressC = S.FromAddressC ,
T.FromNameC = S.FromNameC ,
T.LeadC = S.LeadC ,
T.NumberOfTotalClicksC = S.NumberOfTotalClicksC ,
T.NumberOfUniqueClicksC = S.NumberOfUniqueClicksC ,
T.OpenedC = S.OpenedC ,
T.ReportNameC = S.ReportNameC ,
T.SubjectLineC = S.SubjectLineC ,
T.StgModifiedby =suser_name(),
T.StgModifiedDate = getdate()

 
--WHEN NO RECORDS ARE MATCHED, INSERT THE INCOMING RECORDS FROM SOURCE TABLE TO Tgt TABLE
WHEN NOT MATCHED BY Target THEN 
INSERT (Id,OwnerId,IsDeleted,Name,CreatedDate,CreatedById,LastModifiedDate,LastModifiedById,SystemModStamp,CampaignC,ContactC,DateBouncedC,DateTimeOpenedC,DateTimeSentC,DateUnsubscribedC,FromAddressC,FromNameC,LeadC,NumberOfTotalClicksC,NumberOfUniqueClicksC,OpenedC,ReportNameC,SubjectLineC)
VALUES (S.Id,S.OwnerId,S.IsDeleted,S.Name,S.CreatedDate,S.CreatedById,S.LastModifiedDate,S.LastModifiedById,S.SystemModStamp,S.CampaignC,S.ContactC,S.DateBouncedC,S.DateTimeOpenedC,S.DateTimeSentC,S.DateUnsubscribedC,S.FromAddressC,S.FromNameC,S.LeadC,S.NumberOfTotalClicksC,S.NumberOfUniqueClicksC,S.OpenedC,S.ReportNameC,S.SubjectLineC)

------------------------------------------------------------------------------------
--Audit Log Counts
------------------------------------------------------------------------------------
OUTPUT $action INTO @tableVar; 
	SELECT  
		SUM(Inserted) as vInsertCount
		,SUM(Updated) as vUpdateCount
		
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
  ) a