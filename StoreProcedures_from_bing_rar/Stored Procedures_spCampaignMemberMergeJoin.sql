
CREATE Procedure [dbo].[spCampaignMemberMergeJoin] As
 -- ================================================================================
    -- 
    -- Stored Procedure:   spCMS_StagingCampaignMemberMergeJOin
    --
    -- Purpose:            Inserts and Update the data from CampainMemberLAnding in to CampaignMember
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
    -- 02/15/18    Adevabhakthuni     Merge CampaignMember using CampaignMemberLanding
    --			 
    -- ================================================================================

DECLARE @tableVar TABLE (MergeAction VARCHAR(20))
--SYNCHRONIZE THE Targt TABLE WITH REFRESHED DATA FROM SOURCE TABLE
MERGE CampaignMember AS T
USING CampaignMemberLanding AS S
ON (T.id = S.id) 
--WHEN RECORDS ARE MATCHED, UPDATE THE RECORDS IF THERE IS ANY CHANGE
WHEN MATCHED 
THEN 

Update   SET
T.CampaignId=S.CampaignId ,
T.ContactId=S.ContactId ,
T.CreatedById=S.CreatedById ,
T.CreatedDate=S.CreatedDate ,
T.FirstRespondedDate=S.FirstRespondedDate ,
T.HasResponded=S.HasResponded ,
T.IsDeleted=S.IsDeleted ,
T.LastModifiedById=S.LastModifiedById ,
T.LastModifiedDate=S.LastModifiedDate ,
T.LeadId=S.LeadId,
T.Status=S.Status,
T.SystemModstamp=S.SystemModstamp,
T.StgModifiedby =suser_name()
,T.StgModifiedDate = getdate()

 
--WHEN NO RECORDS ARE MATCHED, INSERT THE INCOMING RECORDS FROM SOURCE TABLE TO Tgt TABLE
WHEN NOT MATCHED BY Target THEN 
INSERT (CampaignId, ContactId, CreatedById, CreatedDate, FirstRespondedDate, HasResponded, Id, IsDeleted, LastModifiedById, LastModifiedDate, LeadId, Status, SystemModstamp) 
VALUES (S.CampaignId,S.ContactId,S.CreatedById,S.CreatedDate,S.FirstRespondedDate,S.HasResponded,S.Id,S.IsDeleted,S.LastModifiedById,S.LastModifiedDate,S.LeadId,S.Status,S.SystemModstamp)

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