-- SB 11/17/2017 - BNG-850
--                 This Stored Procedure is superceded by 
--                     spHR_StagingGenerate_OrgLeaderAccess_DimOraganization,
--                     so it will be removed from the project.  Commenting-out for now
-- 
/*
CREATE   PROCEDURE [dbo].[pOrgLeaderAccess] AS

--drop table #OrgAccess

--Get Orgs.
select * into #Orgs from vOrgs;

with FlattenedOrgs as (
	--Flatten the parent-child Org Hierarchy into ten levels.
	select OrganizationID, OrgName, OrgTypeCode, HierarchyLevelID = cast(left(ID, 2) as int), HierarchyOrganizationID = cast(right(ID, len(ID) - 3) as int)
	from (
		select a.OrganizationID, a.OrgName, a.OrgTypeCode,
		Level1  = '01.' + cast(a.OrganizationID as varchar),
		Level2  = '02.' + cast(a.ParentOrganizationID as varchar),
		Level3  = '03.' + cast(b.ParentOrganizationID as varchar), 
		Level4  = '04.' + cast(c.ParentOrganizationID as varchar), 
		Level5  = '05.' + cast(d.ParentOrganizationID as varchar), 
		Level6  = '06.' + cast(e.ParentOrganizationID as varchar), 
		Level7  = '07.' + cast(f.ParentOrganizationID as varchar), 
		Level8  = '08.' + cast(g.ParentOrganizationID as varchar),
		Level9  = '09.' + cast(h.ParentOrganizationID as varchar),
		Level10 = '10.' + cast(j.ParentOrganizationID as varchar)
		from #Orgs a
		left join #Orgs b on a.ParentOrganizationID = b.OrganizationID
		left join #Orgs c on b.ParentOrganizationID = c.OrganizationID
		left join #Orgs d on c.ParentOrganizationID = d.OrganizationID
		left join #Orgs e on d.ParentOrganizationID = e.OrganizationID
		left join #Orgs f on e.ParentOrganizationID = f.OrganizationID
		left join #Orgs g on f.ParentOrganizationID = g.OrganizationID
		left join #Orgs h on g.ParentOrganizationID = h.OrganizationID
		left join #Orgs j on h.ParentOrganizationID = j.OrganizationID
	) a
	unpivot (ID for LevelID in (Level1, Level2, Level3, Level4, Level5, Level6, Level7, Level8, Level9, Level10)) u
), OrgHierarchy as (
	select a.*, HierarchyOrgName = b.OrgName, HierarchyOrgTypeCode = b.OrgTypeCode 
	from FlattenedOrgs a
	left join #Orgs b on a.HierarchyOrganizationID = b.OrganizationID
), OrgTypeAccess as (
	select * 
	from Shawn_Scratch.dbo.OrgTypeAccess
), CanAccessOrgs as (
	select distinct a.OrganizationID, a.OrgName, a.OrgTypeCode, a.HierarchyOrganizationID, a.HierarchyOrgName, a.HierarchyOrgTypeCode
	from OrgHierarchy a
	inner join OrgTypeAccess b on a.OrgTypeCode = b.OrgTypeCode and a.HierarchyOrgTypeCode = b.CanAccessOrgTypeCode
), LevelUpAccess as (
	select distinct b.OrganizationID, b.OrgName, b.OrgTypeCode, CanAccessOrganizationID = a.OrganizationID, CanAccessOrgName = a.OrgName, CanAccessOrgTypeCode = a.OrgTypeCode 
	from OrgHierarchy a
	inner join CanAccessOrgs b on a.HierarchyOrganizationID = b.HierarchyOrganizationID
), NormalAccess as (
	select OrganizationID = HierarchyOrganizationID, OrgName = HierarchyOrgName, OrgTypeCode = HierarchyOrgTypeCode,
	CanAccessOrganizationID = OrganizationID, CanAccessOrgName = OrgName, CanAccessOrgTypeCode = OrgTypeCode  
	from OrgHierarchy
)
select a.*, 
OrgSelfFlag = case when b.OrganizationID = b.CanAccessOrganizationID then 'Y' else 'N' end,
OrgSelfDescendantsFlag = case when b.OrganizationID is not null then 'Y' else 'N' end,
OrgLevelUpDescendantsFlag = 'Y'
into #OrgAccess
from LevelUpAccess a
left join NormalAccess b on a.OrganizationID = b.OrganizationID and a.CanAccessOrganizationID = b.CanAccessOrganizationID

--Get the list of Jobs that pertain to Org Levels.  For example, a Center Director is assigned to a Center; a District Leader is assigned to a District.
select 
OrgDivisionName		= left(KLC_VALUE, charindex('.', KLC_VALUE) - 1), 
JobCode				= left(right(KLC_VALUE, len(KLC_VALUE) - charindex('.', KLC_VALUE)), 3), 
JobName				= right(right(KLC_VALUE, len(KLC_VALUE) - charindex('.', KLC_VALUE)), len(right(KLC_VALUE, len(KLC_VALUE) - charindex('.', KLC_VALUE))) - 4),
OrgLevelName		= left(PARTNER_VALUE, charindex('.', PARTNER_VALUE) - 1),
OrgLevelSequence    = left(right(PARTNER_VALUE, len(PARTNER_VALUE) - charindex('.', PARTNER_VALUE)), 1),
JobActingFlag		= right(PARTNER_VALUE, len(PARTNER_VALUE) - 2 - charindex('.', PARTNER_VALUE))
into #OrgJobs
from GL_Staging.dbo.xxklcoilxrefvaluesv a
where partner_name = 'BING' and type_name = 'ORG_JOB'

--Get rid of these deletes later.
delete from #OrgJobs where JobCode = 173 and OrgLevelName = 'CENTER'
delete from #OrgJobs where JobCode = 605 and OrgLevelName = 'CENTER'

--drop table #Assignments

--Get all the active Assignments of Person to Org, depending on their Job.
select 
a.AssignmentID, 
a.OrganizationID, 
a.LocationID, 
a.PositionID, 
b.JobID,
b.JobCode, 
b.JobName, 
c.OrgDivisionName,
--e.OrgLevelID,
e.OrgLevelName,
e.OrgLevelSequence, 
e.JobActingFlag, 
a.PersonID, 
d.EmployeeNumber, 
d.PersonFullName, 
c.OrgName, 
c.OrgTypeCode,
AssignmentStatusTypeName,
AssignmentEffectiveDate,
AssignmentEndDate,
AssignmentPositionEffectiveDate
into #Assignments
from vAssignments a
inner join vPositions b on a.PositionID = b.PositionID and a.AssignmentEffectiveDate between b.PositionEffectiveDate and b.PositionEndDate
left join vOrgs c on a.OrganizationID = c.OrganizationID
left join vPeople d on a.PersonID = d.PersonID and a.AssignmentEffectiveDate between d.PersonEffectiveDate and d.PersonEndDate
inner join #OrgJobs e on c.OrgDivisionName = e.OrgDivisionName and b.JobCode = e.JobCode --and e.OrgDivisionName = 'KinderCare Field'
where AssignmentEndDate >= dateadd(yy, 1000, getdate())
and AssignmentEffectiveDate <= getdate()
and AssignmentStatusTypeID <> 3;

--drop table #FieldAssignments

--Get all assignments, with Acting and Primary flags set.
with SequencedAssignments as (
	--There are multiple jobs associated with a given level of the hierarchy.  Grab the highest sequence we have for that Org.
	select OrganizationID, JobActingFlag, MaxJobOrgLevelSequence = max(OrgLevelSequence) 
	from #Assignments
	group by OrganizationID, JobActingFlag
), LongestSequencedAssignments as (
	--If there are multiple people associated with that sequence at that org, get the person who has been there the longest.
	select a.OrganizationID, a.OrgLevelSequence, a.JobActingFlag, MinAssignmentPositionEffectiveDate = min(AssignmentPositionEffectiveDate)
	from #Assignments a
	inner join SequencedAssignments b on a.OrganizationID = b.OrganizationID and a.OrgLevelSequence = b.MaxJobOrgLevelSequence and a.JobActingFlag = b.JobActingFlag
	group by a.OrganizationID, a.OrgLevelSequence, a.JobActingFlag
)
select 
a.OrganizationID, 
a.OrgName, 
a.OrgTypeCode,
--a.OrgLevelID,
a.OrgLevelName, 
a.OrgLevelSequence, 
JobName, 
a.JobActingFlag, 
JobPrimaryFlag = case when b.OrganizationID is not null then 'Y' else 'N' end,
EmployeeNumber, 
PersonFullName, 
AssignmentPositionEffectiveDate
--into #OrgLeaders
into #FieldAssignments
from #Assignments a
left join LongestSequencedAssignments b on a.OrganizationID = b.OrganizationID and a.OrgLevelSequence = b.OrgLevelSequence and a.JobActingFlag = b.JobActingFlag and a.AssignmentPositionEffectiveDate = b.MinAssignmentPositionEffectiveDate
order by 1, 2, 3

--Combine the Org Access dataset with the Field Assignments dataset.
select EmployeeNumber, EmployeeFullName = PersonFullName,  
JobName, JobActingFlag, JobPrimaryFlag, 
--JobOrgLevelID = OrgLevelID, 
JobOrgLevelName = OrgLevelName, JobOrgLevelSequence = OrgLevelSequence,
AssignmentPositionEffectiveDate,
a.OrganizationID, a.OrgName, a.OrgTypeCode, 
CanAccessOrganizationID, CanAccessOrgName, CanAccessOrgTypeCode,
OrgSelfFlag,
OrgSelfDescendantsFlag,
OrgLevelUpDescendantsFlag
--into #test
from #FieldAssignments a
inner join #OrgAccess b on a.OrganizationID = b.OrganizationID
--where EmployeeNumber = 517061
where EmployeeNumber is not null
order by EmployeeNumber, OrgSelfDescendantsFlag desc, OrgSelfFlag desc, CanAccessOrgTypeCode, CanAccessOrgName
*/