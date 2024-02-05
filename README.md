# Ashok-Complex-queries-V1
A collection of Complex SQL Queries
Muliple Column into row level pivot query
    
With e as (      
SELECT   ID_Material,[Values],[Type]       
        
 FROM           
 (        
  SELECT   ID_Material, ID_GroupTypeLevel1, ID_GroupTypeLevel2, ID_GroupTypeLevel3        
     ,ID_GroupTypeLevel4, ID_GroupTypeLevel5          
            
          
  FROM dbo.VMaterialGroupMap         
 ) p        
 UNPIVOT          
  (        
   [Values] FOR [Type] IN           
   (        
    ID_GroupTypeLevel1, ID_GroupTypeLevel2, ID_GroupTypeLevel3, ID_GroupTypeLevel4, ID_GroupTypeLevel5          
   )          
  )AS unpvt         
        
  )      
  select a.ID_Material,a.[Type] as GroupTypeLevelName,d.[Description] as GroupTypeLevelDesc,a.[Values] as ID_GroupValue,b.[Name] as GroupValueDesc     
  ,d.IsActive       
  from e a       
  inner Join dbo.GroupValue b with(nolock)      
 on a.[Values]=b.ID      
Inner JOin dbo.GroupEntity_GroupType c with(nolock)      
 on b.ID_GroupEntityGroupType=c.id      
Inner Join dbo.GroupTypeLevel d with(nolock)      
 On c.ID_GroupTypeLevel=d.id

-------------------------------------------------------------------------------------------------------------------
**Query to Show Which query is running**
----------------------------------------

	select * ,floor(total_elapsed_time / (1000 * 60)) % 60 as minutes
	 ,floor(total_elapsed_time / (1000 * 60 * 60)) % 24 as hours
	from (
SELECT sqltext.TEXT,
req.session_id,
req.status,
req.command,
req.cpu_time,
req.total_elapsed_time
FROM sys.dm_exec_requests req
CROSS APPLY sys.dm_exec_sql_text(sql_handle) AS sqltext
)a


-------------------------------------------------------------------------------------------------------------------
**Query Fragmantation Query**
---------------------------------


 SELECT DB_NAME(ips.database_id) AS DatabaseName,
       SCHEMA_NAME(ob.[schema_id]) SchemaNames,
       ob.[name] AS ObjectName,
       ix.[name] AS IndexName,
       ob.type_desc AS ObjectType,
       ix.type_desc AS IndexType,
       -- ips.partition_number AS PartitionNumber,
       ips.page_count AS [PageCount], -- Only Available in DETAILED Mode
       ips.record_count AS [RecordCount],
       ips.avg_fragmentation_in_percent AS AvgFragmentationInPercent
-- FROM sys.dm_db_index_physical_stats (NULL, NULL, NULL, NULL, 'DETAILED') ips
FROM sys.dm_db_index_physical_stats (NULL, NULL, NULL, NULL, 'SAMPLED') ips -- QuickResult
INNER JOIN sys.indexes ix ON ips.[object_id] = ix.[object_id] 
                AND ips.index_id = ix.index_id
INNER JOIN sys.objects ob ON ix.[object_id] = ob.[object_id]
WHERE ob.[type] IN('U','V')
AND ob.is_ms_shipped = 0
AND ix.[type] IN(1,2,3,4)
AND ix.is_disabled = 0
AND ix.is_hypothetical = 0 AND ips.index_level = 0
and object_name (ob.object_id) in ('Planningedit','Bom','Material', 'movement')

-----------------------------------------------------------
****Missing Index Details****
-------------------------------
  
    -- Missing Index Script
-- Original Author: Pinal Dave TempplannningIndex

select * 
from 
(
SELECT 
dm_mid.database_id AS DatabaseID,
dm_migs.avg_user_impact*(dm_migs.user_seeks+dm_migs.user_scans) Avg_Estimated_Impact,
dm_migs.last_user_seek AS Last_User_Seek,
OBJECT_NAME(dm_mid.OBJECT_ID,dm_mid.database_id) AS [TableName],
'CREATE INDEX [IX_' + OBJECT_NAME(dm_mid.OBJECT_ID,dm_mid.database_id) + '_'
+ REPLACE(REPLACE(REPLACE(ISNULL(dm_mid.equality_columns,''),', ','_'),'[',''),']','') 
+ CASE
WHEN dm_mid.equality_columns IS NOT NULL 
AND dm_mid.inequality_columns IS NOT NULL THEN '_'
ELSE ''
END
+ REPLACE(REPLACE(REPLACE(ISNULL(dm_mid.inequality_columns,''),', ','_'),'[',''),']','')
+ ']'
+ ' ON ' + dm_mid.statement
+ ' (' + ISNULL (dm_mid.equality_columns,'')
+ CASE WHEN dm_mid.equality_columns IS NOT NULL AND dm_mid.inequality_columns 
IS NOT NULL THEN ',' ELSE
'' END
+ ISNULL (dm_mid.inequality_columns, '')
+ ')'
+ ISNULL (' INCLUDE (' + dm_mid.included_columns + ')', '') AS Create_Statement
FROM sys.dm_db_missing_index_groups dm_mig
INNER JOIN sys.dm_db_missing_index_group_stats dm_migs
ON dm_migs.group_handle = dm_mig.index_group_handle
INNER JOIN sys.dm_db_missing_index_details dm_mid
ON dm_mig.index_handle = dm_mid.index_handle
WHERE dm_mid.database_ID = DB_ID()
)a 
where ([TableName] like '%GroupEntity%' or [TableName] like '%GroupTypeLevel%'  or [TableName] like '%GroupEntity_GroupType%'
or [TableName] like '%GroupValue%'or [TableName] like '%GroupEntity_GroupValue%'or [TableName] like '%MaterialGroupMap%')
ORDER BY Avg_Estimated_Impact DESC
GO