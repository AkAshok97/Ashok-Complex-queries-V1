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
