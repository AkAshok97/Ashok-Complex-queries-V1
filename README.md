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
****Multiple Pivot Column query ****
-------------------------------
  
    
    
CREATE     proc [dbo].[Z_Inventory_RollingForecast_GetGrid_V2] (      --FINAL          
              
   --declare           
 @Id_Plant Varchar(100)= null ,                    
 @Region Varchar(100)= null ,        
 @forecastExtension bit = 0         
 )                  
                    
 as                    
         
        
 begin                            
                          
                        
                         
drop table if exists Thirdlevelgriddata                            
drop table if exists SecondlevelGriddata                            
drop table if exists Firstlevelgriddata                            
                          
                            
drop table if exists #intweek                            
Drop table if exists #TEMPRollingReport_DIOInventoryJobData                            
Drop table if exists #Z_Inventory_RollingReport_LatestMpsRevenueData                            
drop table if exists #TempOrdercategorytype                            
drop table if exists #TempArrangingOrderingdatas                            
drop table if exists  #InputDatasFromUI                            
Drop table if exists #TempTypeCategorywiseData                            
drop table if exists #pivotformat                            
 drop table if exists #TempBPPdata                            
 drop table if exists #TempSIOPdata                            
 drop table if exists #TempForecatMPSdata                            
 drop table if exists #TempExpectationdata                            
drop table if exists  #TempFinaldataInPivot                            
drop table if exists  #bpp                            
drop table if exists  #siop                            
drop table if exists  #mps                            
drop table if exists #RevenueDatasmonthwise                            
drop table if exists #intweek_ActionPlan                            
Drop table if exists #TEMPRollingReport_Actionplan                            
drop table if exists #calendar                            
drop table if exists #Tempcalendar_ActionPlan                            
drop table if exists #TempOrdercategorytype_Action                            
drop table if exists #TempArrangingOrderingdatas_Action                            
drop table if exists  #InputDatasFromUI_Action                            
Drop table if exists #TempTypeCategorywiseData_Action                            
drop table if exists #pivotformat_ActionPlan                            
drop table if exists #Intmonthpivotformat_ActionPlan                            
drop table if exists  #TempFinaldataInPivot_ActionPlan                            
drop table if exists #TempIntmonthlevelInPivot_ActionPlan                            
                            
drop table if exists #TempintmonthforPastShipmentdays_ThiedLevelGrid                            
Drop Table if Exists #TempFirstLevelInventoryandactionPanInventory                            
Drop Table if Exists #FinalDIODataThirdlevelDIOCalc                            
Drop Table if Exists #TempFinalDIOAndInventoryvaluesInPivot                            
                            
drop table if exists  #TempThirdlevelGridFinaldataInPivot                            
drop table if exists  #bpp_Thirdlevel                            
drop table if exists  #siop_Thirdlevel                            
drop table if exists  #mps_Thirdlevel                            
drop table if exists #Expectation_Thirdlevel                            
drop table if exists #RevenueDatasmonthwise_Thirdlevel                            
drop table if exists #intmonthforalignment                            
                            
 drop table if exists #TargetusereditedBBPDIOvalue                            
 drop table if exists #FIlearrivalBBPDIOvalue                            
 drop table if exists #TargetusereditedBBPInventoryvalue                            
drop table if exists #FIlearrivalBBPInventoryvalue                            
drop table if exists #TempintmonthforPastShipmentSummaryQuantity                              
  drop table if exists #TempForecatMPSdata_PivotThirdlevel                          
    Drop table if exists #PlantFilterforCommaseperated               
         
/*****forecast extension********/        
        
Declare @forcastMonth int = 3;        
        
If @forecastExtension =1         
select @forcastMonth = value from properties where key1='Rolling_Forecast_Intmonth_Extension'        
else        
select @forcastMonth = value from properties where key1='Rolling Forecast Intmonth'        
        
/*****forecast extension********/        
                         
declare @sqlforplantmerge varchar(max)                        
                        
Drop table if exists #PlantFilterforCommaseperated                        
create table #PlantFilterforCommaseperated                        
(                        
plvalue varchar(100)                        
)                        
                        
select @sqlforplantmerge='                        
insert into #PlantFilterforCommaseperated                        
SELECT distinct cast(pr.value as varchar) as plvalue                        
FROM PROPERTIES pr                        
Inner join Plant  p                        
 ON pr.value=p.id                        
 where pr.Key1 = ''ID_Plant_Merged''                         
'+ case when @id_plant  is not null then ' and cast(pr.ID_plant as varchar) in ('+@id_plant+') ' else '' end +'                            
'+ case when @Region is not null then '  AND p.region in ('+@Region +')' else '' end +'                            
                        
 'print(@sqlforplantmerge)                        
 exec(@sqlforplantmerge)                        
                        
 Set @id_plant=(select distinct STUFF((SELECT distinct ' ,' + CAST(plvalue AS VARCHAR(MAX))                         
                 FROM #PlantFilterforCommaseperated                        
FOR XML PATH(''),TYPE ).value('.','VARCHAR(MAX)') ,1,2,'') as Cust_Material)                        
                        
---select @id_plant                        
                        
                        
                        
                              
   drop table if exists Thirdlevelgriddata                            
  drop table if exists SecondlevelGriddata                            
  drop table if exists Firstlevelgriddata                            
Declare @getdate date=getdate()                            
                            
declare @Currintweek int                            
declare @Currintmonth varchar(50)                            
                            
 Set @Currintweek=(select top 1 intweek from calendar where date=@getdate)                            
 set @Currintmonth=(select top 1 intmonth from calendar where date=@getdate)                            
                            
Declare @StartMonth varchar(100)                            
Declare @EndMonth varchar(100)                            
                            
                            
 select @StartMonth=min(intmonth), @EndMonth=max(intmonth)                            
 from                             
 (                            
 select row_number()over(order by intmonth) Rn,intmonth                            
 from (                            
 select distinct intmonth from calendar where intmonth>=@Currintmonth                            
  )a                            
 )V                            
 Where v.rn<=  @forcastMonth             
                       
                            
  drop table if exists #intmonthforalignment                            
                            
 create table  #intmonthforalignment                            
 (                            
 intmonth int                            
 )                            
 insert  into #intmonthforalignment                            
 select intmonth             
 from                             
 (                            
 select row_number()over(order by intmonth) Rn,intmonth                            
 from (                            
 select distinct intmonth from calendar where intmonth>=@Currintmonth                            
  )a                            
 )V                            
 Where v.rn<= @forcastMonth                     
         
         
         
drop table if exists #intweek                            
                             
 select distinct c.intweek ,c.intmonth                            
  into #intweek                            
  from Calendar c                             
  where c.intMonth between @StartMonth and @endmonth                            
                             
  select Intmonth,case when intmonth=@Currintmonth then 0 when intmonth>@Currintmonth then 1 when intmonth<@Currintmonth then -1 end as MonthStatus                            
 ,Intweek,case when Intweek=@Currintweek then 0 when Intweek>@Currintweek then 1 when Intweek<@Currintweek then -1 end as WeekStatus                            
  from #intweek                            
  order by intweek,intmonth                            
                            
 DECLARE @intWeek_CurrentMonth AS VARCHAR(MAX)                                
                            
 select @intWeek_CurrentMonth =STUFF((SELECT ',' + QUOTENAME(intweek)                                
    from (select distinct intweek  from  #intweek where intmonth=@Currintmonth  )a                            
  group by intweek                                
        order by intweek                                
        FOR XML PATH(''), TYPE                    
            ).value('.', 'NVARCHAR(MAX)')                                 
        ,1,1,'')                                
                            
                                          
 DECLARE @intmonthcols_BBP AS VARCHAR(MAX)                                  
    DECLARE @intmonthcols_SIOP AS VARCHAR(MAX)                                
 DECLARE @intmonthcols_Expectation AS VARCHAR(MAX)                   Declare @intmonthcols_MpsRevenue AS VARCHAR(MAX)                                
                            
 select @intmonthcols_BBP =STUFF((SELECT ',' + QUOTENAME(intmonth)                                
    from (select distinct cast(intmonth as Varchar)+'_BBP' as intmonth from  #intweek   )a                            
  group by intmonth                                
        order by intmonth                                
        FOR XML PATH(''), TYPE                                
            ).value('.', 'NVARCHAR(MAX)')                                 
        ,1,1,'')                                
                            
                            
 select @intmonthcols_SIOP =STUFF((SELECT ',' + QUOTENAME(intmonth)                                
    from (select distinct cast(intmonth as Varchar)+'_SIOP' as intmonth from  #intweek   )a                            
  group by intmonth                                
        order by intmonth                                
        FOR XML PATH(''), TYPE                                
            ).value('.', 'NVARCHAR(MAX)')                                 
        ,1,1,'')                                
                            
 select @intmonthcols_Expectation =STUFF((SELECT ',' + QUOTENAME(intmonth)                                
    from (select distinct cast(intmonth as Varchar)+'_EXPECTATION' as intmonth from  #intweek  where intmonth=@Currintmonth )a                            
  group by intmonth                                
        order by intmonth                                
        FOR XML PATH(''), TYPE                                
            ).value('.', 'NVARCHAR(MAX)')                                 
      ,1,1,'')                                
                            
   select @intmonthcols_MpsRevenue =STUFF((SELECT ',' + QUOTENAME(intmonth)                        
    from (select distinct cast(intmonth as Varchar)+'_ForecastInventory' as intmonth from  #intweek where intmonth>@Currintmonth  )a                            
  group by intmonth                                
        order by intmonth                                
        FOR XML PATH(''), TYPE                                
            ).value('.', 'NVARCHAR(MAX)')                                 
        ,1,1,'')                       
                            
          
         
/*************DIO Denominator Calc ****************/        
        
        
          
 declare @year int = year(getdate()) ,          
@current_month int,          
@rangeStart date = (select min(date) from calendar where cast(date as date) = DATEFROMPARTS(Year(dateadd(mm, -2, getdate())), month (dateadd(mm, -2, getdate())) , 1) ),    
-- = DATEFROMPARTS(Year(getdate()), month (getdate()) - 2 , 1),        
@rangeEnd date = DATEADD(month, 12, eomonth(DATEFROMPARTS(Year(getdate()), month (getdate())  , 1))) ,        
@firstMonth int, @lastMonth int, @weekversion varchar(10);        
          
 select @current_month = intmonth from Calendar where date = cast(getdate() as date);        
 Declare @tb_Month_dtl table         
 (rno int,        
 id_plant int,         
 intmonth  int,        
 noDays int,        
 monthPosition varchar(10))        
        
insert into @tb_Month_dtl         
SELECT  ROW_NUMBER() OVER (        
 ORDER BY id_plant, intmonth        
   ) rno, id_plant, intmonth , count(date) as NoDays  ,        
   case when intmonth < @current_month then 'Past' when intmonth = @current_month then 'Current' when intmonth  = @current_month + 1 then 'NextToCur' when intmonth > @current_month + 1 then 'Future' end as MonthPosition        
FROM Calendar        
WHERE date between @rangeStart and @rangeEnd  and id_plant = 1        
group by intmonth  ,ID_Plant        
        
select @firstMonth = min(intmonth) ,  @lastMonth = max (intmonth) from @tb_Month_dtl          
        
drop table if exists #tblecal        
select intmonth,noDays,monthPosition into #tblecal from  @tb_Month_dtl        
           
/****** End Revenue Region Other Data**************/        
        
drop table if exists #temp_dio_raw_data        
select intmonth, R0,R1,R2, D0,D1,D2, ((R0+R1+R2) / (D0+D1+D2)) as dio_denominator into #temp_dio_raw_data        
from (        
select  intmonth , noDays,monthPosition,         
                  
  Revenue as R0,        
  LAG(Revenue,1) OVER (        
  ORDER BY   intmonth ) as R1,        
  LAG(Revenue,2) OVER (        
  ORDER BY    intmonth ) as R2,        
  noDays as D0,        
  LAG(noDays,1) OVER (        
  ORDER BY    intmonth ) as D1,        
  LAG(noDays,2) OVER (        
  ORDER BY   intmonth ) as D2        
from         
(        
        
select  a.intmonth, a.noDays , a.monthPosition ,Sum(isnull(b.value,0) *1000000 )  as Revenue from  #tblecal a          
left join       
(select r.*, s.region  from Z_Inventory_RollingReport_LatestSIOPRevenueData r left join plant s on r.id_plant = s.id) b      
on a.intmonth = b.Intmonth       
where a.intmonth   between   @firstMonth and @lastMonth       
and        
(@Id_Plant is null or  (b.id_plant)   in (SELECT [value] FROM STRING_SPLIT( replace(@Id_Plant,'''',''), ',')) )      
 and      
(@Region is null or  (b.region)   in (SELECT [value] FROM STRING_SPLIT( replace(@Region,'''',''), ',')) )      
group by  a.intmonth  ,a.noDays , a.monthPosition          
      
      
) as t1        
) as t2        
        
         
        
    /*************End of DIO Denominator Calc ****************/            
        
 drop table if exists #TempintmonthforPastShipmentdays_ThiedLevelGrid                            
create table #TempintmonthforPastShipmentdays_ThiedLevelGrid                            
(                            
id int,                            
intmonth int                            
)                            
insert  into #TempintmonthforPastShipmentdays_ThiedLevelGrid                            
select *  from (                            
select row_number()over(order by intmonth desc ) rn ,* from (                            
select distinct intmonth from calendar where intmonth<=@Currintmonth                            
)a                            
)b where rn<=(select value from properties where key1='Past3monthShipementandCalendarForRollingReport')                            
                            
drop table if exists #TempintmonthforPastShipmentSummaryQuantity          
                            
           
select *  into #TempintmonthforPastShipmentSummaryQuantity from (                            
select row_number()over(order by intmonth desc ) rn ,* from (                            
select distinct intmonth from calendar where intmonth<@Currintmonth                            
)a                            
)b where rn<=(select value-1 from properties where key1='Past3monthShipementandCalendarForRollingReport')                            
                                              
--select @intWeek_CurrentMonth,@intmonthcols_BBP,@intmonthcols_SIOP,@intmonthcols_Expectation,@intmonthcols_MpsRevenue                            
            
        
   drop table if exists #TempIntmonthMaxIntweekgrid                        
                      
 Select intmonth,max(intweek)intweek                        
 Into #TempIntmonthMaxIntweekgrid                        
 from Calendar Where intmonth=@Currintmonth                      
 group by intmonth                        
 Order by intmonth                        
                          
                         
Drop table if exists #TEMPRollingReport_DIOInventoryJobData                            
                            
Drop table if exists #TEMPRollingReport_DIOInventoryJobData_MonthWise                            
Drop table if exists #Z_Inventory_RollingReport_LatestMpsRevenueData                            
Create table #TEMPRollingReport_DIOInventoryJobData                            
(                            
Type varchar(100),                            
CategoryType varchar(150),                            
SubCategoryType varchar(200),                            
intweek int,                            
value float                            
)                            
                            
Create table #TEMPRollingReport_DIOInventoryJobData_MonthWise                            
(                            
Type varchar(100),                            
CategoryType varchar(150),                            
SubCategoryType varchar(200),                            
Intmonth int,                            
value float                            
)                            
                            
create table #Z_Inventory_RollingReport_LatestMpsRevenueData                            
(                            
Type varchar(100),                            
intweek int,                            
value float,                            
id int,                            
Parentid int                            
)                            
                            
                            
 Declare @Query nvarchar(max)                            
                            
 Select @Query='                            
                             
 insert into #TEMPRollingReport_DIOInventoryJobData                            
select Type,CategoryType,SubCategoryType,intweek,sum(value)as value                             
from [Z_Inventory_RollingReport_DIOInventoryJobData] RR                            
Inner Join Plant p                            
 On RR.id_plant=p.id       
inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
where RR.intmonth in ('+@Currintmonth+')                                                  
Group by Type,CategoryType,SubCategoryType,intweek                            
                            
select max(intweek) lastweek,intmonth                             
into #lastweekForFutureMonths   
from Z_Inventory_RollingReport_DIOInventoryJobData                            
group by intMonth                            
                            
 insert into #TEMPRollingReport_DIOInventoryJobData_MonthWise                            
select rr.Type,rr.CategoryType,rr.SubCategoryType,rr.intmonth,sum(rr.value)as value                             
from [Z_Inventory_RollingReport_DIOInventoryJobData] RR                            
inner join #lastweekForFutureMonths lw                            
on lw.intmonth = rr.intmonth and rr.intweek = lw.lastweek                            
Inner Join Plant p                            
 On RR.id_plant=p.id       
inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
where RR.intmonth>'+@Currintmonth+'                                              
Group by rr.Type,rr.CategoryType,rr.SubCategoryType,rr.intmonth                            
                            
                            
insert into #Z_Inventory_RollingReport_LatestMpsRevenueData                            
 Select ''Revenue'' as  Type,intweek,sum(value) as value,1 as id ,'''' as Parentid                            
 from Z_Inventory_RollingReport_LatestMpsRevenueData RR                            
 Inner Join Plant p                            
  On RR.id_plant=p.id       
 inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
    
  where intmonth  in ('+@Currintmonth+')                            
                         
  group by intweek                            
                            
 '                            
 Print(@Query)                            
 Exec(@Query)                            
       declare @plantcount int = 1    
    select @plantcount=count(1) from #PlantFilterforCommaseperated    
    
if (@Region is not null)  or (@plantcount > 1)                          
begin                            
                             
 select  @Query = '                            
 Delete from  #TEMPRollingReport_DIOInventoryJobData where Type=''DIO''                            
 Delete from  #TEMPRollingReport_DIOInventoryJobData_MonthWise  where Type=''DIO''                            
                            
                             
 Declare @getdate date=getdate()                            
 Declare @Currintmonth int                            
 set @Currintmonth=(select top 1 intmonth from calendar where date=@getdate and id_plant in ('+@id_plant+'))                            
                            
 Declare @past3monthshipmentdata decimal(20,5)                            
 Declare @past3monthcalendardays int                            
                            
 Declare @Past3monthdividedvalue  decimal(20,5)                            
                            
                             
 Declare @CurrMonthrevenuevalue decimal(20,5)                            
 Set @CurrMonthrevenuevalue =(Select Sum(value)*1000000                             
 from Z_Inventory_RollingReport_LatestSIOPRevenueData where id_plant in ('+@id_plant+')  and intmonth=@Currintmonth)                            
                            
select @past3monthshipmentdata=Quantity from (                            
Select  convert(decimal(20,5),SUM(ShipmentValue)) as Quantity                             
from Z_Inventory_ShipmentSummaryQuantity s                            
Inner Join Plant p                            
 On s.id_plant=p.id      
inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
where intmonth in (select intmonth from #TempintmonthforPastShipmentSummaryQuantity)                                                   
)a                            
                             
 select @past3monthcalendardays=count(*) from calendar                               
 where intmonth in (select intmonth from #TempintmonthforPastShipmentdays_ThiedLevelGrid)                            
 and id_plant=1 --for default dayss                            
--select @past3monthshipmentdata,@past3monthcalendardays                            
select  @Past3monthdividedvalue =(@past3monthshipmentdata+isnull(@CurrMonthrevenuevalue,0))/@past3monthcalendardays                            
                            
--select @Past3monthdividedvalue                             
set  @Past3monthdividedvalue= @Past3monthdividedvalue /1000000                         
                         
 insert into #TEMPRollingReport_DIOInventoryJobData                            
select distinct ''DIO'',a.CategoryType,a.subCategoryType,a.intweek,b.DIOValue                       
from #TEMPRollingReport_DIOInventoryJobData a                      
Inner Join (                      
   select distinct fv.CategoryType,fv.subCategoryType,fv.intweek,fv.value/nullif(dv.dio_denominator/1000000,0) as DIOValue                        
   from #TEMPRollingReport_DIOInventoryJobData fv                        
   --Inner join #TempIntmonthMaxIntweekgrid ti               
   -- on fv.intweek=ti.intweek       
inner join #temp_dio_raw_data dv        
  on dv.intmonth = '+@Currintmonth +'     
    Where fv.Type=''Inventory''                      
  )b                      
 On a.CategoryType=b.CategoryType                      
 and a.subCategoryType=b.subCategoryType       
 and a.intweek = b.intweek    
 Where a.Type=''Inventory''                      
 order by a.CategoryType,a.subCategoryType,a.intweek                      
                               
                            
insert into #TEMPRollingReport_DIOInventoryJobData_MonthWise                            
Select ''DIO'',Categorytype,SubCategorytype,dv.intmonth,value/nullif(dv.dio_denominator/1000000,0) as value from #TEMPRollingReport_DIOInventoryJobData_MonthWise fv      
inner join #temp_dio_raw_data dv        
  on dv.intmonth = fv.intmonth          
                            
 '                            
 print(@Query)                            
 EXEC(@Query)                            
                            
                            
END                
     
 drop table if exists #TempOrdercategorytype                            
                            
 select ic.id as id_category,ic.Type as categorytype,ifc.Description as Subcategorytype                            
 into #TempOrdercategorytype                   from Z_Inventory_ForecastMapping map                            
 inner join Z_Inventory_Category ic                            
  on ic.id = map.ID_InventoryCategory                            
 inner join Z_Inventory_ForecastCategory ifc                            
  on ifc.id = map.ID_InventoryForecastCategory                            
                 
  -- delete from #TempOrdercategorytype where Subcategorytype = 'Excess'              
  --Update #TempOrdercategorytype set Subcategorytype = 'Regular' where Subcategorytype = 'Running Stock'              
              
                
  -- delete from Z_Inventory_ForecastCategory where Description = 'Excess'              
  --Update Z_Inventory_ForecastCategory set Description = 'Regular' where Description = 'Running Stock'              
              
  --delete from Z_Inventory_ForecastMapping where ID_InventoryForecastCategory = 2              
                            
drop table if exists #TempArrangingOrderingdatas                            
create table #TempArrangingOrderingdatas(                            
Type Varchar(100),                            
categorytype varchar(100),                            
Subcategorytype varchar(100),                            
intweek int,                            
value float                            
                            
)                            
                            
                            
                            
drop table if exists  #InputDatasFromUI                            
create table  #InputDatasFromUI(                            
Type Varchar(100),                            
categorytype varchar(100),                            
Subcategorytype varchar(100),                            
intweek int,                            
value float                            
                            
)                            
                            
insert into #TempArrangingOrderingdatas                            
Select distinct a.type,a.categorytype,a.Subcategorytype,a.intweek,a.value                            
from (                            
  Select distinct c.*,i.*,0 as value,r.*                            
  from #TempOrdercategorytype c                            
   cross join (select distinct intweek from  #intweek where intmonth=@Currintmonth) i                            
   Cross join Z_Inventory_TypeForRollingForecast R                            
  )a                            
 left join                             
  (                            
  Select * from #TEMPRollingReport_DIOInventoryJobData                            
                              
  )b                            
                               
   ON a.categorytype collate database_default=b.categorytype collate database_default                            
   and a.subcategorytype collate database_default=b.subcategorytype collate database_default                            
   and a.Type collate database_default=b.Type collate database_default                            
   and a.intweek=b.intweek                            
                               
   where b.categorytype is  null                            
                            
--insert into #InputDatasFromUI                            
-- select case when it.id=2 then 'Inventory' else it.Description end as Description,ic.Type as categorytype,ifc.Description as Subcategorytype,ifs.intweek,ifs.value                            
-- from InventoryForecastsnapshot ifs                            
-- inner join Z_Inventory_Category ic                            
--  on ic.id = ifs.ID_InventoryCategory                            
-- inner join Z_Inventory_ForecastCategory ifc                            
--  on ifc.id = ifs.ID_ForecastCategory                            
-- Inner join Z_Inventory_TargetType it                            
--  on ifs.id_targettype=it.id                            
                            
                            
                            
Drop table if exists #TempTypeCategorywiseData                            
Select  Type,CategoryType,SubCategoryType,intweek,sum(value)as value                             
into #TempTypeCategorywiseData                            
from (                            
  Select * from #TEMPRollingReport_DIOInventoryJobData                            
  union all                            
  select * from #TempArrangingOrderingdatas                            
  --union all                            
  --select * from #InputDatasFromUI                            
  )a                            
  group by  Type,CategoryType,SubCategoryType,intweek                            
                            
                            
                            
                            
drop table if exists #pivotformat                            
                            
create table #pivotformat                      
(                            
                            
Description varchar(250),                            
intweek int,                            
value decimal(35,2),                            
id int,                            
Parentid int,                            
Type varchar(100)                            
)                    
                
                
drop table if exists #TempTypeCategorywiseData_FirstlevelGrid                
                 
select * into #TempTypeCategorywiseData_FirstlevelGrid from #TempTypeCategorywiseData                 
where SubCategoryType collate database_default not in (select description collate database_default from Z_Inventory_ForecastCategory where ismanualinput = 1 )                 
                
/*      
insert into #pivotformat                            
------------MPS version Revenue---------------                            
                            
select *,case when id between 1 and 23 then 'DIO'  else 'Total Inventory Value' end as Type                            
from (                            
select * from #Z_Inventory_RollingReport_LatestMpsRevenueData                            
union all                            
 ----------DIO and Rm-------------                            
  select  type,intweek,sum(value) as value,2 as id,'' as Parentid                            
  from #TempTypeCategorywiseData                            
   where type='DIO' and subcategorytype<>'EO Accural'                            
   group by type,intweek                            
union all                            
  select  categorytype,intweek,sum(value) as value,3 as id,2 as Parentid                            
  from #TempTypeCategorywiseData                            
   where type='DIO' and categorytype='RM' and subcategorytype<>'EO Accural'                            
   group by categorytype,intweek                            
union all                              
  select  subcategorytype,intweek,sum(value) as value,case when subcategorytype='Regular' then 4                             
   when subcategorytype='Excess' then 5    when subcategorytype='NPI' then 6                
    when subcategorytype='LTB' then 7  when subcategorytype='Special Agreement' then 8  when subcategorytype='Intercompany in-transit' then 9                             
   when subcategorytype='In-Transit from Supplier' then 10                             
when subcategorytype='Others(Fx impact)' then 11    --- when subcategorytype='EO Accural' then 12                        
  end as id                            
                              
  ,3 as Parentid                            
  from #TempTypeCategorywiseData                            
   where type='DIO' and categorytype='RM'    and subcategorytype<>'EO Accural'                        
   group by subcategorytype,intweek                            
union all                            
 ----------DIO and WIP---                            
    select  categorytype,intweek,sum(value) as value,12 as id                            
                                
    ,2 as Parentid                         
  from #TempTypeCategorywiseData                            
   where type='DIO' and categorytype='WIP' and subcategorytype<>'EO Accural'                            
   group by categorytype,intweek                            
union all                              
  select  subcategorytype,intweek,sum(value) as value,case when subcategorytype='Regular' then 13                             
   when subcategorytype='Excess' then 14    when subcategorytype='NPI' then 15                             
      when subcategorytype='Others(Fx impact)' then 16  ---  when subcategorytype='EO Accural' then 18                        
  end as id                            
                              
  ,12 as Parentid                            
  from #TempTypeCategorywiseData                            
   where type='DIO' and categorytype='WIP'    and subcategorytype<>'EO Accural'                          
   group by subcategorytype,intweek                            
union all                            
 ----------------DIO and FG-----------                            
                            
    select  categorytype,intweek,sum(value) as value,17 as id,2 as Parentid                            
  from #TempTypeCategorywiseData                            
   where type='DIO' and categorytype='FG' and subcategorytype<>'EO Accural'                            
   group by categorytype,intweek                            
 union all                             
  select  subcategorytype,intweek,sum(value) as value,case when subcategorytype='Regular' then 18                             
   when subcategorytype='Excess' then 19  when subcategorytype='NPI' then 20                             
    when subcategorytype='Special Agreement' then 21                             
     when subcategorytype='In-Transit to Customer' then 22                             
      when subcategorytype='Others(Fx impact)' then 23    --when subcategorytype='EO Accural' then 26                          
   end as id                            
                              
  ,17 as Parentid                            
  from #TempTypeCategorywiseData                            
   where type='DIO' and categorytype='FG'      and subcategorytype<>'EO Accural'                        
   group by subcategorytype,intweek                            
                            
union all                            
--------------Inventory and Rm-----------                            
                            
  select  'Total Inventory Value'type,intweek,sum(value) as value,24 as id,'' as parentid                            
  from #TempTypeCategorywiseData                            
   where type='Inventory' and subcategorytype<>'EO Accural'                            
   group by type,intweek                            
union all                            
  select  categorytype,intweek,sum(value) as value,25 as id                            
  ,24 as parentid                            
  from #TempTypeCategorywiseData                            
   where type='Inventory' and categorytype='RM' and subcategorytype<>'EO Accural'             
   group by categorytype,intweek                            
 union all                             
  select  subcategorytype,intweek,sum(value) as value,case when subcategorytype='Regular' then 26                             
   when subcategorytype='Excess' then 27  when subcategorytype='NPI' then 28                             
    when subcategorytype='LTB' then 29  when subcategorytype='Special Agreement' then 30  when subcategorytype='Intercompany in-transit' then 31                             
   when subcategorytype='In-Transit from Supplier' then 32                             
      when subcategorytype='Others(Fx impact)' then 33   when subcategorytype='EO Accural' then 34 end as id                            
                              
                              
  ,25 as parentid                            
  from #TempTypeCategorywiseData                            
   where type='Inventory' and categorytype='RM'                            
   group by subcategorytype,intweek                            
union all                            
------------------Inventory and WIP---                            
  select  categorytype,intweek,sum(value) as value,35 as id                            
                              
  ,24 as parentid                            
  from #TempTypeCategorywiseData                            
   where type='Inventory' and categorytype='WIP' and subcategorytype<>'EO Accural'                            
   group by categorytype,intweek                            
union all                              
  select  subcategorytype,intweek,sum(value) as value,case when subcategorytype='Regular' then 36             
   when subcategorytype='Excess' then 37   when subcategorytype='NPI' then 38                             
     when subcategorytype='Others(Fx impact)' then 39 when subcategorytype='EO Accural' then 40                          
  end as id                            
                              
                              
  ,35 as parentid                            
  from #TempTypeCategorywiseData                            
   where type='Inventory' and categorytype='WIP'                            
   group by subcategorytype,intweek                            
union all ------------------Inventory and FG---                            
  select  categorytype,intweek,sum(value) as value,41 as id,24 as parentid                            
  from #TempTypeCategorywiseData                            
   where type='Inventory' and categorytype='FG' and subcategorytype<>'EO Accural'                            
   group by categorytype,intweek                            
union all                              
  select  subcategorytype,intweek,sum(value) as value,case when subcategorytype='Regular' then 42                         
 when subcategorytype='Excess' then 43    when subcategorytype='NPI' then 44                             
    when subcategorytype='Special Agreement' then 45                             
     when subcategorytype='In-Transit to Customer' then 46                             
      when subcategorytype='Others(Fx impact)' then 47   when subcategorytype='EO Accural' then 48                          
   end as id                            
  ,41 as parentid                            
  from #TempTypeCategorywiseData                            
   where type='Inventory' and categorytype='FG'                            
   group by subcategorytype,intweek                            
                            
   )a                            
   --where id is null     */                    
                   
                
                             
insert into #pivotformat                            
------------MPS version Revenue---------------                            
                            
select *,case when id between 1 and 14 then 'DIO'  else 'Total Inventory Value' end as Type                            
from (                            
select * from #Z_Inventory_RollingReport_LatestMpsRevenueData                            
union all                            
 ----------DIO and Rm-------------                            
  select  type,intweek,sum(value) as value,2 as id,'' as Parentid                            
  from #TempTypeCategorywiseData_FirstlevelGrid                            
   where type='DIO' and subcategorytype<>'EO Accural'                            
   group by type,intweek                            
union all                            
  select  categorytype,intweek,sum(value) as value,3 as id,2 as Parentid                            
  from #TempTypeCategorywiseData_FirstlevelGrid                        
   where type='DIO' and categorytype='RM' and subcategorytype<>'EO Accural'                            
   group by categorytype,intweek                            
union all                              
  select  subcategorytype,intweek,sum(value) as value,case when subcategorytype='Regular' then 4                             
  -- when subcategorytype='Excess' then 5                  
   when subcategorytype='NPI' then 5                             
    when subcategorytype='LTB' then 6  when subcategorytype='Special Agreement' then 7                 
 /*when subcategorytype='Intercompany in-transit' then 9                             
    when subcategorytype='In-Transit from Supplier' then 10                             
    when subcategorytype='Others(Fx impact)' then 11                    
 --- when subcategorytype='EO Accural' then 12   */                     
  end as id                            
                              
  ,3 as Parentid                            
  from #TempTypeCategorywiseData_FirstlevelGrid                            
   where type='DIO' and categorytype='RM'    and subcategorytype<>'EO Accural'                        
   group by subcategorytype,intweek                            
union all                            
 ----------DIO and WIP---                            
    select  categorytype,intweek,sum(value) as value,8 as id                            
                                
    ,2 as Parentid                            
  from #TempTypeCategorywiseData_FirstlevelGrid                            
   where type='DIO' and categorytype='WIP' and subcategorytype<>'EO Accural'                            
   group by categorytype,intweek                            
union all                              
  select  subcategorytype,intweek,sum(value) as value,case when subcategorytype='Regular' then 9                             
  -- when subcategorytype='Excess' then 11              
   when subcategorytype='NPI' then 10                             
     /* when subcategorytype='Others(Fx impact)' then 16  */                
   ---  when subcategorytype='EO Accural' then 18                        
  end as id                            
                              
  ,8 as Parentid                            
  from #TempTypeCategorywiseData_FirstlevelGrid                            
   where type='DIO' and categorytype='WIP'    and subcategorytype<>'EO Accural'                          
   group by subcategorytype,intweek                            
union all                            
 ----------------DIO and FG-----------                            
                            
    select  categorytype,intweek,sum(value) as value,11 as id,2 as Parentid                            
  from #TempTypeCategorywiseData_FirstlevelGrid                            
   where type='DIO' and categorytype='FG' and subcategorytype<>'EO Accural'                            
   group by categorytype,intweek                            
 union all                             
  select  subcategorytype,intweek,sum(value) as value,case when subcategorytype='Regular' then 12                      
   --when subcategorytype='Excess' then 15            
   when subcategorytype='NPI' then 13                             
    when subcategorytype='Special Agreement' then 14                             
    /* when subcategorytype='In-Transit to Customer' then 22                             
      when subcategorytype='Others(Fx impact)' then 23 */   --when subcategorytype='EO Accural' then 26                          
   end as id                         
                              
  ,11 as Parentid                            
  from #TempTypeCategorywiseData_FirstlevelGrid                            
   where type='DIO' and categorytype='FG'      and subcategorytype<>'EO Accural'                        
   group by subcategorytype,intweek                            
                            
union all                            
--------------Inventory and Rm-----------                            
                            
  select  'Total Inventory Value'type,intweek,sum(value) as value,15 as id,'' as parentid                            
  from #TempTypeCategorywiseData_FirstlevelGrid                            
   where type='Inventory' and subcategorytype<>'EO Accural'                            
   group by type,intweek                            
union all                            
  select  categorytype,intweek,sum(value) as value,16 as id                            
  ,15 as parentid                            
  from #TempTypeCategorywiseData_FirstlevelGrid                            
   where type='Inventory' and categorytype='RM' and subcategorytype<>'EO Accural'                            
   group by categorytype,intweek                            
 union all                      
  select  subcategorytype,intweek,sum(value) as value,case when subcategorytype='Regular' then 17                             
   --when subcategorytype='Excess' then 21            
   when subcategorytype='NPI' then 18                             
    when subcategorytype='LTB' then 19  when subcategorytype='Special Agreement' then 20                
 /*when subcategorytype='Intercompany in-transit' then 31                             
    when subcategorytype='In-Transit from Supplier' then 32                             
      when subcategorytype='Others(Fx impact)' then 33 */                
   when subcategorytype='EO Accural' then 21 end as id                            
                              
                              
  ,16 as parentid                            
  from #TempTypeCategorywiseData_FirstlevelGrid                            
   where type='Inventory' and categorytype='RM'                            
   group by subcategorytype,intweek                  
union all                            
------------------Inventory and WIP---                            
  select  categorytype,intweek,sum(value) as value,22 as id                            
                              
  ,15 as parentid               
  from #TempTypeCategorywiseData_FirstlevelGrid                            
   where type='Inventory' and categorytype='WIP' and subcategorytype<>'EO Accural'                            
   group by categorytype,intweek                            
union all                              
  select  subcategorytype,intweek,sum(value) as value,case when subcategorytype='Regular' then 23                         
   --when subcategorytype='Excess' then 28             
   when subcategorytype='NPI' then 24                             
     /*when subcategorytype='Others(Fx impact)' then 39*/                
  when subcategorytype='EO Accural' then 25                          
  end as id                            
                              
                              
  ,22 as parentid                            
  from #TempTypeCategorywiseData_FirstlevelGrid                            
   where type='Inventory' and categorytype='WIP'                            
   group by subcategorytype,intweek                            
union all ------------------Inventory and FG---                            
  select  categorytype,intweek,sum(value) as value,26 as id,15 as parentid                            
  from #TempTypeCategorywiseData_FirstlevelGrid                            
   where type='Inventory' and categorytype='FG' and subcategorytype<>'EO Accural'                            
   group by categorytype,intweek                            
union all                              
  select  subcategorytype,intweek,sum(value) as value,case when subcategorytype='Regular' then 27              
 --when subcategorytype='Excess' then 33             
 when subcategorytype='NPI' then 28                             
 when subcategorytype='Special Agreement' then 29                             
     /* when subcategorytype='In-Transit to Customer' then 36                             
      when subcategorytype='Others(Fx impact)' then 37  */                 
   when subcategorytype='EO Accural' then 30                          
   end as id                            
  ,26 as parentid                            
  from #TempTypeCategorywiseData_FirstlevelGrid                            
   where type='Inventory' and categorytype='FG'                            
   group by subcategorytype,intweek                            
                            
   )a                            
   --where id is null                   
                
         
        
          
----------------------------------------------------REVENUE------------------------------------------------                            
                            
                            
 drop table if exists #TempBPPdata                            
 drop table if exists #TempSIOPdata                            
 drop table if exists #TempForecatMPSdata                            
 drop table if exists #TempExpectationdata                            
create table #TempBPPdata                            
(                            
descr  varchar(100),                            
Type varchar(100),            
Intmonth varchar(100),                            
value decimal(35,2)                            
                            
)                            
                            
create table #TempSIOPdata                            
(                            
descr  varchar(100),                            
Type varchar(100),                            
Intmonth varchar(100),                            
value decimal(35,2)                            
                            
)                            
                            
create table #TempForecatMPSdata                            
(                            
descr  varchar(100),                            
Type varchar(100),                          
Categorytype varchar(100) ,                          
Intmonth varchar(100),                            
value decimal(35,2)                            
                            
)                            
create table #TempExpectationdata                            
(                            
descr  varchar(100),                            
Type varchar(100),                            
Intmonth varchar(100),                            
value decimal(35,2)                            
                            
)                            
                            
 Select @Query='                            
Declare @Activeversion int=1                             
                            
                            
                            
 Drop table if exists #TempCheckingExpectationdatausingBPPdata                            
                            
 Select  intmonth,sum(value) as value into #TempCheckingExpectationdatausingBPPdata                            
 from Z_Inventory_RollingReport_LatestBPPRevenueData it                            
 inner join plant p                            
  on p.id=it.id_plant                            
 inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
                             
 where  intmonth between '+@StartMonth+' and '+@endmonth+'                            
                       
 group by intmonth                            
                            
                             
insert into #TempExpectationdata                            
 select ''DIO'' as Descr,''Revenue'' as type,cast(b.intmonth as varchar)+''_EXPECTATION'' as intmonth,isnull(e.ExpValue,b.value) As Value                            
 from #TempCheckingExpectationdatausingBPPdata b                            
 left join                              
 (                            
 select it.intmonth,sum(value)As ExpValue from Z_Inventory_RollingReport_LatestExpectationRevenueData it          
 inner join plant p                            
  on p.id=it.id_plant         
 inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
 where  intmonth between '+@StartMonth+' and '+@endmonth+'                            
                       
 group by it.intmonth                            
 )e                            
 on b.intmonth=e.intmonth                            
                            
 insert into #TempBPPdata                            
 Select  ''DIO'' as descr,''Revenue'' as type,cast(intmonth as varchar)+''_BBP'' as intmonth,sum(value) as value                             
 from Z_Inventory_RollingReport_LatestBPPData  it                            
 inner join plant p                            
  on p.id=it.id_plant                        
 inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
                       
 where it.id_inventorytargettype in (select id from Z_Inventory_TargetType where description = ''Revenue'') and intmonth between '+@StartMonth+' and '+@endmonth+'                                            
 group by intmonth                            
                            
                            
 insert into #TempSIOPdata                            
 Select  ''DIO'' as descr,''Revenue'' as type,cast(intmonth as varchar)+''_SIOP'' as intmonth,sum(value) as value                             
 from Z_Inventory_RollingReport_LatestSIOPRevenueData  it                            
 inner join plant p                            
  on p.id=it.id_plant           
 inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
    
  where  it.intmonth between '+@StartMonth+' and '+@endmonth+'                            
                         
 group by intmonth                            
                             
 insert into #TempForecatMPSdata                            
 Select  ''DIO'' as descr,''Revenue'' as type,''Revenue'' as categorytype,cast(intmonth as varchar)+''_ForecastInventory'' as intmonth,sum(value) as value                             
 from Z_Inventory_RollingReport_LatestMpsRevenueData  it                            
 inner join plant p                            
  on p.id=it.id_plant     
  inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
  where intmonth>'+@Currintmonth+'                            
                         
 group by intmonth                            
'                            
print(@Query)                            
exec(@Query)                            
                            
insert into #TempForecatMPSdata                            
                            
select 'DIO' as  Type,type descr,'DIO' as  CategoryType,cast(intmonth as varchar)+'_ForecastInventory' as intmonth,sum(value)                            
from #TEMPRollingReport_DIOInventoryJobData_MonthWise                            
 where type='DIO'                             
 and subcategorytype<>'EO Accural'                         
 group by type,intmonth                            
 union all                            
select 'DIO' as  Type,categorytype descr,categorytype ,cast(intmonth as varchar)+'_ForecastInventory' as intmonth,sum(value)                            
from #TEMPRollingReport_DIOInventoryJobData_MonthWise                            
 where type='DIO'                             
 and subcategorytype<>'EO Accural'                         
 group by categorytype,intmonth                            
 union all                            
select 'DIO' as  Type,subcategorytype descr,categorytype,cast(intmonth as varchar)+'_ForecastInventory' as intmonth,sum(value)                            
from #TEMPRollingReport_DIOInventoryJobData_MonthWise                            
 where type='DIO'      and subcategorytype<>'EO Accural'                        
 group by subcategorytype,intmonth,categorytype                            
 union all                            
select 'Total Inventory Value' as  Type,'Total Inventory Value' as  descr,'Total Inventory Value' as categoryType,cast(intmonth as varchar)+'_ForecastInventory' as intmonth,sum(value)                            
from #TEMPRollingReport_DIOInventoryJobData_MonthWise                            
 where type='Inventory'                             
 and subcategorytype<>'EO Accural'                         
 group by type,intmonth                            
 union all                            
select 'Total Inventory Value' as  Type,categorytype descr,categorytype,cast(intmonth as varchar)+'_ForecastInventory' as intmonth,sum(value)                            
from #TEMPRollingReport_DIOInventoryJobData_MonthWise                            
 where type='Inventory'                             
 and subcategorytype<>'EO Accural'                         
 group by categorytype,intmonth                            
 union all                            
select 'Total Inventory Value' as  Type,subcategorytype descr,categorytype,cast(intmonth as varchar)+'_ForecastInventory' as intmonth,sum(value)                            
from #TEMPRollingReport_DIOInventoryJobData_MonthWise                            
 where type='Inventory'                       
 group by subcategorytype,intmonth,categorytype                            
                            
                            
drop table if exists #TargetusereditedBBPDIOvalue                            
 drop table if exists #FIlearrivalBBPDIOvalue                            
                             
                            
 Select @Query='                            
                            
  select a.Type,a.intmonth ,isnull(sum(a.value),0) as value                            
  into #TargetusereditedBBPDIOvalue                            
  from (                            
 select ''DIO'' as type,intmonth  as intmonth,isnull(sum(value),0) as value                            
                            
  from Z_Inventory_RollingReport_LatestBPPData  it                             
 inner join Z_Inventory_TargetType t                            
 on t.id=it.id_inventorytargettype                 
                 
 inner join plant p                            
  on p.id=it.id_plant        
 inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
    
  where   t.description=''Dio''                            
                          
  group by it.intmonth                            
  union all                            
select ''DIO'',intmonth,0 from #intmonthforalignment                            
  )a                            
  group by a.type,a.intmonth                            
                            
 Select t.description as Type,it.intmonth,isnull(sum(value),0) as value                            
 into #FIlearrivalBBPDIOvalue                            
 from Z_Inventory_RollingReport_LatestBPPData it                           
 inner join Z_Inventory_TargetType t                            
  on t.id=it.id_inventorytargettype                            
 inner join plant p                            
  on p.id=it.id_plant           
  inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
    
  where  t.description=''Dio''                            
                         
  group by t.description ,intmonth                            
                            
  insert into #TempBPPdata                            
  Select t.Type as descr,''DIO'' as Type,cast(t.intmonth as varchar)+''_BBP'' as intmonth,case when isnull(t.value,0)=0 then isnull(f.value ,0) else t.value end as value                            
  from #TargetusereditedBBPDIOvalue t                            
  left join #FIlearrivalBBPDIOvalue f                            
   On t.Type collate database_default=f.type collate database_default                            
   and t.intmonth=f.intmonth                            
                            
                            
   -----------inventory Bpp                            
   select a.Type,a.intmonth ,isnull(sum(a.value),0) as value                            
  into #TargetusereditedBBPInventoryvalue                            
  from (                            
 select ''Total Inventory Value'' as type,intmonth  as intmonth,isnull(sum(value),0) as value                            
                            
                  
    from Z_Inventory_RollingReport_LatestBPPData  it                         
 inner join Z_Inventory_TargetType t                            
 on t.id=it.id_inventorytargettype                    
                
 inner join plant p                            
  on p.id=it.id_plant           
  inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
    
  where   t.description=''Total Inventory Value''                            
                         
  group by it.intmonth                            
  union all                            
select ''Total Inventory Value'',intmonth,0 from #intmonthforalignment                            
  )a                            
  group by a.type,a.intmonth                            
                            
 Select t.description as Type,it.intmonth,isnull(sum(value),0) as value                            
 into #FIlearrivalBBPInventoryvalue                            
 from Z_Inventory_RollingReport_LatestBPPData it                            
 inner join Z_Inventory_TargetType t                            
  on t.id=it.id_inventorytargettype                            
 inner join plant p                            
  on p.id=it.id_plant           
  inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
    
where  t.description=''Total Inventory Value''                            
                             
  group by t.description ,intmonth                            
                            
  insert into #TempBPPdata                            
  Select t.Type as descr,''Total Inventory Value'' as Type,cast(t.intmonth as varchar)+''_BBP'' as intmonth,case when isnull(t.value,0)=0 then isnull(f.value ,0) else t.value end as value                            
  from #TargetusereditedBBPInventoryvalue t                            
  left join #FIlearrivalBBPInventoryvalue f                            
   On t.Type collate database_default=f.type collate database_default                           
   and t.intmonth=f.intmonth                            
                            
                            
                            
 '                            
 print(@Query)                            
 exec(@Query)                            
                            
 Select @Query='                            
                            
 -------------_BPP------------------                            
 insert into #TempBPPdata                            
 select ''DIO'' as descr,ic.type,cast(intmonth as varchar)+''_BBP'' as intmonth,sum(value) as value                            
  from [Z_Inventory_Targets] it                            
 inner join Z_Inventory_TargetType t                            
 on t.id=it.id_targettype                            
 inner join Z_Inventory_Category ic                            
  on ic.id = it.ID_InventoryCategory                            
 inner join plant p                            
  on p.id=it.id_plant       
  inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
    
  where it.target=''BBP'' and t.description=''Dio''      
      
                     
  group by ic.type,it.intmonth                            
                            
  ------------------Inventory for BPP----                            
  union all                            
  select ''Total Inventory Value'' as descr,ic.type,cast(intmonth as varchar)+''_BBP'' as intmonth,sum(value) as value                            
  from [Z_Inventory_Targets] it                            
 inner join Z_Inventory_TargetType t                            
 on t.id=it.id_targettype                            
 inner join Z_Inventory_Category ic                            
  on ic.id = it.ID_InventoryCategory                            
 inner join plant p                            
  on p.id=it.id_plant       
  inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
    
  where it.target=''BBP'' and t.description=''Total Inventory Value''                            
                           
  group by ic.type,it.intmonth                            
                            
                            
  ---------------------------SIOP--------------------------                            
  insert into #TempSIOPdata                            
 select ''DIO'' as descr,ic.type,cast(intmonth as varchar)+''_SIOP'' as intmonth,sum(value) as value                            
  from [Z_Inventory_Targets] it                            
 inner join Z_Inventory_TargetType t                        
 on t.id=it.id_targettype                            
 inner join Z_Inventory_Category ic                            
  on ic.id = it.ID_InventoryCategory                            
 inner join plant p                            
  on p.id=it.id_plant              
  inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
    
  where it.target=''SIOP'' and t.description=''Dio''                            
                    
  group by ic.type,it.intmonth                            
 union all                            
  select ''DIO'' as descr,''DIO'' as type,cast(intmonth as varchar)+''_SIOP'' as intmonth,sum(value) as value                      
                  
  from Z_Inventory_RollingReport_LatestSIOPData it                            
 inner join Z_Inventory_TargetType t                            
 on t.id=it.id_inventorytargettype                     
                 
 inner join plant p                            
  on p.id=it.id_plant        
  inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
    
  where   t.description=''Dio''                            
                            
  group by it.intmonth                            
  ------------------Inventory for BPP----                            
  union all                            
  select ''Total Inventory Value'' as descr,ic.type,cast(intmonth as varchar)+''_SIOP'' as intmonth,sum(value) as value                            
  from [Z_Inventory_Targets] it                            
 inner join Z_Inventory_TargetType t                            
 on t.id=it.id_targettype                            
 inner join Z_Inventory_Category ic                            
  on ic.id = it.ID_InventoryCategory                            
 inner join plant p                            
  on p.id=it.id_plant       
 inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
    
  where it.target=''SIOP'' and t.description=''Total Inventory Value''                            
                            
  group by ic.type,it.intmonth                            
 union all                            
  select ''Total Inventory Value'' as descr,''Total Inventory Value'' as type,cast(intmonth as varchar)+''_SIOP'' as intmonth,sum(value) as value                            
                 
 from Z_Inventory_RollingReport_LatestSIOPData it                            
 inner join Z_Inventory_TargetType t                            
 on t.id=it.id_inventorytargettype                 
                 
 inner join plant p                            
  on p.id=it.id_plant       
 inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
    
  where   t.description=''Total Inventory Value''                            
                           
  group by it.intmonth                            
                            
  ----------------------------EXPECTATION-------------------------------                            
                            
  insert into  #TempExpectationdata                            
 select ''DIO'' as descr,ic.type,cast(intmonth as varchar)+''_EXPECTATION'' as intmonth,sum(value) as value                            
  from [Z_Inventory_Targets] it                            
 inner join Z_Inventory_TargetType t                            
 on t.id=it.id_targettype                            
 inner join Z_Inventory_Category ic                            
  on ic.id = it.ID_InventoryCategory                            
 inner join plant p                            
  on p.id=it.id_plant       
 inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
    
  where it.target=''Expectation'' and t.description=''Dio''                            
                    
  group by ic.type,it.intmonth                            
 union all                            
  select ''DIO'' as descr,''DIO'' as type,cast(intmonth as varchar)+''_EXPECTATION'' as intmonth,sum(value) as value                            
  from [Z_Inventory_Targets] it                            
 inner join Z_Inventory_TargetType t                            
 on t.id=it.id_targettype                            
 inner join Z_Inventory_Category ic                            
  on ic.id = it.ID_InventoryCategory                            
 inner join plant p                            
  on p.id=it.id_plant       
 inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
    
  where it.target=''Expectation'' and t.description=''Dio''                            
                           
  group by it.intmonth                            
  ------------------Inventory for BPP----                            
  union all                            
  select ''Total Inventory Value'' as descr,ic.type,cast(intmonth as varchar)+''_EXPECTATION'' as intmonth,sum(value) as value                            
  from [Z_Inventory_Targets] it                            
 inner join Z_Inventory_TargetType t                            
 on t.id=it.id_targettype                            
 inner join Z_Inventory_Category ic                            
  on ic.id = it.ID_InventoryCategory                            
 inner join plant p                            
  on p.id=it.id_plant      
 inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
    
  where it.target=''Expectation'' and t.description=''Total Inventory Value''                            
                           
  group by ic.type,it.intmonth                            
 union all                            
  select ''Total Inventory Value'' as descr,''Total Inventory Value'' as type,cast(intmonth as varchar)+''_EXPECTATION'' as intmonth,sum(value) as value                            
  from [Z_Inventory_Targets] it                            
 inner join Z_Inventory_TargetType t                            
 on t.id=it.id_targettype                            
 inner join Z_Inventory_Category ic                            
  on ic.id = it.ID_InventoryCategory                            
 inner join plant p                            
  on p.id=it.id_plant         
  inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
    
  where it.target=''Expectation'' and t.description=''Total Inventory Value''                            
                          
  group by it.intmonth                            
                            
                            
                            
 '                            
print(@Query)                            
exec(@Query)                            
                            
drop table if exists  #TempFinaldataInPivot                            
drop table if exists  #bpp                            
drop table if exists  #siop                            
drop table if exists  #mps                            
drop table if exists #RevenueDatasmonthwise                            
 Declare @nQuery nvarchar(max)                            
                              
 Select @nQuery='                            
                            
select    Id as Row,Parentid,type,case  when Description=''Revenue'' and type=''DIO'' then ''Revenue''                           
          when Description=''DIO'' and type=''DIO'' then ''DIO''                          
          when Description=''Total Inventory Value'' and type=''Total Inventory Value'' then ''Total Inventory Value''                           
          when Description=''RM'' and type=''DIO'' then ''RM''                       
          when Description=''WIP'' and type=''DIO'' then ''WIP''                            
          when Description=''FG'' and type=''DIO'' then ''FG''                             
           when Description=''RM'' and type=''Total Inventory Value'' then ''RM''                            
          when Description=''WIP'' and type=''Total Inventory Value'' then ''WIP''                            
          when Description=''FG'' and type=''Total Inventory Value'' then ''FG''                 
    when Parentid=3 then ''RM'' when Parentid=8 then ''WIP'' when Parentid=11 then ''FG''                          
          when Parentid=16 then ''RM'' when Parentid=22 then ''WIP'' when Parentid=26 then ''FG'' end as Categorytype                
 ,Description                            
     ,' + @intWeek_CurrentMonth + ' into #TempFinaldataInPivot                            
     from (                            
      select id,Parentid,type,description,intweek,value                            
                               
                                
                from #pivotformat                                    
            ) x                             
       pivot                                         
            (                               
                 sum( value)                                         
                for intweek in (' + @intWeek_CurrentMonth + ')                                        
 )a                            
 order by id                            
                            
 select    descr,type                            
     ,' + @intmonthcols_Expectation + ' into #Expectation                            
     from (                            
      select descr,type,intmonth,value                            
                from #TempExpectationdata                                  
            ) x                               
       pivot                                         
            (                                        
                 max( value)                                         
                for intmonth in (' + @intmonthcols_Expectation + ')                                        
 )a                            
                            
 select    descr,type                            
     ,' + @intmonthcols_BBP + ' into #Bpp                            
     from (                            
      select descr,type,intmonth,value                            
                from #TempBPPdata                                  
            ) x                               
       pivot                                         
            (                                        
                 max( value)                                       
             for intmonth in (' + @intmonthcols_BBP + ')                                        
 )a                            
                            
 select    descr,type                            
     ,' + @intmonthcols_SIOP + ' into #SIop                            
     from (                            
      select descr,type,intmonth,value                
                from #TempSIOPdata                               
            ) x                               
       pivot                                         
            (                                        
                 max( value)                                         
                for intmonth in (' + @intmonthcols_SIOP + ')                                        
 )a                            
  select    descr,type,categoryType                            
     ,' + @intmonthcols_MpsRevenue + ' into #MPS                            
     from (                            
      select descr,type,CategoryType,intmonth,value                            
                from #TempForecatMPSdata                               
            ) x                               
       pivot                                         
            (                                        
                 max( value)                                         
                for intmonth in (' + @intmonthcols_MpsRevenue + ')                                        
 )a                            
                            
 Select b.*,'+@intmonthcols_SIOP +','+@intmonthcols_Expectation+','+@intmonthcols_BBP+' into #RevenueDatasmonthwise                            
 from #mps b                            
 Left join  #siop s                            
 on b.type=s.type                            
 and b.descr=s.descr                            
 left join #Bpp m                            
 on b.type=m.type                             
 and b.descr=m.descr                           
left join #Expectation e                            
 on b.type=e.type                            
 and b.descr=e.descr                            
                             
---- select * from #RevenueDatasmonthwise                            
                            
Select row_number()over(order by Row ) as ID,''Natural'' as Module,p.*,'+@intmonthcols_BBP+','+@intmonthcols_SIOP +','+@intmonthcols_MpsRevenue+'                             
into Firstlevelgriddata                            
from #TempFinaldataInPivot P                            
left join #RevenueDatasmonthwise r                            
 On p.Description=r.Type                            
 and p.Type=r.descr                           
and p.CategoryType collate database_default=r.CategoryType  collate database_default                    
where p.description collate database_default not in (select description collate database_default from Z_Inventory_ForecastCategory where ismanualinput = 1 )   
and p.row is not null  
order by p.row            
        
'                            
 Print(@nQuery)                            
 Exec(@nQuery)                            
                            
                          
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                            
                            
           ----Second Grid-----------------                            
                            
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                            
                            
                            
                            
                            
--select distinct intweek from calendar where id_plant=1 and intmonth=202011                            
                            
Declare @Id_Plant_Action Varchar(100)=@Id_Plant                            
Declare @Region_Action Varchar(100)=@Region                            
                            
Declare @getdate_Action date=getdate()                            
                            
declare @Currintweek_Action int                            
declare @Currintmonth_Action varchar(50)                            
                            
 Set @Currintweek_Action=(select top 1 intweek from calendar where date=@getdate_Action)                            
 set @Currintmonth_Action=(select top 1 intmonth from calendar where date=@getdate_Action)                            
                            
Declare @StartMonth_Action varchar(100)                            
Declare @EndMonth_Action varchar(100), @StartMonthPlusOne varchar(100);                           
                            
                            
 select @StartMonthPlusOne =min(intmonth) + 1 ,@StartMonth_Action=min(intmonth), @EndMonth_Action=max(intmonth)                            
 from                  
 (                            
 select row_number()over(order by intmonth) Rn,intmonth                            
 from (                            
 select distinct intmonth from calendar where intmonth>=@Currintmonth_Action                            
  )a                            
 )V                            
 Where v.rn<= @forcastMonth                         
                            
drop table if exists #intweek_ActionPlan                            
                             
 select distinct c.intweek ,c.intmonth                            
  into #intweek_ActionPlan                            
  from Calendar c                             
where c.intMonth between @StartMonth_Action and @EndMonth_Action                            
                             
                            
 DECLARE @intWeek_CurrentMonth_Action AS VARCHAR(MAX)                           
                            
 select @intWeek_CurrentMonth_Action =STUFF((SELECT ',' + QUOTENAME(intweek)                                
    from (select distinct max(intweek) intweek from  #intweek_ActionPlan where intmonth=@Currintmonth_Action  )a                            
  group by intweek                                
        order by intweek                                
        FOR XML PATH(''), TYPE                                
            ).value('.', 'NVARCHAR(MAX)')                                 
        ,1,1,'')                       
                            
                                          
  DECLARE @intmonthcols_ActionPlan AS VARCHAR(MAX)                                  
                            
                            
 select @intmonthcols_ActionPlan =STUFF((SELECT ',' + QUOTENAME(intmonth)                                
    from (select distinct  cast(intmonth as Varchar)+'_ForecastInventory' as intmonth from  #intweek_ActionPlan where intmonth>@Currintmonth_Action  )a                            
  group by intmonth                                
        order by intmonth                                
        FOR XML PATH(''), TYPE             
            ).value('.', 'NVARCHAR(MAX)')                                 
        ,1,1,'')                                
                            
                            
                            
--select @intWeek_CurrentMonth_Action,@intmonthcols_ActionPlan,@intmonthcols_SIOP,@intmonthcols_Expectation,@intmonthcols_MpsRevenue                            
                            
Drop table if exists #TEMPRollingReport_Actionplan                            
                            
                            
Create table #TEMPRollingReport_Actionplan                            
(                            
id_plant int,                            
Type varchar(150),                            
CategoryType varchar(150),                            
SubCategoryType varchar(200),                            
DateClosure date,                            
intweek int,                            
TargetInventoryValue float                      
)                            
                            
                            
drop table if exists #calendar                            
create table #calendar                            
(                            
id_plant int,                            
intweek int,                            
intmonth int,                            
date date,                            
                            
                            
)                            
                            
drop table if exists #Tempcalendar_ActionPlan                            
create table #Tempcalendar_ActionPlan                            
(                            
                            
intweek int,                            
intmonth int                            
)                            
insert into #Tempcalendar_ActionPlan                            
Select Distinct intweek,intmonth from calendar where intmonth between @StartMonth_Action and @EndMonth_Action                            
                            
 Declare @Query_Actionplan nvarchar(max)                            
                            
 Select @Query_Actionplan='                            
                             
insert into #calendar                             
select distinct c.id_plant,a.intweek,c.intmonth,c.date                             
                            
from (                            
select id_plant,intweek,intmonth,date from calendar                             
where intmonth between '+@StartMonth_Action+' and '+@EndMonth_Action+'                            
'+ case when @Id_Plant_Action is not null then '  AND id_plant in ('+@Id_Plant_Action +')' else '' end +')                            
c                            
inner join (                            
                            
select id_plant,intmonth,max(intweek)intweek                             
from calendar                             
where intmonth between '+@StartMonth_Action+' and '+@EndMonth_Action+'                            
'+ case when @Id_Plant_Action is not null then '  AND id_plant in ('+@Id_Plant_Action +')' else '' end +'                            
group by id_plant,intmonth                            
)a                            
on c.id_plant=a.id_plant                            
and c.intmonth=a.intmonth                            
                            
 insert into #TEMPRollingReport_Actionplan                            
 select p.id as id_plant,''Inventory'' as type,ic.type as CategoryType                            
   ,case when ifc.type=''regular'' then ''Regular'' else ifc.type end as SubCategoryType                            
   ,rr.DateClosure,cc.intweek                            
   ,-1*sum(rr.TargetInventoryValue)/1000000 as value                             
 from [Z_Inventory_Analysis] RR                            
 inner join Z_Inventory_MaterialMaster mm                            
  on rr.id_inventorymaterialmaster=mm.id                            
 inner join Z_Inventory_Category ic                            
   on ic.id = mm.ID_InventoryCategory                            
 inner join Z_Inventory_SubCategory ifc                            
   on ifc.id = mm.ID_InventorysubCategory                            
 Inner Join Plant p                            
  On mm.id_plant=p.id                              
 inner join #calendar cc                            
  on cc.id_plant=p.id                            
  and cc.date=rr.DateClosure         
 inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
    
  where rr.status in (1,4)                     
 and  cc.intmonth between '+@StartMonth_Action+' and '+@EndMonth_Action+'                            
                           
 Group by  p.id,ic.type,ifc.type,rr.DateClosure,cc.intweek                            
                           
                 
 union all     
   
   select rf.id_plant, ''Inventory'' as type , ic.type as CategoryType  , ''Past Dues'' as SubCategoryType,  
  null as DateClosure,   rf.intweek   , Value as PastDueValue  
  from Z_Inventory_RollingReport_DIOInventoryJobData rf  
  inner join Z_Inventory_Category ic                            
   on ic.id = rf.ID_Category          
  Inner Join Plant p                            
 On rf.id_plant=p.id      
  inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
where rf.id_category = 1   
and rf.Type=''PastDue''  
and  rf.intmonth  =  cast('+@Currintmonth+' as int )     
                
 union all         
        
        
select d.Id_plant, ''Inventory'' as type , ''RM'' as CategoryType  , ''Past Dues'' as SubCategoryType, null as DateClosure,  c.intweek, sum(d.PastDues) as PastDues from Z_Inventory_RollingReport_PastDues d         
inner join plant p on d.id_plant  = p.id         
inner join         
(select id_plant, intmonth, max (intweek) as intweek from #calendar c  group by intmonth , id_plant)         
c on d.intmonth  = c.intmonth and d.id_plant = c.id_plant        
inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
    
where d.Intmonth between    '+@StartMonthPlusOne   +' and '+@EndMonth_Action+'           
     
group by d.Id_plant, d.intmonth, c.intweek         
        
union all         
        
select d.Id_plant, ''Inventory'' as type , ''RM'' as CategoryType  , ''Past Dues Carry Forward'' as SubCategoryType, null as DateClosure,  c.intweek, sum(d.Pastdues_CarryFwd) as PastDuesCF from Z_Inventory_RollingReport_PastDues d         
inner join plant p on d.id_plant  = p.id         
inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
    
inner join         
(select id_plant, intmonth, max (intweek) as intweek from #calendar c  group by intmonth , id_plant)         
c on d.intmonth  = c.intmonth and d.id_plant = c.id_plant        
where d.Intmonth between    '+ @StartMonth_Action   +' and '+@EndMonth_Action+'           
          
        
group by d.Id_plant, d.intmonth, c.intweek         
        
union all                      
                 
                
   select    a.id_plant , a.type, a.categorytype, a.subcategorytype, null , a.intweek , a.value   from                 
   Z_Inventory_RollingReport_DIOInventoryJobData a with(nolock)                
   Inner Join Plant p                            
   On a.id_plant=p.id       
   inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
    
   inner join Z_Inventory_ForecastCategory i on                 
   a.Subcategorytype = i.Description                 
   where                 
    a.type = ''Inventory''  and i.isManualInput = 1 and intweek                
 in ( select max(intweek) from calendar where  intmonth =  cast('+@Currintmonth+' as int ) )                
               
                
                         
 '                            
 Print(@Query_Actionplan)                            
 Exec(@Query_Actionplan)                            
  /********* Dummy Past Dues Fwd if no records *************/      
      
 if not exists (  select * from  #TEMPRollingReport_Actionplan  where SubCategoryType = 'Past Dues Carry Forward')      
  begin       
        
  insert into  #TEMPRollingReport_Actionplan         
  select top 1 id_plant, 'Inventory' as Type, 'RM' as CategoryType,'Past Dues Carry Forward' as SubCategoryType, NULL as DateClosure, intweek, 0 as TargetInventoryValue      
  from #calendar order by intweek        
 end       
      
  /********* End of Dummy Past Dues Fwd if no records *************/                          
                             
 drop table if exists #TempOrdercategorytype_Action                            
                            
 select distinct ic.id as id_category,ic.Type as categorytype,ifc.Description as Subcategorytype                            
 into #TempOrdercategorytype_Action                            
 from Z_Inventory_ForecastMapping map                            
 inner join Z_Inventory_Category ic                            
  on ic.id = map.ID_InventoryCategory                            
 inner join Z_Inventory_ForecastCategory ifc                            
  on ifc.id = map.ID_InventoryForecastCategory                            
                            
                            
drop table if exists #TempArrangingOrderingdatas_Action                            
create table #TempArrangingOrderingdatas_Action(                            
Type Varchar(100),                            
categorytype varchar(100),                            
Subcategorytype varchar(100),                            
intweek int,                            
value float                            
                            
)                            
                            
                            
drop table if exists  #InputDatasFromUI_Action                            
create table  #InputDatasFromUI_Action(                            
Type Varchar(100),                            
categorytype varchar(100),                            
Subcategorytype varchar(100),                            
intweek int,                            
value float                            
                            
)                            
                            
insert into #TempArrangingOrderingdatas_Action               
Select distinct a.type,a.categorytype,a.Subcategorytype,a.intweek,a.value                            
from (                            
Select distinct c.*,i.*,0 as value,r.*                            
  from #TempOrdercategorytype_Action c                            
   cross join (select distinct intweek from  #intweek_ActionPlan where intmonth between @StartMonth_Action and @EndMonth_Action) i                            
   Cross join (select * from  Z_Inventory_TypeForRollingForecast where type='Inventory')R                            
  )a                            
 left join                             
  (                            
  Select * from  #TEMPRollingReport_Actionplan                            
                              
  )b                            
                               
   ON a.categorytype collate database_default=b.categorytype collate database_default                            
   and a.subcategorytype collate database_default=b.subcategorytype collate database_default             
   and a.Type collate database_default=b.Type collate database_default                            
   and a.intweek=b.intweek                            
                               
   where b.categorytype is  null                            
                            
                            
                            
                            
Drop table if exists #TempTypeCategorywiseData_Action                            
Select  Type,CategoryType,SubCategoryType,intweek,sum(value)as value                             
into #TempTypeCategorywiseData_Action                            
from (                            
  Select Type,CategoryType,SubCategoryType,intweek,sum(TargetInventoryValue)as value                              
  from  #TEMPRollingReport_Actionplan                            
  group by Type,CategoryType,SubCategoryType,intweek                            
  union all                       
  select * from #TempArrangingOrderingdatas_Action                            
  )a                            
  group by  Type,CategoryType,SubCategoryType,intweek                            
                            
                            
                            
                            
drop table if exists #pivotformat_ActionPlan                            
                            
create table #pivotformat_ActionPlan                            
(                            
                            
Description varchar(250),                            
intweek int,                            
value decimal(35,2),                            
id int,                            
Parentid int                            
)                            
                            
drop table if exists #Intmonthpivotformat_ActionPlan                            
create table #Intmonthpivotformat_ActionPlan                      
(                            
                            
Description varchar(250),                            
intmonth varchar(250),                            
value decimal(35,2),                            
id int,                            
Parentid int                            
)                      
                            
insert into #pivotformat_ActionPlan                            
                            
--------------Inventory and Rm-----------                            
                            
  select  'Action Plan'type,intweek,sum( case when subcategorytype= 'Past Dues Carry Forward' then  value *-1 else value end ) as value,1 as id,1 as parentid                          
  from #TempTypeCategorywiseData_Action                            
   where type='Inventory' and subcategorytype<>'EO Accural'                            
   group by type,intweek                            
union all                            
    select  categorytype,intweek,sum(case when subcategorytype= 'Past Dues Carry Forward' then  value *-1 else value end ) as value,2 as id,1 as parentid        
  from #TempTypeCategorywiseData_Action                            
   where type='Inventory' and categorytype='RM' and subcategorytype<>'EO Accural'                            
   group by categorytype,intweek                            
 union all                             
  select  subcategorytype,intweek,sum(value) as value,                
                  
 /* case when subcategorytype='Regular' then 3                             
   when subcategorytype='Excess' then 4    when subcategorytype='NPI' then 5                             
    when subcategorytype='LTB' then 6  when subcategorytype='Special Agreement' then 7                              
     when subcategorytype='Intercompany in-transit' then 8   when subcategorytype='In-Transit from Supplier' then 9                             
      when subcategorytype='Others(Fx impact)' then 10    when subcategorytype='EO Accural' then 11                   
     when subcategorytype='Past Dues' then 3                 
  end as id    */                
                  
                
   case     when subcategorytype='Past Dues' then 3        
   when subcategorytype = 'Past Dues Carry Forward' then 4         
   when subcategorytype='Regular' then 5                             
  -- when subcategorytype='Excess' then 5                
   when subcategorytype='NPI' then 6                             
    when subcategorytype='LTB' then 7  when subcategorytype='Special Agreement' then 8                
     when subcategorytype='Intercompany in-transit' then 9   when subcategorytype='In-Transit from Supplier' then 10                             
      when subcategorytype='Others(Fx impact)' then 11    when subcategorytype='EO Accural' then 12                   
                 
  end as id                   
                              
  ,2 as parentid                            
  from #TempTypeCategorywiseData_Action                            
   where type='Inventory' and categorytype='RM'                            
   group by subcategorytype,intweek                            
union all                            
------------------Inventory and WIP---                            
  select  categorytype,intweek,sum(value) as value,13 as id,1 as parentid                            
  from #TempTypeCategorywiseData_Action                            
   where type='Inventory' and categorytype='WIP' and subcategorytype<>'EO Accural'                            
   group by categorytype,intweek                    
                   
     union all                             
    select  subcategorytype,intweek,sum(value) as value,                
 case                  
  when subcategorytype='Others(Fx impact)' then 14 end as id , 13 as  parentid                  
  from #TempTypeCategorywiseData_Action                            
   where type='Inventory' and categorytype='WIP' and subcategorytype not in ( 'EO Accural', 'Excess',  'NPI', 'Regular')                      
   group by subcategorytype,intweek                   
                            
union all ------------------Inventory and FG---                            
  select  categorytype,intweek,sum(value) as value,15 as id,1 as parentid                            
  from #TempTypeCategorywiseData_Action                            
   where type='Inventory' and categorytype='FG' and subcategorytype<>'EO Accural'                            
   group by categorytype,intweek                        
                
      union all                    
                 
    select  subcategorytype,intweek,sum(value) as value,                   
 case  when subcategorytype='Others(Fx impact)' then 16                 
  when subcategorytype='In-Transit to Customer' then 17                
 end as id , 15 as  parentid                  
  from #TempTypeCategorywiseData_Action                            
   where type='Inventory' and categorytype='FG' and subcategorytype               
   not in ( 'EO Accural', 'Excess',  'NPI', 'Regular', 'Special Agreement')                      
   group by subcategorytype,intweek                  
                            
-------------------------------------Intmonth Level--------------------                            
                            
                            
                            
                            
insert into #Intmonthpivotformat_ActionPlan                            
                            
--------------Inventory and Rm-----------                            
                            
  select  'Action Plan'type,cast(intmonth as Varchar)+'_ForecastInventory' as intmonth,sum(case when subcategorytype= 'Past Dues Carry Forward' then  value *-1 else value end ) as value,1 as id,1 as parentid                           
  from #TempTypeCategorywiseData_Action t                            
  inner join (select distinct intweek,intmonth from #calendar) c                            
   ON t.intweek=c.intweek                            
   where type='Inventory' and subcategorytype<>'EO Accural'                  
   group by type,intmonth                            
union all                            
select  categorytype,cast(intmonth as Varchar)+'_ForecastInventory' as intmonth,sum(case when subcategorytype= 'Past Dues Carry Forward' then  value *-1 else value end ) as value,2 as id,1 as parentid                          
  from #TempTypeCategorywiseData_Action  t                            
  inner join (select distinct intweek,intmonth from #calendar) c                     
   ON t.intweek=c.intweek                             
   where type='Inventory' and categorytype='RM' and subcategorytype<>'EO Accural'                            
   group by categorytype,intmonth                            
 union all                             
  select  subcategorytype,cast(intmonth as Varchar)+'_ForecastInventory' as intmonth,sum(value) as value,3 as id,2 as parentid                            
  from #TempTypeCategorywiseData_Action t                            
  inner join (select distinct intweek,intmonth from #calendar) c                            
   ON t.intweek=c.intweek                            
   where type='Inventory' and categorytype='RM' --and subcategorytype<>'EO Accural'                            
   group by subcategorytype,intmonth                            
union all                            
------------------Inventory and WIP---                            
  select  categorytype,cast(intmonth as Varchar)+'_ForecastInventory' as intmonth,sum(value) as value,4 as id,1 as parentid                            
  from #TempTypeCategorywiseData_Action t              
  inner join (select distinct intweek,intmonth from #calendar) c                            
   ON t.intweek=c.intweek                            
   where type='Inventory' and categorytype='WIP' and subcategorytype<>'EO Accural'                            
   group by categorytype,intmonth                            
                            
union all ------------------Inventory and FG---                            
  select  categorytype,cast(intmonth as Varchar)+'_ForecastInventory' as intmonth,sum(value) as value,5 as id,1 as parentid                            
  from #TempTypeCategorywiseData_Action t                            
  inner join (select distinct intweek,intmonth from #calendar) c                            
   ON t.intweek=c.intweek                            
   where type='Inventory' and categorytype='FG' and subcategorytype<>'EO Accural'                            
   group by categorytype,intmonth                            
                            
                            
                            
                            
drop table if exists  #TempFinaldataInPivot_ActionPlan                            
drop table if exists #TempIntmonthlevelInPivot_ActionPlan                            
                            
 Declare @nQuery_ActionPlan nvarchar(max)                            
                              
 Select @nQuery_ActionPlan='                            
                            
select    Id as Row,Parentid,Description                            
     ,' + @intWeek_CurrentMonth_Action + ' into #TempFinaldataInPivot_ActionPlan                            
     from (                            
      select id,Parentid,description,intweek,value                            
                               
                                
                from #pivotformat_ActionPlan                                    
            ) x                               
       pivot                                         
            (                                        
                 sum( value)                                         
                for intweek in (' + @intWeek_CurrentMonth_Action + ')                                        
 )a                            
 order by id                            
                            
 select    Description                            
     ,' + @intmonthcols_ActionPlan + '  into #TempIntmonthlevelInPivot_ActionPlan                            
     from (                            
      select id,Parentid,description,intmonth,value                            
                               
                                
       from #Intmonthpivotformat_ActionPlan                              
            ) x                               
       pivot                                         
            (          
                 sum( value)                                         
                for intmonth in (' + @intmonthcols_ActionPlan + ')                               
 )a                            
 order by id                            
                            
                             
Select row_number()over(order by Row ) as ID,''Action Plan'' as Module,p.*,' + @intmonthcols_ActionPlan + '                             
into SecondlevelGriddata                            
from #TempFinaldataInPivot_ActionPlan P                            
left join #TempIntmonthlevelInPivot_ActionPlan M                            
  On p.Description=M.Description                         
 '                            
 Print(@nQuery_ActionPlan)                            
 Exec(@nQuery_ActionPlan)                            
                            
                            
                            
------------------------------------------------------------------Third Level Starts-----------------------------------------------------              
---                            
                            
                            
Drop Table if Exists #TempFirstLevelInventoryandactionPanInventory                            
create table #TempFirstLevelInventoryandactionPanInventory                            
(                            
                            
CategoryType varchar(250),                            
intweek int,                            
value float                            
                            
)                            
                            
                            
Drop Table if Exists #FinalDIODataThirdlevelDIOCalc                            
create table #FinalDIODataThirdlevelDIOCalc                            
(                           
                            
CategoryType varchar(250),                            
intweek int,                            
value float                            
                            
)                            
                            
--select z.CategoryType,z.intweek,sum(z.value)                
--from #TempTypeCategorywiseData z                
--where Type = 'DIO' and subcategorytype<>'EO Accural' and intweek < 202116                
--group by z.CategoryType,z.intweek                
                
--------------------------DIO Calculation FOr Third Level Grid--------------------------                           
                        
insert into #TempFirstLevelInventoryandactionPanInventory                            
Select  categorytype,intweek,sum(value) as value from (                            
  select  categorytype,intweek,sum(value) as value                            
  from #TempTypeCategorywiseData                            
   where type='Inventory' and subcategorytype<>'EO Accural'                 
   and SubCategoryType collate database_default not in (select description collate database_default               
   from Z_Inventory_ForecastCategory where ismanualinput = 1 )                
   group by categorytype,intweek                            
 Union all                            
  select  categorytype,intweek,sum(case when subcategorytype= 'Past Dues Carry Forward' then  value *-1 else value end) as value                           
  from #TempTypeCategorywiseData_Action                            
   where type='Inventory'  and subcategorytype<>'EO Accural'                
   group by categorytype,intweek                            
   )a group by categorytype,intweek                            
------------------------------------------------------------------------------------------------                            
--declare @id_plant varchar(max)=1    ,@region varchar(max)=null                        
declare @SQLString varchar(max)                   
      
        
       
drop table if exists #dividantValue                 
     
create table #dividantValue(                
intmonth int,                
dividantValue float        
)                
                            
select  @SQLString = '                  
 Declare @past3monthcalendardays int                           
 Declare @getdate date=getdate()                            
 Declare @Currintmonth int                            
 set @Currintmonth=(select top 1 intmonth from calendar where date=@getdate and id_plant in ('+@id_plant +'))                            
                            
drop table if exists #TempintmonthforPastShipmentSummaryQuantity                         
                            
                            
select *  into #TempintmonthforPastShipmentSummaryQuantity from (                            
select row_number()over(order by intmonth desc ) rn ,* from (                            
select distinct intmonth from calendar where intmonth<@Currintmonth                            
)a                            
)b where rn<=(select value-1 from properties where key1=''Past3monthShipementandCalendarForRollingReport'')                           
                            
select @past3monthcalendardays=count(*) from calendar                               
 where intmonth in (select intmonth from #TempintmonthforPastShipmentSummaryQuantity union select @Currintmonth)                            
 and id_plant=1 --for default                    
                  
                  
drop table if exists #TempintmonthforFutureRevenue                            
                            
                            
select *  into #TempintmonthforFutureRevenue from (                            
select row_number()over(order by intmonth asc ) rn ,* from (                            
select distinct intmonth from calendar where intmonth>=@Currintmonth                   
)a                                     
)b where rn<=(select value from properties where key1=''Past3monthShipementandCalendarForRollingReport'')                  
                  
drop table if exists #ShipmentAndRevenue                  
Select  intmonth,convert(decimal(20,5),SUM(ShipmentValue))/1000000 as Shipment,null as Revenue                     
into #ShipmentAndRevenue                  
from Z_Inventory_ShipmentSummaryQuantity s                            
inner join plant p                            
on s.id_plant=p.id      
inner join #PlantFilterforCommaseperated pl    
  on p.id =pl.plvalue    
    
where intmonth in (select intmonth from #TempintmonthforPastShipmentSummaryQuantity)                            
                  
group by intmonth                  
union all                  
Select intmonth,null as Shipment, Sum(value) as Revenue from Z_Inventory_RollingReport_LatestSIOPRevenueData where id_plant in ('+@id_plant +')               
and intmonth in (select intmonth from #TempintmonthforFutureRevenue)                  
group by intmonth                  
                
                
Insert into #dividantValue                    
select intmonth,rev/@past3monthcalendardays as dividantValue                  
                 
from                  
(                  
select *,sum(case when shipment is null then Revenue else Shipment end)              
OVER(ORDER BY intmonth asc ROWS BETWEEN  2 Preceding AND current ROW) as rev from #ShipmentAndRevenue                  
)z                  
                  
insert into #FinalDIODataThirdlevelDIOCalc                       
select z.CategoryType,z.intweek,sum(z.value) as DIOValue                
from #TempTypeCategorywiseData z                
where Type = ''DIO'' and subcategorytype<>''EO Accural'' and intweek < '+convert(varchar,@Currintweek)+'                
group by z.CategoryType,z.intweek                
                
insert into #FinalDIODataThirdlevelDIOCalc                  
select distinct a.CategoryType,a.intweek,b.DIOValue                       
from #TempFirstLevelInventoryandactionPanInventory a                      
Inner Join (                      
   select distinct fv.CategoryType,fv.intweek,fv.value/nullif(dv.dio_denominator/1000000,0) as DIOValue                       
   from #TempFirstLevelInventoryandactionPanInventory fv                        
   --Inner join #TempIntmonthMaxIntweekgrid ti                        
   -- on fv.intweek=ti.intweek        
   inner join #temp_dio_raw_data dv        
  on dv.intmonth = '+@Currintmonth +'         
  -- Inner join #dividantValue dv                  
 --   on dv.intmonth = '+@Currintmonth +'                 
 where fv.intweek >= '+convert(varchar,@Currintweek)+'                
  )b                      
 On a.CategoryType=b.CategoryType                     
 and a.intweek = b.intweek                
 order by a.CategoryType,a.intweek                    
                            
                            
----------Deleting the existing DIO to calculate a new one using above past three mont shipment for third level grid                            
                            
delete from #TempForecatMPSdata where descr=''DIO'' and type not in (''Revenue'')                            
                            
insert into #TempForecatMPSdata                        
select b.Descr,b.type,b.CategoryType,b.intmonth,b.value/nullif(dv.dio_denominator/1000000,0)  as Value                  
from                  
(                  
select Descr,type,CategoryType,a.intmonth,sum(value)  as Value                        
from (                        
 select ''DIO'' as Descr,case when Description=''Action Plan'' then ''DIO'' else Description end as type,''DIO'' as CategoryType                          
 ,intmonth,value*1 as value                             
 from #Intmonthpivotformat_ActionPlan                          
 union all                     
 select ''DIO'' as Descr,''DIO'' as type,''DIO'' as CategoryType ,intmonth,value                         
 from #TempForecatMPSdata                         
 where Type=''Total Inventory Value'' and descr=''Total Inventory Value''                        
  union all                        
 select ''DIO'' as Descr,type,CategoryType ,intmonth,value                         
 from #TempForecatMPSdata                         
 where Type in (''RM'',''FG'',''WIP'') and descr=''Total Inventory Value''                        
 )a                   
group by Descr,type,CategoryType,a.intmonth                   
)b                  
                  
--inner join #dividantValue dv                  
--    on dv.intmonth = SUBSTRING(b.intmonth,0,CHARINDEX(''_'',b.intmonth))                      
  inner join #temp_dio_raw_data dv        
  on dv.intmonth = SUBSTRING(b.intmonth,0,CHARINDEX(''_'',b.intmonth))                          
 '                            
 print(@SQLString)                    
 EXEC(@SQLString)                
                             
Drop Table if Exists #TempFinalDIOAndInventoryvaluesInPivot                            
create table #TempFinalDIOAndInventoryvaluesInPivot                            
(                            
                            
Description varchar(250),                            
intweek int,                            
value decimal(35,2),                            
id int,                            
Parentid int,                            
Type varchar(250)                  
)                            
                            
                            
                            
insert into #TempFinalDIOAndInventoryvaluesInPivot                            
select *,'DIO' as Type from #Z_Inventory_RollingReport_LatestMpsRevenueData                            
union all                            
 ----------DIO and Rm-------------                            
  select  'DIO' type,intweek,sum(value) as value,2 as id,2 as Parentid,'DIO' as Type                            
  from #FinalDIODataThirdlevelDIOCalc                            
                               
   group by intweek                            
union all                            
  select  categorytype,intweek,sum(value) as value,3 as id,2 as Parentid,'DIO' as Type                            
  from #FinalDIODataThirdlevelDIOCalc                            
   where  categorytype='RM'                            
   group by categorytype,intweek                            
                            
union all                            
 ----------DIO and WIP---                            
    select  categorytype,intweek,sum(value) as value,4 as id,2 as Parentid,'DIO' as Type                            
  from #FinalDIODataThirdlevelDIOCalc                            
   where  categorytype='WIP'                            
   group by categorytype,intweek                            
                            
union all                            
 ----------------DIO and FG-----------                            
                            
    select  categorytype,intweek,sum(value) as value,5 as id,2 as Parentid,'DIO' as Type                            
  from #FinalDIODataThirdlevelDIOCalc                            
   where  categorytype='FG'                            
   group by categorytype,intweek                            
                            
                            
union all                            
--------------Inventory and Rm-----------                            
                            
  select  'Total Inventory Value'type,intweek,sum(value) as value,6 as id,6 as parentid,'Total Inventory Value' as Type                            
  from #TempFirstLevelInventoryandactionPanInventory                            
   group by intweek                            
union all                            
  select  categorytype,intweek,sum(value) as value,7 as id,6 as parentid,'Total Inventory Value' as Type                            
  from #TempFirstLevelInventoryandactionPanInventory                            
   where categorytype='RM'                            
   group by categorytype,intweek                            
                            
union all                            
------------------Inventory and WIP---                            
  select  categorytype,intweek,sum(value) as value,8 as id,6 as parentid,'Total Inventory Value' as Type                            
  from #TempFirstLevelInventoryandactionPanInventory                            
   where categorytype='WIP'                            
   group by categorytype,intweek                            
                            
union all ------------------Inventory and FG---                            
  select  categorytype,intweek,sum(value) as value,9 as id,6 as parentid,'Total Inventory Value' as Type                            
  from #TempFirstLevelInventoryandactionPanInventory                            
   where categorytype='FG'                            
   group by categorytype,intweek                            
                            
                            
                            
drop table if exists  #TempThirdlevelGridFinaldataInPivot                            
drop table if exists  #bpp_Thirdlevel                            
drop table if exists  #siop_Thirdlevel                            
drop table if exists  #mps_Thirdlevel                            
drop table if exists #Expectation_Thirdlevel                            
drop table if exists #RevenueDatasmonthwise_Thirdlevel                            
  drop table if exists #TempForecatMPSdata_PivotThirdlevel                          
                             
  select Descr,Type,intmonth,sum(value)value                           
  into #TempForecatMPSdata_PivotThirdlevel                          
  from                           
  (                            
  select descr,type,intmonth,value from #TempForecatMPSdata                            
  union all                            
 select 'Total Inventory Value' as Descr,case when Description='Action Plan' then 'Total Inventory Value' else Description end as type                          
 ,intmonth,value --*-1              /****reverted this past due sign change ****/               
 from #Intmonthpivotformat_ActionPlan                            
 )a group by Descr,Type,intmonth                            
 Select @nQuery='                 
                            
select    Id as Row,Parentid,type,Description                            
     ,' + @intWeek_CurrentMonth + ' into #TempThirdlevelGridFinaldataInPivot                            
     from (          
      select id,Parentid,type,description,intweek,value                            
                               
                                
                from #TempFinalDIOAndInventoryvaluesInPivot                                  
            ) x                               
       pivot                                         
       (                                        
                 sum( value)                                         
                for intweek in (' + @intWeek_CurrentMonth + ')                                        
 )a                            
 order by id                            
                            
 select    descr,type                            
     ,' + @intmonthcols_Expectation + ' into #Expectation_Thirdlevel                            
     from (                            
      select descr,type,intmonth,value                            
                from #TempExpectationdata                                  
            ) x                               
       pivot                                         
            (                                        
                 max( value)                                         
                for intmonth in (' + @intmonthcols_Expectation + ')                                        
 )a                            
                            
 select    descr,type                            
     ,' + @intmonthcols_BBP + ' into #bpp_Thirdlevel                            
     from (                            
      select descr,type,intmonth,value                            
                from #TempBPPdata                                  
            ) x                               
       pivot                                         
            (                                        
                 max( value)                                         
                for intmonth in (' + @intmonthcols_BBP + ')                                        
 )a                            
                            
 select    descr,type                            
     ,' + @intmonthcols_SIOP + ' into #siop_Thirdlevel                            
     from (                            
      select descr,type,intmonth,value                            
   from #TempSIOPdata                               
            ) x                               
       pivot                                         
            (                                        
                 max( value)                                         
                for intmonth in (' + @intmonthcols_SIOP + ')                                        
 )a                            
  select    descr,type                            
     ,' + @intmonthcols_MpsRevenue + ' into #mps_Thirdlevel                            
     from (        
      select descr,type,intmonth,value                            
                from #TempForecatMPSdata_PivotThirdlevel                              
            ) x                               
       pivot                                         
            (                                        
                 max( value)                                         
                for intmonth in (' + @intmonthcols_MpsRevenue + ')                                        
 )a                            
                            
 --select * from  #mps_Thirdlevel                            
                            
 Select b.*,'+@intmonthcols_SIOP +','+@intmonthcols_Expectation+','+@intmonthcols_BBP+' into #RevenueDatasmonthwise_Thirdlevel                            
 from  #mps_Thirdlevel b                         
 Left join  #siop_Thirdlevel s                            
 on b.type=s.type                            
 and b.descr=s.descr                            
 left join #bpp_Thirdlevel m                            
 on b.type=m.type                             
 and b.descr=m.descr                            
left join #Expectation_Thirdlevel e                            
 on b.type=e.type                    
 and b.descr=e.descr                            
 ----    select * from #RevenueDatasmonthwise_Thirdlevel                         
Select row_number()over(order by Row ) as ID,''Forecast'' as Module,p.*,'+@intmonthcols_BBP+              
','+@intmonthcols_SIOP +','+@intmonthcols_MpsRevenue+'                 into Thirdlevelgriddata                            
from #TempThirdlevelGridFinaldataInPivot P                            
left join #RevenueDatasmonthwise_Thirdlevel r                            
 On p.Description=r.Type                           
 and p.Type=r.descr                            
                            
 '                           
 Print(@nQuery)                            
 Exec(@nQuery)                            
                         
                            
 Select * from Thirdlevelgriddata                            
 select * from SecondlevelGriddata                    
 select * from Firstlevelgriddata                             
                         
--End                         
                        
                     
drop table if exists Thirdlevelgriddata                            
  drop table if exists SecondlevelGriddata                            
  drop table if exists Firstlevelgriddata                            
                          
                            
drop table if exists #intweek                            
Drop table if exists #TEMPRollingReport_DIOInventoryJobData                            
Drop table if exists #Z_Inventory_RollingReport_LatestMpsRevenueData                            
drop table if exists #TempOrdercategorytype                            
drop table if exists #TempArrangingOrderingdatas                            
drop table if exists  #InputDatasFromUI                            
Drop table if exists #TempTypeCategorywiseData                            
drop table if exists #pivotformat                            
 drop table if exists #TempBPPdata                            
 drop table if exists #TempSIOPdata                            
 drop table if exists #TempForecatMPSdata                            
 drop table if exists #TempExpectationdata                            
drop table if exists  #TempFinaldataInPivot                            
drop table if exists  #bpp                            
drop table if exists  #siop                            
drop table if exists  #mps                            
drop table if exists #RevenueDatasmonthwise                            
drop table if exists #intweek_ActionPlan                            
Drop table if exists #TEMPRollingReport_Actionplan                            
drop table if exists #calendar                            
drop table if exists #Tempcalendar_ActionPlan                            
drop table if exists #TempOrdercategorytype_Action                            
drop table if exists #TempArrangingOrderingdatas_Action                            
drop table if exists  #InputDatasFromUI_Action                            
Drop table if exists #TempTypeCategorywiseData_Action                      
drop table if exists #pivotformat_ActionPlan                            
drop table if exists #Intmonthpivotformat_ActionPlan                            
drop table if exists  #TempFinaldataInPivot_ActionPlan                            
drop table if exists #TempIntmonthlevelInPivot_ActionPlan                            
                            
drop table if exists #TempintmonthforPastShipmentdays_ThiedLevelGrid                            
Drop Table if Exists #TempFirstLevelInventoryandactionPanInventory                            
Drop Table if Exists #FinalDIODataThirdlevelDIOCalc                            
Drop Table if Exists #TempFinalDIOAndInventoryvaluesInPivot                            
                            
drop table if exists  #TempThirdlevelGridFinaldataInPivot                            
drop table if exists  #bpp_Thirdlevel                            
drop table if exists  #siop_Thirdlevel                            
drop table if exists  #mps_Thirdlevel                            
drop table if exists #Expectation_Thirdlevel                            
drop table if exists #RevenueDatasmonthwise_Thirdlevel                            
drop table if exists #intmonthforalignment                            
                            
 drop table if exists #TargetusereditedBBPDIOvalue                            
 drop table if exists #FIlearrivalBBPDIOvalue                            
 drop table if exists #TargetusereditedBBPInventoryvalue              
drop table if exists #FIlearrivalBBPInventoryvalue                            
drop table if exists #TempintmonthforPastShipmentSummaryQuantity                              
  drop table if exists #TempForecatMPSdata_PivotThirdlevel                          
    Drop table if exists #PlantFilterforCommaseperated                        
drop table if exists #TempIntmonthMaxIntweekgrid                          
                        
                        
END     


