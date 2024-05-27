## Import Liberaries
import pandas as pd
import sqlite3

## Read the files
df = pd.read_csv(R"C:\Users\LENOVO\Downloads\sql-project\Latest Covid-19 India Status.csv")
print(df.head())
df_copy= df 
df_copy.to_csv('CovidNinteenIndia.csv',index=False)

## Read the Renamed FIle
df_short_name = pd.read_csv('CovidNinteenIndia.csv')
print(df_short_name.head())

## Let's Rename the titles to simple forms as it will simplify query writing

df_SQL = df_short_name.rename(columns = {'State/UTs': 'StateNUTs',
                          'Total Cases': 'TotalCases',
                          'Active Ratio' :'activeratio',
                           'Active' :'Active',
                            'Discharged' :'Discharged',
                             'Discharge Ratio':'DischargeRatio',
                              'Death Ratio':'DeathRatio',
                              }, inplace = False)
print(df_SQL.head())

# import sqlalchemy and create a sqlite engine
from sqlalchemy import create_engine
engine = create_engine('sqlite://', echo=False)

# export the dataframe as a table 'playstore' to the sqlite engine
df_SQL.to_sql("CovidNinteenIndia", con =engine)

df_SQL.to_sql('CovidNinteenIndia', con=engine, if_exists='append', index=False)
print(df_SQL.head())

## Testing the code by select the complete table

sql='''

Select * from CovidNinteenIndia


''';
    
df_sql = pd.read_sql_query(sql,con=engine)
print(df_sql.head())

## Query 1:- Select the states and UTs available for the data analysis

sql='''

Select statenuts from CovidNinteenIndia


''';

    
df_sql = pd.read_sql_query(sql,con=engine)
print(df_sql.head(100))

## Query 2:- states with most active cases

sql='''

Select 
statenuts as State_UTs,
active as Active_Case
from CovidNinteenIndia
group by statenuts
order by active desc
limit 10

''';

    
df_sql = pd.read_sql_query(sql,con=engine)
print(df_sql)


## Query 3:- states with successful discharges

sql='''

Select 
statenuts as State_UTs,
discharged as Discharge_Case
from CovidNinteenIndia
group by statenuts
order by discharged desc
limit 10

''';

    
df_sql = pd.read_sql_query(sql,con=engine)
print(df_sql)

## Query 4:- states with most 10 deaths

sql='''

Select 
statenuts as State_UTs,
deaths as Deaths
from CovidNinteenIndia
group by statenuts
order by deaths desc
limit 10

''';

    
df_sql = pd.read_sql_query(sql,con=engine)
print(df_sql)

## Query 5:- Compute the Discharge Rate

##Note:- The colums is already shared in the input data, the below query is written for demonstrative use of the round off function, the calculated results are check and verifed with the inputs results.

sql='''

select * from (
Select 
statenuts as State_UTs,
discharged as Discharged_Cases,
totalcases as Total_Cases,
round(100*(round(discharged,2)/round(totalcases,2)),2) as Discharge_Ratio_Calculated
from CovidNinteenIndia
group by statenuts
order by deaths desc
limit 10
) a

order by a.discharge_ratio_Calculated desc
''';

    
df_sql = pd.read_sql_query(sql,con=engine)
print(df_sql)

## Query 6:- Max Total_Case vs Min Total_Case


sql='''

select * from (
select 'The State with Minimum Cases is   :-  ' || statenuts as State, min(totalcases) as Max_Min_Cases from CovidNinteenIndia
union all
select'The State with Maximum Cases is   :-  ' || statenuts as State, max(totalcases) as Max_Min_Cases from CovidNinteenIndia
)a

order by a.Max_Min_Cases desc

''';

    
df_sql = pd.read_sql_query(sql,con=engine)
print(df_sql)

## Query 7:- Max Death vs Min Death


sql='''

select * from (
select 'The State with Minimum Deaths is   :-  ' || statenuts as State, min(deaths) as Max_Min_Deaths from CovidNinteenIndia
union all
select 'The State with Maximum Deaths is  :-  ' || statenuts as State, max(deaths) as Max_Min_Deaths from CovidNinteenIndia
)a
order by a.Max_Min_Deaths desc
''';

    
df_sql = pd.read_sql_query(sql,con=engine)
print(df_sql)

## Query 8:- Max Active Cases vs Min Active Cases


sql='''

select * from (
select 'The State with Minimum Active Cases is   :-  ' || statenuts as State, min(active) as Max_Min_Active_Cases from CovidNinteenIndia
union all
select 'The State with Maximum Active Cases A is  :-  ' || statenuts as State, max(active) as Max_Min_Active_Cases from CovidNinteenIndia
)a
order by a.Max_Min_Active_Cases desc
''';

    
df_sql = pd.read_sql_query(sql,con=engine)
print(df_sql)

## Query 9:- Death Ratio South India vs North India


sql= '''

select 'South India is ' || round(avg(DeathRatio),2) as Avg_Death_Rate from CovidNinteenIndia
where statenuts in ('Tamil Nadu','Puducherry','Telengana','Andhra Pradesh','Karnataka','Kerala')
union all
select 'North India is ' || round(avg(DeathRatio),2) as Avg_Death_Rate from CovidNinteenIndia
where statenuts not in ('Tamil Nadu','Puducherry','Telengana','Andhra Pradesh','Karnataka','Kerala')

''';

    
df_sql = pd.read_sql_query(sql,con=engine)
print(df_sql)

## Query 10:- Active Ratio South India vs North India


sql= '''

select 'South India is ' || round(avg(activeRatio),2) as Avg_active_Rate from CovidNinteenIndia
where statenuts in ('Tamil Nadu','Puducherry','Telengana','Andhra Pradesh','Karnataka','Kerala')
union all
select 'North India is ' || round(avg(activeRatio),2) as Avg_active_Rate from CovidNinteenIndia
where statenuts not in ('Tamil Nadu','Puducherry','Telengana','Andhra Pradesh','Karnataka','Kerala')

''';

    
df_sql = pd.read_sql_query(sql,con=engine)
print(df_sql)   

## Query 11:- Death Ratio South India vs North India


sql= '''

select 'South India is ' || round(avg(dischargeRatio),2) as Avg_Death_Rate from CovidNinteenIndia
where statenuts in ('Tamil Nadu','Puducherry','Telengana','Andhra Pradesh','Karnataka','Kerala')
union all
select 'North India is ' || round(avg(dischargeRatio),2) as Avg_Death_Rate from CovidNinteenIndia
where statenuts not in ('Tamil Nadu','Puducherry','Telengana','Andhra Pradesh','Karnataka','Kerala')

''';

    
df_sql = pd.read_sql_query(sql,con=engine)
print(df_sql)

## Query 12:- Comparision of Active Rate KPIs for all the states

KPI1='AR'## Active Rate
KPI2='activeratio' ## KPI value

sql= f'''
select
max(case when a.statenuts='Andaman and Nicobar' then {KPI2} else 0 end) as {KPI1}_AndamanNNicobar,
max(case when a.statenuts='Andhra Pradesh' then {KPI2} else 0 end) as {KPI1}_AndhraPradesh,
max(case when a.statenuts='Assam' then {KPI2} else 0 end) as {KPI1}_Assam,
max(case when a.statenuts='Bihar' then {KPI2} else 0 end) as {KPI1}_Bihar,
max(case when a.statenuts='Chandigarh' then {KPI2} else 0 end) as {KPI1}_Chandigarh,
max(case when a.statenuts='Chhattisgarh' then {KPI2} else 0 end) as {KPI1}_Chhattisgarh,
max(case when a.statenuts='Dadra and Nagar Haveli and Daman and Diu' then {KPI2} else 0 end) as {KPI1}_DadraandNagarHaveliandDamanandDiu,
max(case when a.statenuts='Delhi' then {KPI2} else 0 end) as {KPI1}_Delhi,
max(case when a.statenuts='Goa' then {KPI2} else 0 end) as {KPI1}_Goa,
max(case when a.statenuts='Gujarat' then {KPI2} else 0 end) as {KPI1}_Gujarat,
max(case when a.statenuts='Haryana' then {KPI2} else 0 end) as {KPI1}_Haryana,
max(case when a.statenuts='Himachal Pradesh' then {KPI2} else 0 end) as {KPI1}_HimachalPradesh,
max(case when a.statenuts='Jammu and Kashmir' then {KPI2} else 0 end) as {KPI1}_JammuandKashmir,
max(case when a.statenuts='Jharkhand' then {KPI2} else 0 end) as {KPI1}_Jharkhand,
max(case when a.statenuts='Karnataka' then {KPI2} else 0 end) as {KPI1}_Karnataka,
max(case when a.statenuts='Kerala' then {KPI2} else 0 end) as {KPI1}_Kerala,
max(case when a.statenuts='Ladakh' then {KPI2} else 0 end) as {KPI1}_Ladakh,
max(case when a.statenuts='Lakshadweep' then {KPI2} else 0 end) as {KPI1}_Lakshadweep,
max(case when a.statenuts='Madhya Pradesh' then {KPI2} else 0 end) as {KPI1}_MadhyaPradesh,
max(case when a.statenuts='Maharashtra' then {KPI2} else 0 end) as {KPI1}_Maharashtra,
max(case when a.statenuts='Manipur' then {KPI2} else 0 end) as {KPI1}_Manipur,
max(case when a.statenuts='Meghalaya' then {KPI2} else 0 end) as {KPI1}_Meghalaya,
max(case when a.statenuts='Mizoram' then {KPI2} else 0 end) as {KPI1}_Mizoram,
max(case when a.statenuts='Nagaland' then {KPI2} else 0 end) as {KPI1}_Nagaland,
max(case when a.statenuts='Odisha' then {KPI2} else 0 end) as {KPI1}_Odisha,
max(case when a.statenuts='Puducherry' then {KPI2} else 0 end) as {KPI1}_Puducherry,
max(case when a.statenuts='Punjab' then {KPI2} else 0 end) as {KPI1}_Punjab,
max(case when a.statenuts='Rajasthan' then {KPI2} else 0 end) as {KPI1}_Rajasthan,
max(case when a.statenuts='Sikkim' then {KPI2} else 0 end) as {KPI1}_Sikkim,
max(case when a.statenuts='Tamil Nadu' then {KPI2} else 0 end) as {KPI1}_TamilNadu,
max(case when a.statenuts='Telengana' then {KPI2} else 0 end) as {KPI1}_Telengana,
max(case when a.statenuts='Tripura' then {KPI2} else 0 end) as {KPI1}_Tripura,
max(case when a.statenuts='Uttar Pradesh' then {KPI2} else 0 end) as {KPI1}_UttarPradesh,
max(case when a.statenuts='Uttarakhand' then {KPI2} else 0 end) as {KPI1}_Uttarakhand,
max(case when a.statenuts='West Bengal' then {KPI2} else 0 end) as {KPI1}_WestBengal
from
(select statenuts,activeratio,deathratio,dischargeratio
from CovidNinteenIndia) a

''';

    
df_sql = pd.read_sql_query(sql,con=engine)
print(df_sql)


## Query 13:- Comparision of Discharge Ratio for all the states

KPI1='DizchR'## Death Rate
KPI2='dischargeratio' ## KPI value

sql= f'''
select
max(case when a.statenuts='Andaman and Nicobar' then {KPI2} else 0 end) as {KPI1}_AndamanNNicobar,
max(case when a.statenuts='Andhra Pradesh' then {KPI2} else 0 end) as {KPI1}_AndhraPradesh,
max(case when a.statenuts='Assam' then {KPI2} else 0 end) as {KPI1}_Assam,
max(case when a.statenuts='Bihar' then {KPI2} else 0 end) as {KPI1}_Bihar,
max(case when a.statenuts='Chandigarh' then {KPI2} else 0 end) as {KPI1}_Chandigarh,
max(case when a.statenuts='Chhattisgarh' then {KPI2} else 0 end) as {KPI1}_Chhattisgarh,
max(case when a.statenuts='Dadra and Nagar Haveli and Daman and Diu' then {KPI2} else 0 end) as {KPI1}_DadraandNagarHaveliandDamanandDiu,
max(case when a.statenuts='Delhi' then {KPI2} else 0 end) as {KPI1}_Delhi,
max(case when a.statenuts='Goa' then {KPI2} else 0 end) as {KPI1}_Goa,
max(case when a.statenuts='Gujarat' then {KPI2} else 0 end) as {KPI1}_Gujarat,
max(case when a.statenuts='Haryana' then {KPI2} else 0 end) as {KPI1}_Haryana,
max(case when a.statenuts='Himachal Pradesh' then {KPI2} else 0 end) as {KPI1}_HimachalPradesh,
max(case when a.statenuts='Jammu and Kashmir' then {KPI2} else 0 end) as {KPI1}_JammuandKashmir,
max(case when a.statenuts='Jharkhand' then {KPI2} else 0 end) as {KPI1}_Jharkhand,
max(case when a.statenuts='Karnataka' then {KPI2} else 0 end) as {KPI1}_Karnataka,
max(case when a.statenuts='Kerala' then {KPI2} else 0 end) as {KPI1}_Kerala,
max(case when a.statenuts='Ladakh' then {KPI2} else 0 end) as {KPI1}_Ladakh,
max(case when a.statenuts='Lakshadweep' then {KPI2} else 0 end) as {KPI1}_Lakshadweep,
max(case when a.statenuts='Madhya Pradesh' then {KPI2} else 0 end) as {KPI1}_MadhyaPradesh,
max(case when a.statenuts='Maharashtra' then {KPI2} else 0 end) as {KPI1}_Maharashtra,
max(case when a.statenuts='Manipur' then {KPI2} else 0 end) as {KPI1}_Manipur,
max(case when a.statenuts='Meghalaya' then {KPI2} else 0 end) as {KPI1}_Meghalaya,
max(case when a.statenuts='Mizoram' then {KPI2} else 0 end) as {KPI1}_Mizoram,
max(case when a.statenuts='Nagaland' then {KPI2} else 0 end) as {KPI1}_Nagaland,
max(case when a.statenuts='Odisha' then {KPI2} else 0 end) as {KPI1}_Odisha,
max(case when a.statenuts='Puducherry' then {KPI2} else 0 end) as {KPI1}_Puducherry,
max(case when a.statenuts='Punjab' then {KPI2} else 0 end) as {KPI1}_Punjab,
max(case when a.statenuts='Rajasthan' then {KPI2} else 0 end) as {KPI1}_Rajasthan,
max(case when a.statenuts='Sikkim' then {KPI2} else 0 end) as {KPI1}_Sikkim,
max(case when a.statenuts='Tamil Nadu' then {KPI2} else 0 end) as {KPI1}_TamilNadu,
max(case when a.statenuts='Telengana' then {KPI2} else 0 end) as {KPI1}_Telengana,
max(case when a.statenuts='Tripura' then {KPI2} else 0 end) as {KPI1}_Tripura,
max(case when a.statenuts='Uttar Pradesh' then {KPI2} else 0 end) as {KPI1}_UttarPradesh,
max(case when a.statenuts='Uttarakhand' then {KPI2} else 0 end) as {KPI1}_Uttarakhand,
max(case when a.statenuts='West Bengal' then {KPI2} else 0 end) as {KPI1}_WestBengal
from
(select statenuts,activeratio,deathratio,dischargeratio
from CovidNinteenIndia) a

''';

    
df_sql = pd.read_sql_query(sql,con=engine)
print(df_sql)

## Query 14:- Comparision of Death Rate KPIs for all the states

KPI1='DR'## Death Rate
KPI2='deathratio' ## KPI value

sql= f'''
select
max(case when a.statenuts='Andaman and Nicobar' then {KPI2} else 0 end) as {KPI1}_AndamanNNicobar,
max(case when a.statenuts='Andhra Pradesh' then {KPI2} else 0 end) as {KPI1}_AndhraPradesh,
max(case when a.statenuts='Assam' then {KPI2} else 0 end) as {KPI1}_Assam,
max(case when a.statenuts='Bihar' then {KPI2} else 0 end) as {KPI1}_Bihar,
max(case when a.statenuts='Chandigarh' then {KPI2} else 0 end) as {KPI1}_Chandigarh,
max(case when a.statenuts='Chhattisgarh' then {KPI2} else 0 end) as {KPI1}_Chhattisgarh,
max(case when a.statenuts='Dadra and Nagar Haveli and Daman and Diu' then {KPI2} else 0 end) as {KPI1}_DadraandNagarHaveliandDamanandDiu,
max(case when a.statenuts='Delhi' then {KPI2} else 0 end) as {KPI1}_Delhi,
max(case when a.statenuts='Goa' then {KPI2} else 0 end) as {KPI1}_Goa,
max(case when a.statenuts='Gujarat' then {KPI2} else 0 end) as {KPI1}_Gujarat,
max(case when a.statenuts='Haryana' then {KPI2} else 0 end) as {KPI1}_Haryana,
max(case when a.statenuts='Himachal Pradesh' then {KPI2} else 0 end) as {KPI1}_HimachalPradesh,
max(case when a.statenuts='Jammu and Kashmir' then {KPI2} else 0 end) as {KPI1}_JammuandKashmir,
max(case when a.statenuts='Jharkhand' then {KPI2} else 0 end) as {KPI1}_Jharkhand,
max(case when a.statenuts='Karnataka' then {KPI2} else 0 end) as {KPI1}_Karnataka,
max(case when a.statenuts='Kerala' then {KPI2} else 0 end) as {KPI1}_Kerala,
max(case when a.statenuts='Ladakh' then {KPI2} else 0 end) as {KPI1}_Ladakh,
max(case when a.statenuts='Lakshadweep' then {KPI2} else 0 end) as {KPI1}_Lakshadweep,
max(case when a.statenuts='Madhya Pradesh' then {KPI2} else 0 end) as {KPI1}_MadhyaPradesh,
max(case when a.statenuts='Maharashtra' then {KPI2} else 0 end) as {KPI1}_Maharashtra,
max(case when a.statenuts='Manipur' then {KPI2} else 0 end) as {KPI1}_Manipur,
max(case when a.statenuts='Meghalaya' then {KPI2} else 0 end) as {KPI1}_Meghalaya,
max(case when a.statenuts='Mizoram' then {KPI2} else 0 end) as {KPI1}_Mizoram,
max(case when a.statenuts='Nagaland' then {KPI2} else 0 end) as {KPI1}_Nagaland,
max(case when a.statenuts='Odisha' then {KPI2} else 0 end) as {KPI1}_Odisha,
max(case when a.statenuts='Puducherry' then {KPI2} else 0 end) as {KPI1}_Puducherry,
max(case when a.statenuts='Punjab' then {KPI2} else 0 end) as {KPI1}_Punjab,
max(case when a.statenuts='Rajasthan' then {KPI2} else 0 end) as {KPI1}_Rajasthan,
max(case when a.statenuts='Sikkim' then {KPI2} else 0 end) as {KPI1}_Sikkim,
max(case when a.statenuts='Tamil Nadu' then {KPI2} else 0 end) as {KPI1}_TamilNadu,
max(case when a.statenuts='Telengana' then {KPI2} else 0 end) as {KPI1}_Telengana,
max(case when a.statenuts='Tripura' then {KPI2} else 0 end) as {KPI1}_Tripura,
max(case when a.statenuts='Uttar Pradesh' then {KPI2} else 0 end) as {KPI1}_UttarPradesh,
max(case when a.statenuts='Uttarakhand' then {KPI2} else 0 end) as {KPI1}_Uttarakhand,
max(case when a.statenuts='West Bengal' then {KPI2} else 0 end) as {KPI1}_WestBengal
from
(select statenuts,activeratio,deathratio,dischargeratio
from CovidNinteenIndia) a

''';

    
df_sql = pd.read_sql_query(sql,con=engine)
print(df_sql)

## Query 15:- All the key KPIs combine in 1 table

KPI1='AR'## Active Rate
KPI2='activeratio' ## KPI value
KPI3='deathratio'
KPI4='dischargeratio'

sql= f'''
select * from (
select
max(case when a.statenuts='Andaman and Nicobar' then {KPI2} else 0 end) as AndamanNNicobar,
max(case when a.statenuts='Andhra Pradesh' then {KPI2} else 0 end) as AndhraPradesh,
max(case when a.statenuts='Assam' then {KPI2} else 0 end) as Assam,
max(case when a.statenuts='Bihar' then {KPI2} else 0 end) as Bihar,
max(case when a.statenuts='Chandigarh' then {KPI2} else 0 end) as Chandigarh,
max(case when a.statenuts='Chhattisgarh' then {KPI2} else 0 end) as Chhattisgarh,
max(case when a.statenuts='Dadra and Nagar Haveli and Daman and Diu' then {KPI2} else 0 end) as DadraandNagarHaveliandDamanandDiu,
max(case when a.statenuts='Delhi' then {KPI2} else 0 end) as delhi,
max(case when a.statenuts='Goa' then {KPI2} else 0 end) as Goa,
max(case when a.statenuts='Gujarat' then {KPI2} else 0 end) as Gujarat,
max(case when a.statenuts='Haryana' then {KPI2} else 0 end) as Haryana,
max(case when a.statenuts='Himachal Pradesh' then {KPI2} else 0 end) as HimachalPradesh,
max(case when a.statenuts='Jammu and Kashmir' then {KPI2} else 0 end) as JammuandKashmir,
max(case when a.statenuts='Jharkhand' then {KPI2} else 0 end) as Jharkhand,
max(case when a.statenuts='Karnataka' then {KPI2} else 0 end) as Karnataka,
max(case when a.statenuts='Kerala' then {KPI2} else 0 end) as Kerala,
max(case when a.statenuts='Ladakh' then {KPI2} else 0 end) as Ladakh,
max(case when a.statenuts='Lakshadweep' then {KPI2} else 0 end) as Lakshadweep,
max(case when a.statenuts='Madhya Pradesh' then {KPI2} else 0 end) as MadhyaPradesh,
max(case when a.statenuts='Maharashtra' then {KPI2} else 0 end) as Maharashtra,
max(case when a.statenuts='Manipur' then {KPI2} else 0 end) as Manipur,
max(case when a.statenuts='Meghalaya' then {KPI2} else 0 end) as Meghalaya,
max(case when a.statenuts='Mizoram' then {KPI2} else 0 end) as Mizoram,
max(case when a.statenuts='Nagaland' then {KPI2} else 0 end) as Nagaland,
max(case when a.statenuts='Odisha' then {KPI2} else 0 end) as Odisha,
max(case when a.statenuts='Puducherry' then {KPI2} else 0 end) as Puducherry,
max(case when a.statenuts='Punjab' then {KPI2} else 0 end) as Punjab,
max(case when a.statenuts='Rajasthan' then {KPI2} else 0 end) as Rajasthan,
max(case when a.statenuts='Sikkim' then {KPI2} else 0 end) as Sikkim,
max(case when a.statenuts='Tamil Nadu' then {KPI2} else 0 end) as TamilNadu,
max(case when a.statenuts='Telengana' then {KPI2} else 0 end) as Telengana,
max(case when a.statenuts='Tripura' then {KPI2} else 0 end) as Tripura,
max(case when a.statenuts='Uttar Pradesh' then {KPI2} else 0 end) as UttarPradesh,
max(case when a.statenuts='Uttarakhand' then {KPI2} else 0 end) as Uttarakhand,
max(case when a.statenuts='West Bengal' then {KPI2} else 0 end) as WestBengal
from
(select statenuts,activeratio,deathratio,dischargeratio
from CovidNinteenIndia) a ) active_rate_table
union all
select * from (
select
max(case when a.statenuts='Andaman and Nicobar' then {KPI3} else 0 end) as AndamanNNicobar,
max(case when a.statenuts='Andhra Pradesh' then {KPI3} else 0 end) as AndhraPradesh,
max(case when a.statenuts='Assam' then {KPI3} else 0 end) as Assam,
max(case when a.statenuts='Bihar' then {KPI3} else 0 end) as Bihar,
max(case when a.statenuts='Chandigarh' then {KPI3} else 0 end) as Chandigarh,
max(case when a.statenuts='Chhattisgarh' then {KPI3} else 0 end) as Chhattisgarh,
max(case when a.statenuts='Dadra and Nagar Haveli and Daman and Diu' then {KPI3} else 0 end) as DadraandNagarHaveliandDamanandDiu,
max(case when a.statenuts='Delhi' then {KPI3} else 0 end) as delhi,
max(case when a.statenuts='Goa' then {KPI3} else 0 end) as Goa,
max(case when a.statenuts='Gujarat' then {KPI3} else 0 end) as Gujarat,
max(case when a.statenuts='Haryana' then {KPI3} else 0 end) as Haryana,
max(case when a.statenuts='Himachal Pradesh' then {KPI3} else 0 end) as HimachalPradesh,
max(case when a.statenuts='Jammu and Kashmir' then {KPI3} else 0 end) as JammuandKashmir,
max(case when a.statenuts='Jharkhand' then {KPI3} else 0 end) as Jharkhand,
max(case when a.statenuts='Karnataka' then {KPI3} else 0 end) as Karnataka,
max(case when a.statenuts='Kerala' then {KPI3} else 0 end) as Kerala,
max(case when a.statenuts='Ladakh' then {KPI3} else 0 end) as Ladakh,
max(case when a.statenuts='Lakshadweep' then {KPI3} else 0 end) as Lakshadweep,
max(case when a.statenuts='Madhya Pradesh' then {KPI3} else 0 end) as MadhyaPradesh,
max(case when a.statenuts='Maharashtra' then {KPI3} else 0 end) as Maharashtra,
max(case when a.statenuts='Manipur' then {KPI3} else 0 end) as Manipur,
max(case when a.statenuts='Meghalaya' then {KPI3} else 0 end) as Meghalaya,
max(case when a.statenuts='Mizoram' then {KPI3} else 0 end) as Mizoram,
max(case when a.statenuts='Nagaland' then {KPI3} else 0 end) as Nagaland,
max(case when a.statenuts='Odisha' then {KPI3} else 0 end) as Odisha,
max(case when a.statenuts='Puducherry' then {KPI3} else 0 end) as Puducherry,
max(case when a.statenuts='Punjab' then {KPI3} else 0 end) as Punjab,
max(case when a.statenuts='Rajasthan' then {KPI3} else 0 end) as Rajasthan,
max(case when a.statenuts='Sikkim' then {KPI3} else 0 end) as Sikkim,
max(case when a.statenuts='Tamil Nadu' then {KPI3} else 0 end) as TamilNadu,
max(case when a.statenuts='Telengana' then {KPI3} else 0 end) as Telengana,
max(case when a.statenuts='Tripura' then {KPI3} else 0 end) as Tripura,
max(case when a.statenuts='Uttar Pradesh' then {KPI3} else 0 end) as UttarPradesh,
max(case when a.statenuts='Uttarakhand' then {KPI3} else 0 end) as Uttarakhand,
max(case when a.statenuts='West Bengal' then {KPI3} else 0 end) as WestBengal
from
(select statenuts,activeratio,deathratio,dischargeratio
from CovidNinteenIndia) a ) death_rate_table
union all
select * from (
select
max(case when a.statenuts='Andaman and Nicobar' then {KPI4} else 0 end) as AndamanNNicobar,
max(case when a.statenuts='Andhra Pradesh' then {KPI4} else 0 end) as AndhraPradesh,
max(case when a.statenuts='Assam' then {KPI4} else 0 end) as Assam,
max(case when a.statenuts='Bihar' then {KPI4} else 0 end) as Bihar,
max(case when a.statenuts='Chandigarh' then {KPI4} else 0 end) as Chandigarh,
max(case when a.statenuts='Chhattisgarh' then {KPI4} else 0 end) as Chhattisgarh,
max(case when a.statenuts='Dadra and Nagar Haveli and Daman and Diu' then {KPI4} else 0 end) as DadraandNagarHaveliandDamanandDiu,
max(case when a.statenuts='Delhi' then {KPI4} else 0 end) as delhi,
max(case when a.statenuts='Goa' then {KPI4} else 0 end) as Goa,
max(case when a.statenuts='Gujarat' then {KPI4} else 0 end) as Gujarat,
max(case when a.statenuts='Haryana' then {KPI4} else 0 end) as Haryana,
max(case when a.statenuts='Himachal Pradesh' then {KPI4} else 0 end) as HimachalPradesh,
max(case when a.statenuts='Jammu and Kashmir' then {KPI4} else 0 end) as JammuandKashmir,
max(case when a.statenuts='Jharkhand' then {KPI4} else 0 end) as Jharkhand,
max(case when a.statenuts='Karnataka' then {KPI4} else 0 end) as Karnataka,
max(case when a.statenuts='Kerala' then {KPI4} else 0 end) as Kerala,
max(case when a.statenuts='Ladakh' then {KPI4} else 0 end) as Ladakh,
max(case when a.statenuts='Lakshadweep' then {KPI4} else 0 end) as Lakshadweep,
max(case when a.statenuts='Madhya Pradesh' then {KPI4} else 0 end) as MadhyaPradesh,
max(case when a.statenuts='Maharashtra' then {KPI4} else 0 end) as Maharashtra,
max(case when a.statenuts='Manipur' then {KPI4} else 0 end) as Manipur,
max(case when a.statenuts='Meghalaya' then {KPI4} else 0 end) as Meghalaya,
max(case when a.statenuts='Mizoram' then {KPI4} else 0 end) as Mizoram,
max(case when a.statenuts='Nagaland' then {KPI4} else 0 end) as Nagaland,
max(case when a.statenuts='Odisha' then {KPI4} else 0 end) as Odisha,
max(case when a.statenuts='Puducherry' then {KPI4} else 0 end) as Puducherry,
max(case when a.statenuts='Punjab' then {KPI4} else 0 end) as Punjab,
max(case when a.statenuts='Rajasthan' then {KPI4} else 0 end) as Rajasthan,
max(case when a.statenuts='Sikkim' then {KPI4} else 0 end) as Sikkim,
max(case when a.statenuts='Tamil Nadu' then {KPI4} else 0 end) as TamilNadu,
max(case when a.statenuts='Telengana' then {KPI4} else 0 end) as Telengana,
max(case when a.statenuts='Tripura' then {KPI4} else 0 end) as Tripura,
max(case when a.statenuts='Uttar Pradesh' then {KPI4} else 0 end) as UttarPradesh,
max(case when a.statenuts='Uttarakhand' then {KPI4} else 0 end) as Uttarakhand,
max(case when a.statenuts='West Bengal' then {KPI4} else 0 end) as WestBengal
from
(select statenuts,activeratio,deathratio,dischargeratio
from CovidNinteenIndia) a ) discharge_rate_table



''';

    
df_sql = pd.read_sql_query(sql,con=engine)
print('The KPIs are printed as \n 0:-Active Rate \n 1:- Death Rate \n 2:- Discharge Rate')
print(df_sql)






