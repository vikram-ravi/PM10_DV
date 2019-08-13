# -*- coding: utf-8 -*-
"""
Created on Tue Oct 30 11:36:38 2018

@author: vravi
"""
#%%
# import libs
import pandas as pd
import os
import math
#%%
# define files
workDir = r"T:\ProjectDocs\917106 TO 6 Particulate Matter Analysis Support\Subtask - 1 ArcGIS\design_values_from_epa"
conc_file_2015 = os.path.join(workDir, "annual_conc_by_monitor_2015.csv")
conc_file_2016 = os.path.join(workDir, "annual_conc_by_monitor_2016.csv")
conc_file_2017 = os.path.join(workDir, "annual_conc_by_monitor_2017.csv")
#%%
# read data and get all years in one dataframe, grab PM10 
# data for california
conc_2015 = pd.read_csv(conc_file_2015)
conc_2016 = pd.read_csv(conc_file_2016)
conc_2017 = pd.read_csv(conc_file_2017)
df_conc = pd.concat([conc_2015, conc_2016, conc_2017], ignore_index=True)
df_conc_ca = df_conc[df_conc['State Code'] == 6].infer_objects()
df_pm10 = df_conc_ca[(df_conc_ca['Parameter Name'] == 'PM10 Total 0-10um STP') &
                     #(df_conc_ca['Sample Duration'] != '24-HR BLK AVG') &
                     (df_conc_ca['Metric Used'] == 'Daily Mean')]
#%%
# get site id as a string
state_id_str = lambda x: ('0'*(2-len(str(x))) + str(x))
county_id_str = lambda x: ('0'*(3-len(str(x))) + str(x))
siteNum_id_str = lambda x: ('0'*(4-len(str(x))) + str(x))
df_pm10['siteID'] = df_pm10['State Code'].apply(state_id_str) + \
                    df_pm10['County Code'].apply(county_id_str) + \
                    df_pm10['Site Num'].apply(siteNum_id_str)
#%%
value_cols = ['1st Max Value', '2nd Max Value', 
              '3rd Max Value', '4th Max Value']
def round_to_nearestXten(x):
    if x%10 < 5:
        return int(x-x%10)
    elif x%10 >= 5:
        return int(x-x%10 + 10)
    
def get_dv(df_in):
    df = df_in.copy(deep=True)
    dict_keys = ['Latitude', 'Longitude', 'LocalSiteName', 'POC', 
                 'County','Concentration', 'Completeness_Indicator',
                 'Total_Obs_Count']
    df_out = pd.DataFrame(columns=dict_keys)
    site_id = df.siteID.unique()[0]
    site_lat = df.Latitude.unique()[0]
    site_lon = df.Longitude.unique()[0]
    site_name = df['Local Site Name'].unique()[0]
    county_name = df['County Name'].unique()[0]
    poc_list = list(set(df.POC))
    
    dv_poc = {k1:{k2:None for k2 in dict_keys} for k1 in set(df.siteID)}

    for poc in poc_list:
        conc_values = []
        completeness = []
        #print (site_id, poc)
        dv_poc[site_id]['County'] = county_name
        dv_poc[site_id]['Latitude'] = site_lat
        dv_poc[site_id]['Longitude'] = site_lon
        dv_poc[site_id]['LocalSiteName'] = site_name
        
        df_poc = df[(df.POC == poc) & 
                    ((df['Event Type'] == 'No Events') | 
                     (df['Event Type'] == 'Concurred Events Excluded'))]
        if df_poc.empty: 
            df_poc = df[( df.POC == poc) & 
                        ((df['Event Type'] == 'No Events') | 
                         (df['Event Type'] == 'Events Inclucded'))]
            if df_poc.empty: break
        #if sorted(list(set(df_poc.Year))) != [2015, 2016, 2017]: break
        #obs_sum = sum(df_poc['Valid Day Count'])
        obs_sum = sum(df_poc['Observation Count'])
        completeness.extend(df_poc['Completeness Indicator'].values.tolist())
        #print (completeness)
        for v_col in value_cols:
            conc_values.extend(df_poc[v_col].values.tolist())
        
        # sort and get the design value
        conc_values = sorted([x for x in conc_values if not math.isnan(x)])
        if obs_sum >= 1043:
            dv = conc_values[len(conc_values)-4]
        elif obs_sum >= 696:
            dv = conc_values[len(conc_values)-3]
        elif obs_sum >= 348:
            dv = conc_values[len(conc_values)-2]
        elif obs_sum <= 347:
            dv = conc_values[len(conc_values)-1]
        dv_poc[site_id]['POC'] = poc
        dv_poc[site_id]['Total_Obs_Count'] = obs_sum
        dv_poc[site_id]['Concentration'] = dv#round_to_nearestXten(dv)
        get_completeness = lambda x: (all([item == 'Y' for item in x]) if len(x)==3 else False)
        dv_poc[site_id]['Completeness_Indicator'] = all([item == 'Y' for item in completeness])
        dv_poc[site_id]['Completeness_Indicator'] = get_completeness(completeness)
        if len(completeness)<2: print (site_id, poc)
        df_out = pd.concat([df_out,pd.DataFrame(dv_poc).transpose()], sort=False)
    return df_out

#%%
df_columns=['POC', 'County','Latitude', 'Longitude', 'LocalSiteName', 
            'Completeness_Indicator','Concentration', 'Total_Obs_Count']
df_dv = pd.DataFrame()
for site_id in set(df_pm10.siteID):
    df_site = df_pm10[df_pm10.siteID==site_id]
    df_site_dv = get_dv(df_site)
    df_dv = pd.concat([df_dv, df_site_dv])

df_dv = df_dv[df_columns]
df_dv = df_dv.infer_objects().sort_index()
df_dv.Completeness_Indicator = df_dv.Completeness_Indicator.apply(lambda x: ('Yes' if x==1 else 'No'))
#df_dv.to_excel(os.path.join(workDir,'pm10_background_2015_2017.xlsx'))
df_dv_above_naaqs = df_dv.where((df_dv.Concentration>=150)).dropna(how='all') # & (df_dv.Completeness_Indicator=='Yes')).dropna(how='all')    
