# -*- coding: utf-8 -*-
"""
Created on Tue Jan 12 19:45:12 2021

@author: Fernando-Bluesquare
"""
import pandas as pd
import numpy as np
import gc

#Reviewed version

def median_unit_values_reviewed_generation(df,re):
    """""
    Function to generate median values of a DE for an specific OU. 
    
    To avoid mislabeling any outlier as a rightful median value itself we
    avoid assinging a median batch value to an OU if it presents less than 
    three observations in its timeseries and if its median value is an outlier,
    within the context of the whole dataset.
    
    When this happens we label that OU with the median value of medians.
    """""
    
    median_units_values=df.groupby('OU_UID').VALUE.median()
    median_units_values_median=median_units_values.median()
    
    units_values_count=df.groupby('OU_UID').VALUE.count()
    
    median_rs_score=re.fit_transform(median_units_values.values.reshape(-1,1))
    median_rs_score=pd.DataFrame(median_rs_score.reshape(-1,1),index=units_values_count.index)[0].abs()

    scarse_values= (units_values_count<3) & (median_rs_score>=7)
    
    median_units_values=median_units_values.mask(scarse_values,median_units_values_median)
    
    gc.collect()
    
    return median_units_values
    
def df_median_batching_assignment(df,re=None):
    """""
    Function to generate median values of a DE for an specific OU.
    
    We estimate the median per FOSA and group FOSA by ranges of values of width 10: 
    so 0-10,10-20,...etc.
    
    The idea of using a median is right to later locate outliers is correct as
    long as values on a FOSA are expected to be stable around a certain value
    
    The idea behind grouping by median is to compensate the empty months on the 
    units that are partially filled with more point of reference, and avoid 
    extreme sensitivites in normalized values by having small samples of reference   
    """""
    
    #To reduce computations we only proceed to calculate if any data is present.
    if not np.isnan(df.groupby('OU_UID').VALUE.max()):
        
        median_units_values=median_unit_values_reviewed_generation(df,re)
        
        #To adapt to DE value ranges we get the max value to estimate the bins of 
        #data needed
        median_units_values_max=median_units_values.max()
        median_uindex=median_units_values.index
        
        #We assign to each OU the median range value where its values belong
        interval_range = pd.interval_range(start=0, freq=10, end=median_units_values_max)
        batch_median = pd.cut(median_units_values, bins=interval_range).astype(str)
        batch_median=np.where(median_units_values==0,'0',batch_median)

        df=df.join(pd.DataFrame({'batch_median':batch_median},index=median_uindex),on='OU_UID')
        gc.collect()
    else:
        
        df['batch_median']=np.nan
        gc.collect()
    return df


def rs_values_addition(df,re=None):
    """""
    Function to calculate RS values on our data
    """""
    if all(df.batch_median.isna()):
        for col in ['RS_SCORE','EXTREME_RS','OUTLIER_RS','ZERO']:
            df[col]=np.nan
    else:
        #Once we have the data grouped we estimate the outliers following the 
        #previous formula
        df['RS_SCORE']=re.fit_transform(df['VALUE'].values.reshape(-1,1))
    return df


def outliers_rs_based_generator(df):
    """""
    We consider an outlier by calculating the normalized values for the median 
    batch and picking those over 3.5*IQR and tagging as abnormal extreme ( but 
    not outliers) those between 1.5-3.5 IQR
    
    For the case where median is 0 ( this implies that more than half present 
    values are 0) we assume than any value over 30 is unusual. This is a 
    compromise solution so far because we want to detect also cases that are 
    usually empty and suddenly not.
    
    So in this sense we concatenate  series of conditions to form outlier boolean
    conditions.
    
    
    """""
    
    rs_abs=df['RS_SCORE'].abs()
    
    # Later we will use this condition to separate assigment between normal
    # general criterias of median>0 and previously detailed special conditions
    filter_median_positive=df['batch_median']!='0'
    
    na_filter=rs_abs.isna()
    
    filter_extreme_rs_value=(rs_abs >3) & (rs_abs<7)
    filter_outlier_rs_value=rs_abs>=7
    filter_overabs30_value=(df['VALUE'].abs() >30)
    
    df['EXTREME_RS']=np.where(filter_median_positive,
                              filter_extreme_rs_value,
                              (filter_outlier_rs_value) & (~filter_overabs30_value))
    
    df['EXTREME_RS']=np.where(na_filter,np.nan,df['EXTREME_RS'])
    df['EXTREME_RS']=df['EXTREME_RS'].astype('float16')
    
    df['OUTLIER_RS']=np.where(filter_median_positive,
                              filter_outlier_rs_value,
                              filter_overabs30_value)
    
    df['OUTLIER_RS']=np.where(na_filter,np.nan,df['OUTLIER_RS'])
    df['OUTLIER_RS']=df['OUTLIER_RS'].astype('float16')
    
    df['ZERO']=((df['VALUE']==0)).astype('float16')
    df['ZERO']=np.where(na_filter,np.nan,df['ZERO'])

    return df


def outlier_detection_assignment (df,de_uid_vars):
    from sklearn.preprocessing import RobustScaler
    from functools import partial
    re=RobustScaler()
    
    data_with_outliers=df.groupby(de_uid_vars,as_index=False).apply(partial(df_median_batching_assignment,re=re))
    data_with_outliers=data_with_outliers.groupby(['batch_median'],as_index=False).apply(partial(rs_values_addition,re=re))
    data_with_outliers=outliers_rs_based_generator(data_with_outliers)
    gc.collect()
    
    return data_with_outliers


def outlier_detection_handler (df,de_uid_vars,project_path_processed,files_main_name,save_raw_file=True,file_suffix='_with_outliers_raw'):
    #TODO include batching
    #de_uid_vars=[col for col in df.columns if col in ['DE_UID','COC_UID'] ]
    
    data_with_outliers=outlier_detection_assignment (df,de_uid_vars)
    
    #if save_raw_file:
    #    data_with_outliers.to_csv(project_path_processed+files_main_name+file_suffix+'.csv',index=False)
    
    #data_with_outliers_trimmed=data_with_outliers.drop(['batch_median','EXTREME_RS','RS_SCORE'],axis=1)
    #del data_with_outliers
    gc.collect()
    
    return data_with_outliers#data_with_outliers_trimmed
