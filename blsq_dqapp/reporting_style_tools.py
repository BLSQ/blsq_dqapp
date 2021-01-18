# -*- coding: utf-8 -*-
"""
Created on Tue Jan 12 21:30:41 2021

@author: Fernando-Bluesquare
"""
import numpy as np

##Reviewed Version, for DE
def reporting_style_classifier_function(df,added_de_uid_vars):

    #We assume that the df periods are already sorted in the previous step
    grouped=df.groupby(['OU_UID']+added_de_uid_vars)
    #If all months or none are reported we classify directly the facility
    full=grouped['DE_AVAILABILITY_BOOL'].mean()==1
    never=grouped['DE_AVAILABILITY_BOOL'].mean()==0

    #If a facility has started/stopped reporting at some point it's 
    #DE_AVAILABILITY_BOOL values are monotonic increasing/decreasing, else it's 
    #irregular.
    since_full=grouped.DE_AVAILABILITY_BOOL.is_monotonic_increasing
    since_empty=grouped.DE_AVAILABILITY_BOOL.is_monotonic_decreasing
    summary=np.where(full,'ALWAYS',
                     np.where(never,'NEVER',
                              np.where(since_full,'SINCE_FULL',
                                       np.where(since_empty,'STOPPED',
                                                                    'INCONSISTENT'))))
    # We renamed IRREGULARLY_FULL to inconsistent and since_EMPTY to STOPPED
    df=full.reset_index().assign(COMPLETENESS=summary).drop('DE_AVAILABILITY_BOOL',axis=1)
    return df

def reporting_style_classifier(df,de_uid_vars):
    #de_uid_vars=[col for col in df.columns if col in ['DE_UID','COC_UID'] ]
    ##Sorting of periods on the df
    df=df.sort_values('PERIOD')    
    return reporting_style_classifier_function(df[['OU_UID','DE_AVAILABILITY_BOOL']+de_uid_vars],de_uid_vars)
