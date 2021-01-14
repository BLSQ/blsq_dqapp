# -*- coding: utf-8 -*-
"""
Created on Tue Jan 12 21:20:18 2021

@author: Fernando-Bluesquare
"""

def cross_join(df_left,df_right):
    df_crossed=df_left.assign(join=1).merge(df_right.assign(join=1)).drop('join',axis=1)
    return df_crossed


def de_coc_ou_uids_table_generator(dataset_de,dataset_ou):
    #By DE
    return dataset_de.merge(dataset_ou,on=['DS_UID','DS_NAME'])[['DE_UID','COC_UID','OU_UID']].drop_duplicates()

#Notice we're filtering by de present,assuming that the DE is somehwere, this should be erase if we're sure we're taking all DE information
# and not partial extracts

def availability_tree_table_maker(de_coc_ou_uids,period_table):
    return cross_join(de_coc_ou_uids,period_table)



    
def availability_condition_generator(df):
    pass
    
def _availability_condition_direct(df):
    #We calculate the availability value by presence of any VALUE
    df['DE_AVAILABILITY_BOOL']=(~df.VALUE.isna()).astype('uint8')
    return df

def availability_condition_generator_handler(values_df,tree_df,de_uid_vars):
    ##de_uid_vars=[col for col in values_df.columns if col in ['DE_UID','COC_UID'] ]
    #We attached expected to report values
    data_tree_extended=tree_df.merge(values_df,on=['OU_UID','PERIOD']+de_uid_vars,how='left')
    return _availability_condition_direct(data_tree_extended)