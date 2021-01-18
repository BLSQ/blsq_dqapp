# -*- coding: utf-8 -*-
"""
Created on Tue Jan 12 21:20:18 2021

@author: Fernando-Bluesquare
"""

def cross_join(df_left,df_right):
    df_crossed=df_left.assign(join=1).merge(df_right.assign(join=1)).drop('join',axis=1)
    return df_crossed


def de_coc_ou_uids_table_generator(dataset_ou,de_df):
    return dataset_ou.merge(de_df,on=['DS_UID'])[['DE_UID','COC_UID','OU_UID']].drop_duplicates()

#Notice we're filtering by de present,assuming that the DE is somewhere, this should be erase if we're sure we're taking all DE information
# and not partial extracts

def availability_tree_table_maker(dataset_ou,de_df,period_table):
    return cross_join(de_coc_ou_uids_table_generator(dataset_ou,de_df),period_table)

def de_ou_coverage_dict(dataset_ou,de_df,ou_tree_df)
    #Using the late table we obtain a table that determines the belonging of an DE for a OU
    ou_de_full_table=cross_join(org_unit_tree[['OU_UID']],de_coc_ou_uids[['DE_UID','COC_UID']].drop_duplicates())
    ou_de_full_table=ou_de_full_table.merge(de_coc_ou_uids.assign(DE_BELONG=1),how='left',on=['OU_UID', 'DE_UID', 'COC_UID']).fillna(0)
    de_ou_coverage=ou_de_full_table.groupby('DE_UID').mean().to_dict()['DE_BELONG']
    return de_ou_coverage

    
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