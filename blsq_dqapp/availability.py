# -*- coding: utf-8 -*-
"""
Created on Tue Jan 12 21:20:18 2021

@author: Fernando-Bluesquare
"""
from functools import partial

def cross_join(df_left,df_right):
    df_crossed=df_left.assign(join=1).merge(df_right.assign(join=1)).drop('join',axis=1)
    return df_crossed


def de_coc_ou_uids_table_generator(dataset_ou,de_df):
    return dataset_ou.merge(de_df,on=['DS_UID'])[['DE_UID','COC_UID','OU_UID']].drop_duplicates()

#Notice we're filtering by de present,assuming that the DE is somewhere, this should be erase if we're sure we're taking all DE information
# and not partial extracts

def availability_tree_table_maker(dataset_ou,de_df,period_table):
    return cross_join(de_coc_ou_uids_table_generator(dataset_ou,de_df),period_table)

def _analytics_lvl_function_cleaner(df,level_max):
    level=df.LEVEL.max()
    if level<level_max:
        df['ERASE']=df.OU_UID.apply(lambda x: x in lvl_uids_dict[level])
        return df
    else:
        df['ERASE']=False
        return df


def direct_ancestor_cleaner(df_ous):
    from functools import partial
    levels=df_ous.LEVEL.sort_values(ascending=True).unique().tolist()
    lvl_uids_dict={}
    for level in levels[:-1]:
        lvl_uids_dict.update({
                            level:df_ous['LEVEL_'+str(level)+'_UID'].unique().tolist()
                            })

    df_ous_processed=df_ous.groupby('LEVEL',index=False).partial(_analytics_lvl_function_cleaner,level_max=levels[0])
    return df_ous_processed.query('ERASE==0')


def analytics_tree_dependencies_cleaner(de_list,de_df,dataset_ou,ou_tree_df):
    ds_de=de_df.query('DE_UID in @de_list')[['DE_UID','DS_UID']].drop_duplicates()
    ds_de_ou=ds_de.merge(dataset_ou,on=['DS_UID'])
    ds_de_ou=merge(ou_tree_df,on='OU_UID')
    
    return ds_de_ou.groupby('DE_UID',as_index=False).direct_ancestor_cleaner()

def de_ou_coverage_dict(dataset_ou,de_coc_ou_uids,ou_tree_df):
    #Using the late table we obtain a table that determines the belonging of an DE for a OU
    ou_de_full_table=cross_join(ou_tree_df[['OU_UID']],de_coc_ou_uids[['DE_UID','COC_UID']].drop_duplicates())
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