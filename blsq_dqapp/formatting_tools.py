# -*- coding: utf-8 -*-
"""
Created on Tue Jan 12 21:22:17 2021

@author: Fernando-Bluesquare
"""

import numpy as np
import pandas as pd
#Reviewed for DE
def fosa_stats_generator(df,grouping_cols):
    values_cols=['VALUE', 'DE_AVAILABILITY_BOOL','OUTLIER_RS', 'ZERO']
    grouped=df[values_cols+grouping_cols].groupby(grouping_cols)
    
    df_sum=grouped['DE_AVAILABILITY_BOOL','OUTLIER_RS','ZERO'].sum().astype('uint8')

    df_sum=df_sum.rename(columns={'DE_AVAILABILITY_BOOL':'REPORTING_MONTHS_COUNT','OUTLIER_RS':'OUTLIER_RS_COUNT'})
    df_mean=grouped['VALUE', 'DE_AVAILABILITY_BOOL','OUTLIER_RS'].mean()
    df_mean['OUTLIER_FOSA']=(df_mean['OUTLIER_RS']>0).astype('uint8')
    df_mean=df_mean.drop('OUTLIER_RS',axis=1)
    df_mean=df_mean.join(df_sum)
    
    return df_mean

def reporting_style_df_extension(df,added_de_uid_vars):
    df.COMPLETENESS=np.where(df.COMPLETENESS=='SINCE_FULL','ALWAYS',df.COMPLETENESS)
    reporting_matrix=pd.get_dummies(df.COMPLETENESS)
    df=pd.concat([df.drop('COMPLETENESS',axis=1),reporting_matrix],axis=1)
    return df

def fosa_level_df_generation(df,df_reporting_style,added_de_uid_vars):
    grouping_cols=['OU_UID']+added_de_uid_vars
    
    fosa_stats_df=fosa_stats_generator(df,grouping_cols)
    df_reporting_style=reporting_style_df_extension(df_reporting_style,added_de_uid_vars)
    
    return df_reporting_style.join(fosa_stats_df,on=grouping_cols)





def lvl3_transformation_from_fosa_function(df,added_de_uid_vars):
    grouped=df.groupby(['LEVEL_2_UID','LEVEL_3_UID']+added_de_uid_vars)
    df_mean=grouped.mean().drop(['OUTLIER_RS_COUNT','ZERO'],axis=1)
    
    df_sum=grouped['REPORTING_MONTHS_COUNT','OUTLIER_RS_COUNT','ZERO'].sum()
    df_sum['OUTLIER_VALUES']=df_sum['OUTLIER_RS_COUNT'].div(df_sum['REPORTING_MONTHS_COUNT']).replace(np.inf, 0)
    df_sum['ZERO']=df_sum['ZERO'].div(df_sum['REPORTING_MONTHS_COUNT']).replace(np.inf, 0)
    
    df_sum=df_sum.drop(['REPORTING_MONTHS_COUNT','OUTLIER_RS_COUNT'],axis=1)
    
    return df_mean.join(df_sum,on=['LEVEL_2_UID','LEVEL_3_UID']+added_de_uid_vars).reset_index()



def lvl3_transformation_from_fosa(df_fosa,df_tree,de_uid_vars):

    #We attached level_3 info to facility level
    df_tree=df_tree[['OU_UID','LEVEL_2_UID','LEVEL_3_UID']].drop_duplicates()
    info_lvl_3_df=df_tree.merge(df_fosa,on=['OU_UID'])
    info_lvl_3_df=lvl3_transformation_from_fosa_function(info_lvl_3_df,de_uid_vars)
    return info_lvl_3_df


def tableau_format_generator(info_lvl_3_df,de_uid_vars):
    #Tableau format is needed to have separate color legends at tableau
    de_vars_len=len(de_uid_vars)
    rename_dict={0:'VALUE','level_'+str(de_vars_len):'AGG','level_'+str(de_vars_len+1):'VARIABLE'}

    de_full_table=info_lvl_3_df.groupby(de_uid_vars).agg('describe').stack().stack().reset_index().rename(columns=rename_dict)
    de_full_table_tableau_format=pd.pivot_table(de_full_table,index=de_uid_vars+['AGG'],columns='VARIABLE',values='VALUE').reset_index()
    
    agg_stats_final=['25%', '50%', '75%','max', 'mean', 'min', 'std']
    de_full_table_tableau_format=de_full_table_tableau_format[de_full_table_tableau_format.AGG.isin(agg_stats_final)]
    
    return de_full_table_tableau_format


fosa_df=fosa_level_df_generation(data_tree_extended,reporting_style_df,de_uid_vars)




lvl2_uid_dict=org_unit_tree[['LEVEL_2_NAME','LEVEL_2_UID']].drop_duplicates().set_index('LEVEL_2_NAME').to_dict()['LEVEL_2_UID']
lvl3_uid_dict=org_unit_tree[['LEVEL_3_NAME','LEVEL_3_UID']].drop_duplicates().set_index('LEVEL_3_NAME').to_dict()['LEVEL_3_UID']

info_lvl_3_df['LEVEL_3_UID']=info_lvl_3_df.LEVEL_3_NAME.map(lvl3_uid_dict)
info_lvl_3_df['LEVEL_2_UID']=info_lvl_3_df.LEVEL_2_NAME.map(lvl2_uid_dict)






#DE uids/name/ds dict
de_uid_dict=pd.read_csv('s3://habari-public/Metadata/Cameroon/de_ids_Cameroon-23-09-2020.csv').set_index('DE_UID').to_dict()['DE_NAME']
de_uid_ds_dict=RMA_datasets_datelements_metadata.set_index('DE_UID').to_dict()['DS_NAME']



#Using the late table we obtain a table that determines the belongign of an DE for a OU
ou_de_full_table=cross_join(org_unit_tree[['OU_UID']],de_table)
ou_de_full_table=ou_de_full_table.merge(de_coc_ou_uids.assign(DE_BELONG=1),how='left',on=['OU_UID', 'DE_UID', 'COC_UID']).fillna(0)
de_ou_coverage=ou_de_full_table.groupby('DE_UID').mean().to_dict()['DE_BELONG']


de_full_table_tableau_format['DE_NAME']=de_full_table_tableau_format.DE_UID.map(de_uid_dict)
de_full_table_tableau_format['COC_NAME']=de_full_table_tableau_format.COC_UID.map(coc_uid_dict)
de_full_table_tableau_format['DS_NAME']=de_full_table_tableau_format.DE_UID.map(de_uid_ds_dict)
de_full_table_tableau_format['DE_COVERAGE']=de_full_table_tableau_format.DE_UID.map(de_ou_coverage)


de_full_table_tableau_format=de_full_table_tableau_format[['DS_NAME','DE_NAME','DE_UID','DE_COVERAGE','AGG','ALWAYS','INCONSISTENT','NEVER','STOPPED','DE_AVAILABILITY_BOOL','REPORTING_MONTHS_COUNT','OUTLIER_FOSA', 'OUTLIER_VALUES', 'VALUE','ZERO']]