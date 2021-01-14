# -*- coding: utf-8 -*-
"""
Created on Tue Jan 12 21:31:20 2021

@author: Fernando-Bluesquare
"""

def de_batch_selector(df,de_present_data,managable_row_size=5000000):
    #Function to divide the dtaframe of a smaller size that a threshold but without splitting DE values among groups
    
    total_rows=df.shape[0]
    de_batches_list=[]
    index_selector=[x for x in range(0,total_rows,managable_row_size)]
    
    for de_ind in index_selector:
        if de_ind+managable_row_size<=total_rows:
            selected_de=df.loc[de_ind:de_ind+managable_row_size,'DE_UID'].unique().tolist()
        else:
            selected_de=df.loc[de_ind:total_rows,'DE_UID'].unique().tolist()
        de_batches_list.append(selected_de)
    
    repeated_de_list=[]
    for de_existing in de_present_data:
        total_list=0
        for de_batch in de_batches_list:
            if de_existing in de_batch:
                total_list+=1
        if total_list>1:
            repeated_de_list.append(de_existing)
    
    de_batches_list_review=[]      
    for de_batch in de_batches_list:
        de_batches_list_review.append([de for de in de_batch if de not in repeated_de_list])
    de_batches_list_review.append(repeated_de_list)
    
    return de_batches_list_review




#TODO filter on time and DE present