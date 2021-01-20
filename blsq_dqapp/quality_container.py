# -*- coding: utf-8 -*-
"""
Created on Thu Jan 14 15:16:53 2021

@author: Fernando-Bluesquare
"""

from .outlier_detection import outlier_detection_handler
from .availability import availability_condition_generator_handler,cross_join
from .reporting_style_tools import reporting_style_classifier
from .formatting_tools import fosa_level_df_generation,lvl3_transformation_from_fosa,tableau_format_generator

class quality_auction_container(object):
    """Information and metadata about a given DHIS instance.
    Parameters
    ----------
    Attributes
    ----------

    """

    def __init__(self, input_value_df,input_metadata_tree,input_metadata_de,project_path_processed,files_main_name,de_uid_vars):
        """Create a dhis instance."""
        self.processed_df = input_value_df
        self.input_metadata_tree = input_metadata_tree
        self.project_path_processed = project_path_processed
        self.files_main_name = files_main_name
        self.de_uid_vars=de_uid_vars
        self.period_table=self.processed_df[['PERIOD']].drop_duplicates()
        self.input_metadata_de=input_metadata_de
        
    def outliers_generation(self):
        self.processed_df=outlier_detection_handler(self.processed_df,
                                                    self.de_uid_vars,
                                                    self.project_path_processed,
                                                    self.files_main_name)
            
    def availability_generation(self,custom_tree_input=None,level_to_filter=5):
        if custom_tree_input==None:
            fosa_tree_expected=self.input_metadata_tree.query('LEVEL=='+str(level_to_filter))
            fosa_tree_expected=fosa_tree_expected[['OU_UID']]
            custom_tree_input=cross_join(fosa_tree_expected,self.period_table)
            
        self.processed_df=availability_condition_generator_handler(self.processed_df,
                                                                   custom_tree_input,
                                                                   self.de_uid_vars)
        
    def reporting_style_generation(self):
        self.reporting_style_df=reporting_style_classifier(self.processed_df,
                                                     self.de_uid_vars)
        
        
    def fosa_df_generation(self):
        self.fosa_df=fosa_level_df_generation(self.processed_df,
                                              self.reporting_style_df,
                                              self.de_uid_vars)
        
    def lvl_3_generation(self):
        self.lvl_df=lvl3_transformation_from_fosa(self.fosa_df,
                                                  self.input_metadata_tree,
                                                  self.de_uid_vars)
        
        
    def tabeau_format_table_generation(self):
        self.tableau_format_table=tableau_format_generator(self.lvl_df,
                                                           self.de_uid_vars)
        
    def full_process_run(self):
        self.outliers_generation()
        self.availability_generation()
        self.reporting_style_generation()
        self.fosa_df_generation()
        self.lvl_3_generation()
        self.tabeau_format_table_generation()