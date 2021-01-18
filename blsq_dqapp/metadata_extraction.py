#Author:Fernando
#Modified of Stéphan original code

import requests
import urllib.parse
import pandas as pd
import getpass
import urllib
from datetime import datetime


class Dhis2Client(object):
    def __init__(self,host,full_url=False):
        
        if host.startswith('http'):
            self.baseurl = host
            
        else:
            API_USER = getpass.getpass("API User")
            API_PWD = getpass.getpass("API Password")
            
            user = urllib.parse.quote(API_USER)
            pwd = urllib.parse.quote(API_PWD)

            self.baseurl = "https://"+user+":"+pwd+"@"+host
        
        self.session = requests.Session()

    def get(self, path, params=None):
        url = self.baseurl+"/api/"+path
        resp = self.session.get(url, params=params)
        print(resp.request.path_url)
        return resp.json()

    def fetch_program_description(self, program_id):
        programDescription = self.get("programs/"+program_id+".json", params={
            "fields":"id,name,categoryCombo[id,name,categoryOptionCombos[id,name]]"+
            ",trackedEntityType[id,name,code,trackedEntityTypeAttributes[id,name,trackedEntityAttribute[id,name,code,valueType,optionSet[options[id,name,code]]]]]"+
            ",programStages[id,name,programStageDataElements[compulsory,dataElement[id,name,code,valueType,optionSet[options[id,name,code]]]]"})
        return programDescription
    
    def fetch_organisation_units_structure(self):
        organisationUnitsStructure=self.get("organisationUnits.json", 
                                             params={
                                                     "paging":False, 
                                                     "fields":
                                                              "id,name,level"+
                                                              ",ancestors[id,name]"
                                                     })["organisationUnits"]
        
        organisationUnitsStructure=self._outree_json_to_df(organisationUnitsStructure)
        return organisationUnitsStructure
    
    def fetch_data_elements_structure(self):
        dataElementsStructure = self.get("dataElements.json", 
                                         params={
                                                "paging":False,                                                          
                                                "fields":
                                                         "id,name,categoryCombo"+
                                                         ",dataSetElements[dataSet]"
                                                })['dataElements']
        
        dataElementsStructure=self._data_elements_json_to_df(dataElementsStructure)
        
        categoryOptionCombosStructure=self.fetch_coc_structure()
        dataElementsStructure=dataElementsStructure.merge(categoryOptionCombosStructure,on='CC_UID',how='left')
        
        return dataElementsStructure
    
    def fetch_dataset_structure(self):
        dataSetsStructure = self.get("dataSets.json", 
                                     params={
                                            "paging":False,                                                          
                                            "fields":
                                                     "id,name,organisationUnits"+
                                                     ",dataSetElements[dataElement]"
                                            })['dataSets']
        
        dataSetsStructure=self._datasets_json_to_df(dataSetsStructure)
        return dataSetsStructure
    
    def fetch_coc_structure(self):
        categoryOptionCombosStructure = self.get("categoryOptionCombos.json", 
                                                     params={
                                                            "paging":False,                                                          
                                                            "fields":
                                                                     "id,name,categoryCombo"
                                                            })['categoryOptionCombos']
        
        categoryOptionCombosStructure=self._cocs_json_to_df(categoryOptionCombosStructure)
        return categoryOptionCombosStructure
    
    def metadata_country_habari_db_full_refresh(self,iso_code,base_path=None):
        if not base_path:
            metadata_root_path='s3://habari-public/Metadata/'+str(iso_code)+'/'+str(iso_code)+'_'
            ou_path='s3://habari-public/OrganisationTrees/'+str(iso_code)+'/'+str(iso_code)+'_'
        else:
            metadata_root_path=base_path
            ou_path=base_path

        date=datetime.now().strftime('%Y-%m-%d')
        suffix_path='_'+date+'.csv'
        
        dataSetsStructure=self.fetch_dataset_structure()
        dataSetsStructure.update({'dataElementsStructure':self.fetch_data_elements_structure()})
        for key,item in dataSetsStructure.items():
            item.to_csv(metadata_root_path+key+suffix_path,index=False)
            
        self.fetch_organisation_units_structure().to_csv(ou_path+'organisationUnitsStructure'+suffix_path,index=False)
        print('habari_'+str(iso_code)+'_db_updated')
    
    def _data_elements_json_to_df(self,df):
        df_list=[]
        for de in df:
            cc=[None]
            de_name=[None]
            de_uid=[de['id']]
            if 'name' in de.keys():
                de_name=[de['name']]
            if 'categoryCombo' in de.keys():
                if de['categoryCombo']:
                    cc=[de['categoryCombo']['id']]
            if 'dataSetElements' in de.keys():
                if de['dataSetElements']:
                    for ds_de in de['dataSetElements']:
                        df_list.append(pd.DataFrame({'DE_UID':de_uid,'DE_NAME':de_name,'CC_UID':cc,'DS_UID':[ds_de['dataSet']['id']]}))
            else:
                df_list.append(pd.DataFrame({'DE_UID':de_uid,'DE_NAME':de_name,'CC_UID':cc,'DS_UID':[None]}))
        return pd.concat(df_list,ignore_index=True)
    
    def _datasets_json_to_df(self,df):
        df_list_ou=[]
        df_list_de=[]  
        for ds in df:
            ou=[None]
            ds_name=[None]
            ds_uid=[ds['id']]
            if 'name' in ds.keys():
                ds_name=[ds['name']]
            if 'organisationUnits' in ds.keys():
                if ds['organisationUnits']:
                    for ou in ds['organisationUnits']:
                        df_list_ou.append(pd.DataFrame({'DS_UID':ds_uid,'DS_NAME':ds_name,'OU_UID':[ou['id']]}))
            else:
                df_list_ou.append(pd.DataFrame({'DS_UID':ds_uid,'DS_NAME':ds_name,'OU_UID':[None]}))
            if 'dataSetElements' in ds.keys():
                if ds['dataSetElements']:
                    for ds_de in ds['dataSetElements']:
                        df_list_de.append(pd.DataFrame({'DS_UID':ds_uid,'DS_NAME':ds_name,'DE_UID':[ds_de['dataElement']['id']]}))
            else:
                df_list_de.append(pd.DataFrame({'DS_UID':ds_uid,'DS_NAME':ds_name,'DE_UID':[None]}))
        
        return {"dataSetsStructure_dataElements":pd.concat(df_list_de,ignore_index=True),"dataSetsStructure_organisationUnits":pd.concat(df_list_ou,ignore_index=True)}

    def _cocs_json_to_df(self,df):
        coc_df_list=[]
        for coc in df:
            cc=[None]
            coc_name=[None]
            coc_uid=[coc['id']]
            if 'name' in coc.keys():
                coc_name=[coc['name']]
            if 'categoryCombo' in coc.keys():
                if coc['categoryCombo']:
                    cc=[coc['categoryCombo']['id']]

            coc_df_list.append(pd.DataFrame({'COC_UID':coc_uid,'COC_NAME':coc_name,'CC_UID':cc}))


        return pd.concat(coc_df_list,ignore_index=True)
    

    def _outree_json_to_df(self,df):
        ou_df_list=[]
        for ou in df:
            ou_level=[None]
            ou_name=[None]
            ou_uid=[ou['id']]
            if 'name' in ou.keys():
                ou_name=[ou['name']]
            ou_dict={'OU_UID':ou_uid,
                     'OU_NAME':ou_name}
            
            if 'ancestors' in ou.keys():
                if ou['ancestors']:
                    ancestors_list=ou['ancestors']
                    for ancestor_lvl in range(0,len(ancestors_list)):
                        ancestor_dict=ancestors_list[ancestor_lvl]
                        ou_dict.update({
                                       'LEVEL_'+str(ancestor_lvl+1)+'_UID':[ancestor_dict['id']],
                                       'LEVEL_'+str(ancestor_lvl+1)+'_NAME':[ancestor_dict['name']]
                                      })
            
            if (('level' in ou.keys()) or (ancestors_list)):
                if not 'level' in ou.keys():
                    ou_level=ou['level']
                else:
                    ou_level=len(ancestors_list)+1
                    
                ou_dict.update({
                                'LEVEL':[ou_level],
                                'LEVEL_'+str(ou_level)+'_UID':ou_uid,
                                'LEVEL_'+str(ou_level)+'_NAME':ou_name
                               })
                
            ou_df_list.append(pd.DataFrame(ou_dict))


        return pd.concat(ou_df_list,ignore_index=True)