#Author:Fernando
#Modified of Stéphan original code
import numpy as np
import requests
import urllib.parse
import pandas as pd
import getpass
import urllib
from datetime import datetime
from.periods import Periods
from .geometry import geometrify
import geopandas


class Dhis2Client(object):
    def __init__(self,host,full_url=False,optional_prefix=None):
        
        if host.startswith('http'):
            self.baseurl = host
            
        else:
            API_USER = getpass.getpass("API User")
            API_PWD = getpass.getpass("API Password")
            
            user = urllib.parse.quote(API_USER)
            pwd = urllib.parse.quote(API_PWD)

            self.baseurl = "https://"+user+":"+pwd+"@"+host
        
        self.session = requests.Session()
        self.optional_prefix=optional_prefix

    def get(self, path, params=None):
        if self.optional_prefix:
            url = self.baseurl+self.optional_prefix+"/api/"+path
        else:
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
    
    def fetch_indicators_structure(self,coc_default_name="default"):
        indicatorsStructure = self.get("indicators.json", 
                                         params={
                                                "paging":False,                                                          
                                                "fields":
                                                         "id,name"+
                                                         ",numerator,denominator"
                                                })['indicators']
        
        coc_default_uid=self.fetch_coc_structure().query('COC_NAME=="'+coc_default_name+'"').COC_UID.values[0]
        indicatorsStructure=self._indicators_json_to_df(indicatorsStructure,coc_default_uid=coc_default_uid)
        
        return indicatorsStructure
    
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
    
    def fetch_deg_structure(self):
        dataElementGroupsStructure = self.get("dataElementGroups.json", 
                                                     params={
                                                            "paging":False,                                                          
                                                            "fields":
                                                                     "id,name"+
                                                                     ",dataElements[id,name]"
                                                            })['dataElementGroups']
        
        dataElementGroupsStructure=self._deg_json_to_df(dataElementGroupsStructure)
        return dataElementGroupsStructure
    
    def fetch_indg_structure(self):
        indicatorGroupsStructure = self.get("indicatorGroups.json", 
                                                     params={
                                                            "paging":False,                                                          
                                                            "fields":
                                                                     "id,name"+
                                                                     ",indicators[id,name]"
                                                            })['indicatorGroups']
        
        indicatorGroupsStructure=self._indg_json_to_df(indicatorGroupsStructure)
        return indicatorGroupsStructure
    
    def fetch_oug_structure(self):
        organisationUnitGroupsStructure = self.get("organisationUnitGroups.json", 
                                                     params={
                                                            "paging":False,                                                          
                                                            "fields":
                                                                     "id,name"+
                                                                     ",organisationUnits[id,name]"
                                                            })['organisationUnitGroups']
        
        organisationUnitGroupsStructure=self._oug_json_to_df(organisationUnitGroupsStructure)
        return organisationUnitGroupsStructure
    
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
        dataSetsStructure.update({'dataElementGroupsStructure':self.fetch_deg_structure()})
        
        for key,item in dataSetsStructure.items():
            item.to_csv(metadata_root_path+key+suffix_path,index=False)
        
        organisationUnits={'organisationUnitsStructure':self.fetch_organisation_units_structure()}
        organisationUnits.update({'organisationUnitGroupsStructure':self.fetch_oug_structure()})
        for key,item in organisationUnits.items():
            item.to_csv(ou_path+key+suffix_path,index=False)
        print('habari_'+str(iso_code)+'_db_updated')
        
    def fetch_tracked_entity_instances(self, program_id, orgunit_id,page_size=40, fetch_all = False):
        
        tei_url='trackedEntityInstances.json'
        tei_url=tei_url+"?ou="+orgunit_id+"&ouMode=DESCENDANTS"
        tei_url=tei_url+"&fields=:all,enrollments[:all,events[:all]]"
        tei_url=tei_url+"&program="+program_id
        tei_url=tei_url+"&totalPages=true&pageSize="+str(page_size)
        
        tracked_entity_instances = self.get(tei_url)
        num_pages = tracked_entity_instances["pager"]["pageCount"]

        print(tracked_entity_instances["pager"])
        if fetch_all :
            for page in range(2, num_pages + 1) : 
                page_url_suffix="&page="+str(page)
                other_pages_instances = self.get(tei_url+page_url_suffix) 
                tracked_entity_instances["trackedEntityInstances"].extend(other_pages_instances["trackedEntityInstances"])
                
        return tracked_entity_instances["trackedEntityInstances"]

    def fetch_program_description(self, program_id):
        
        programDescription = self.get("programs/"+program_id+".json", 
                                             params={                                                     
                                                    "fields":
                                                             "id,name"+
                                                             ",programTrackedEntityAttributes[id,name,trackedEntityAttribute[id,name,code,valueType]]"+
                                                             ",programStages[id,name,programStageDataElements[dataElement[id,name,code,CvalueType,optionSet[options[id,name,code]]]"
                                                    })

        return programDescription


    def get_geodataframe(self, geometry_type=None):
            filters = []
            if geometry_type == "point":
                filters.extend("featureType:eq:POINT")
            elif geometry_type == "shape":
                filters.extend("featureType:in:[POLYGON,MULTI_POLYGON]")
            elif geometry_type == None:
                pass
            else:
                raise Exception("unsupported geometry type '" +
                                geometry_type+"'? Should be point,shape or None")
    
            params = {
                "fields": "id,name,coordinates,geometry,level",
                "paging": "false"
            }
    
            if len(filters) > 0:
                params["filter"] = "".join(filters),
            orgunits = self.get("organisationUnits", params)["organisationUnits"]
            #print(orgunits)
            #geometrify(orgunits)
    
            df = pd.DataFrame(orgunits)
    
            gdf = geopandas.GeoDataFrame(df)
            gdf=gdf.rename(columns={'id':'OU_UID','name':'OU_NAME'})
            gdf.columns=gdf.columns.str.upper()
    
            return gdf

        
    def extract_reporting(self,report_type='REPORTING_RATE'):
        pass
    def extract_data(self,dx_descriptor,pe_start_date,pe_end_date,frequency,ou_descriptor,coc_default_name="default"):
        path="analytics.json"
        if self.optional_prefix:
            url_analytics = self.baseurl+self.optional_prefix+"/api/"+path
        else:
            url_analytics = self.baseurl+"/api/"+path
        url_analytics =url_analytics+'?dimension='+self._dx_composer_feed(dx_descriptor)
        url_analytics =url_analytics+'&dimension='+self._ou_composer_feed(ou_descriptor)
        url_analytics =url_analytics+'&dimension='+self._pe_composer_feed(pe_start_date,pe_end_date,frequency)
            
        resp_analytics = self.session.get(url_analytics)
        print(resp_analytics.request.path_url)

        analyticsData=resp_analytics.json()['rows']
        
        if not analyticsData:
            print( "No Data in DB")
            pass 
        else:
            coc_default_uid=self.fetch_coc_structure().query('COC_NAME=="'+coc_default_name+'"').COC_UID.values[0]
            analyticsData=self._analytics_json_to_df(analyticsData,coc_default_uid=coc_default_uid)
            return analyticsData
    
    def _ou_composer_feed(self,ou_descriptor):
        for key in ou_descriptor.keys():
            if key=='OUG':
                pass
            if key=='OU':
                if len(ou_descriptor['OU'])>1:
                    return 'ou:'+';'.join(ou_descriptor['OU'])

                else:
                    return 'ou:'+ou_descriptor['OU'][0]
                
    def _dx_composer_feed(self,dx_descriptor):
        for key in dx_descriptor.keys():
            if key=='DEG':
                pass
            if key=='DX':
                if len(dx_descriptor['DX'])>1:
                    return 'dx:'+';'.join(dx_descriptor['DX'])
                else:
                    return 'dx:'+dx_descriptor['DX'][0]
                
    def _pe_composer_feed(self,pe_start_date,pe_end_date,frequency):
        pe_list=Periods.split([pe_start_date,pe_end_date],frequency)
        if len(pe_list)>1:
            return 'pe:'+';'.join(pe_list)
        else:
            return 'pe:'+pe_list[0]
    
    
    def _analytics_json_to_df(self,json_data,coc_default_uid):
        df=pd.DataFrame.from_records(json_data,columns=['DE_UID','OU_UID','PERIOD','VALUE'])
        de_uid_splitted=df['DE_UID'].str.split('.',n=1,expand=True)
        
        if (de_uid_splitted.shape[1]==1):
            de_uid_splitted=de_uid_splitted.rename(columns={0:'DE_UID'})
            de_uid_splitted['COC_UID']=coc_default_uid
        elif (de_uid_splitted.shape[1]>1):
            de_uid_splitted=de_uid_splitted.rename(columns={0:'DE_UID',1:'COC_UID'})
            de_uid_splitted['COC_UID']=de_uid_splitted['COC_UID'].fillna(coc_default_uid)
        else:
            print('No Data')
            pass
        df=pd.concat([df.drop('DE_UID',axis=1),de_uid_splitted],axis=1)
                     
        return df
    
        

    ##################  AUXILIAR FUNCTIONS  ########################

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
    
    def _num_den_text_processator(self,text,coc_default_uid):
        from functools import partial
        import re
        if not text.isdecimal():
            def word_splitter_format(word,coc_default_uid):
                if "." in word:
                    return word.split('.')
                else:
                    return [word,coc_default_uid]
            text=re.sub(r'[#{}()]',' ',text)
            text=re.sub(r'[*]',' ',text)
            text=re.sub(r'[/]',' ',text)
            word_token_list=re.findall(r'[\w.]+',text)
            word_token_list=[partial(
                                    word_splitter_format,
                                    coc_default_uid=coc_default_uid
                                    )(word=word) for word in word_token_list]
            return pd.DataFrame.from_records(word_token_list,columns=['DE_UID','COC_UID'])
        else:
            return pd.DataFrame({'DE_UID':[None],'COC_UID':[None]})

    def _indicators_json_to_df(self,json_data,coc_default_uid):
        ind_df_list=[]
        for ind in json_data:
            ind_name=None
            ind_uid=ind['id']
            if 'name' in ind.keys():
                ind_name=ind['name']
            for key_type in ['numerator','denominator']:
                if key_type in ind.keys():
                    if ind[key_type]:
                        ind_df_list.append(self._num_den_text_processator(
                                                                ind[key_type],coc_default_uid=coc_default_uid
                                                                    ).assign(
                                                                            RELATION_TYPE=key_type.upper()
                                                                            )
                                          )
        united_df=pd.concat(ind_df_list,ignore_index=True)
        return united_df.assign(IND_UID=ind_uid).assign(IND_NAME=ind_name)
    
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
    
    def _deg_json_to_df(self,df):
        deg_df_list=[]
        for deg in df:
            deg_name=[None]
            deg_uid=[deg['id']]
            if 'name' in deg.keys():
                deg_name=[deg['name']]
            if 'dataElements' in deg.keys():
                if deg['dataElements']:
                    for deg_de in deg['dataElements']:
                        deg_df_list.append(pd.DataFrame({'DEG_UID':deg_uid,'DEG_NAME':deg_name,'DE_UID':[deg_de['id']],'DE_NAME':[deg_de['name']]}))
            else:
                deg_df_list.append(pd.DataFrame({'DEG_UID':deg_uid,'DEG_NAME':deg_name,'DE_UID':[None],'DE_NAME':[None]}))


        return pd.concat(deg_df_list,ignore_index=True)
    
    
    def _indg_json_to_df(self,df):
        indg_df_list=[]
        for indg in df:
            indg_name=[None]
            indg_uid=[indg['id']]
            if 'name' in indg.keys():
                indg_name=[indg['name']]
            if 'indicators' in indg.keys():
                if indg['indicators']:
                    for indg_ind in indg['indicators']:
                        indg_df_list.append(pd.DataFrame({'INDG_UID':indg_uid,'INDG_NAME':indg_name,'IND_UID':[indg_ind['id']],'IND_NAME':[indg_ind['name']]}))
            else:
                indg_df_list.append(pd.DataFrame({'INDG_UID':indg_uid,'INDG_NAME':indg_name,'IND_UID':[None],'IND_NAME':[None]}))


        return pd.concat(indg_df_list,ignore_index=True)
    
    
    def _oug_json_to_df(self,df):
        oug_df_list=[]
        for oug in df:
            oug_name=[None]
            oug_uid=[oug['id']]
            if 'name' in oug.keys():
                oug_name=[oug['name']]
            if 'organisationUnits' in oug.keys():
                if oug['organisationUnits']:
                    for oug_ou in oug['organisationUnits']:
                        oug_df_list.append(pd.DataFrame({'OUG_UID':oug_uid,'OUG_NAME':oug_name,'OU_UID':[oug_ou['id']],'OU_NAME':[oug_ou['name']]}))
            else:
                oug_df_list.append(pd.DataFrame({'OUG_UID':oug_uid,'OUG_NAME':oug_name,'OU_UID':[None],'OU_NAME':[None]}))


        return pd.concat(oug_df_list,ignore_index=True)
    

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