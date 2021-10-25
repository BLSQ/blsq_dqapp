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
import time
import datetime


class Dhis2Client(object):
    def __init__(self,host,full_url=False,optional_prefix=None,agent_name='dqapp'):
        
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
        self.agent_name=agent_name

    def get(self, path, params=None,silent=False,verify=True):
        if self.optional_prefix:
            url = self.baseurl+self.optional_prefix+"/api/"+path
        else:
            url = self.baseurl+"/api/"+path
        if verify:
            resp = self.session.get(url, 
                                    params=params,
                                    headers={'user-agent':self.agent_name }
                                    )
        if not verify:
            resp = self.session.get(url, 
                                    params=params,
                                    verify=False,
                                    headers={'user-agent':self.agent_name }
                                    )
        if not silent:
            print(resp.request.path_url)
        return resp.json()
    
    def post(self, path, data=None,json=None,silent=False,verify=True):
        if self.optional_prefix:
            url = self.baseurl+self.optional_prefix+"/api/"+path
        else:
            url = self.baseurl+"/api/"+path
        if verify:
            resp = self.session.post(url, data=data,json=json)
        if not verify:
            resp = self.session.post(url, data=data,json=json,verify=False)
        if not silent:
            print(resp.request.path_url)
        return resp.json()
    
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
                                                         ",domainType"+
                                                         ",dataSetElements[dataSet,categoryCombo]"
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
                                                     ",periodType"+
                                                     ",dataSetElements[dataElement]"
                                            })['dataSets']
        
        dataSetsStructure=self._datasets_json_to_df(dataSetsStructure)
        return dataSetsStructure
    
    def fetch_coc_structure(self):
        categoryOptionCombosStructure = self.get("categoryOptionCombos.json",
                                                 silent=True,
                                                 params={
                                                         "paging":False,                                                          
                                                         "fields":
                                                                 "id,name,categoryCombo"
                                                         })['categoryOptionCombos']
        
        categoryOptionCombosStructure=self._cocs_json_to_df(categoryOptionCombosStructure)
        
        #Adding current use categories information 
        categoryCombos_UIDS=categoryOptionCombosStructure.CC_UID.unique().tolist()
        coc_queries_df_list=[]
        for coc_unit_uid in categoryCombos_UIDS:
            categoryComboStructure_UID_unit = self.get(f"categoryCombos/{coc_unit_uid}.json",
                                                 silent=True,
                                                 params={                                                        
                                                         "fields":
                                                                 "id,categoryOptionCombos"
                                                         })
            coc_queries_df_list.append(self._cc_uid_json_to_df(categoryComboStructure_UID_unit))
            
        categoryOptionCombosStructure_Current=pd.concat(coc_queries_df_list,
                                                        ignore_index=True).assign(CURRENT_USE=True)
        categoryOptionCombosStructure=categoryOptionCombosStructure.merge(categoryOptionCombosStructure_Current,
                                                                            on=['CC_UID','COC_UID'],
                                                                            how='left')
        categoryOptionCombosStructure['CURRENT_USE']=categoryOptionCombosStructure['CURRENT_USE'].fillna(False)
        
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
        
    def extract_data_program(self, program_id_list, orgunit_id_list,program_type='event',page_size=40, fetch_all = False):
        programData_json=[]
        if program_type=='event':
            for program_id in program_id_list:
                for ou_unit_uid in orgunit_id_list:
                    programData_json.extend(self._fetch_program_events(program_id=program_id,
                                                                       orgunit_id=ou_unit_uid,
                                                                       page_size=page_size,
                                                                       fetch_all = fetch_all)
                                            )
            
            programData_df=self._program_event_data_json_to_df(programData_json)
            return programData_df
            
        elif program_type=='tracker':
            for program_id in program_id_list:
                for ou_unit_uid in orgunit_id_list:
                    programData_json.extend(self.fetch_tracked_entity_instances( program_id=program_id,
                                                                                orgunit_id=ou_unit_uid,
                                                                                page_size=page_size,
                                                                                fetch_all = fetch_all)
                                        )
            
            programData_df=self._program_tei_json_to_df(programData_json)
            return programData_df
            
        else:
            print('Not a valid program type')
            return
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

    def fetch_program_descriptions(self):
        
        programDescription = self.get("programs.json", 
                                             params={"paging":False,                                                
                                                    "fields":
                                                             "id,name"+
                                                             ",programTrackedEntityAttributes[id,name,trackedEntityAttribute[id,name,code,valueType]]"+
                                                             ",programStages[id,name,programStageDataElements[dataElement[id,name,code,valueType,optionSet[options[id,name,code]]]"
                                                    })['programs']
            
        
        programDescription=self._program_description_json_to_df(programDescription)

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

        
    def extract_reporting(self,datasets,pe_start_date,pe_end_date,frequency,ou_descriptor,report_types=['REPORTING_RATE'],silent=False,dx_batch_size=None,ou_batch_size=None):
        dx_descriptor_list=[]
        for dataset in datasets:
            for report_type in report_types:
                dx_descriptor_list.append(dataset+'.'+report_type)
        dx_descriptor={'DX':dx_descriptor_list}
        
        return self.extract_data(dx_descriptor,pe_start_date,pe_end_date,frequency,ou_descriptor,silent=silent,dx_batch_size=dx_batch_size,ou_batch_size=ou_batch_size).rename(columns={'DE_UID':'DS_UID','COC_UID':'REPORTING_TYPE'})
        
    
    def extract_data(self,dx_descriptor,pe_start_date,pe_end_date,frequency,ou_descriptor,
                     coc_default_name="default",silent=False,expand_coc=True,
                     dx_batch_size=None,ou_batch_size=None):
        
        #TODO Filter on valid data type elements
        #Take DE and filter them 
        time_descriptor={'pe_start_date':pe_start_date,
                         'pe_end_date':pe_end_date,
                         'frequency':frequency}
        
        path="analytics.json"
        if self.optional_prefix:
            url_analytics_base = self.baseurl+self.optional_prefix+"/api/"+path
        else:
            url_analytics_base = self.baseurl+"/api/"+path
            
        coc_default_uid=self.fetch_coc_structure().query('COC_NAME=="'+coc_default_name+'"').COC_UID.values[0]
        
        ##TODO Filter on valid data for analytics, automate the frequency to query, autoamte teh ou to query given a tree 
            
        if expand_coc and ('DX' in dx_descriptor.keys()):
            fetched_dx=self._dx_coc_expander(dx_descriptor)
            dx_descriptor['DX']=fetched_dx
            
        all_extractions_done=False
        analyticsData_df_list_cycles=[]
        
        print('-- Start requests--',datetime.datetime.now())
        t_start=time.time()
        if not dx_batch_size:
            dx_batch_size=self._max_len_descriptor_estimator(dx_descriptor)
        if not ou_batch_size:
            ou_batch_size=self._max_len_descriptor_estimator(ou_descriptor)
        
        while not all_extractions_done:
            dx_batchted_descriptors=self._batch_splitter(dx_batch_size,dx_descriptor)
            ou_batchted_descriptors=self._batch_splitter(ou_batch_size,ou_descriptor)
        
            dx_uncalled_batchs,ou_uncalled_batchs,analyticsData_df_list_cycle=self._query_caller_manager(url_analytics_base,"analytics_extract",
                                                                                                  dx_batchted_descriptors,ou_batchted_descriptors,
                                                                                                  time_descriptor,coc_default_uid,silent=silent)
            analyticsData_df_list_cycles.extend(analyticsData_df_list_cycle)
            
            if len(dx_uncalled_batchs)==0 and len(ou_uncalled_batchs)==0:
                all_extractions_done=True        
            else:
                print("There are still unfinsihed calls. Resetting a new cycle of queries")
                
                #print(dx_uncalled_batchs)
                dx_descriptor=self._batch_rebuilder(dx_uncalled_batchs)
                ou_descriptor=self._batch_rebuilder(ou_uncalled_batchs)
                
                if dx_batch_size>ou_batch_size:
                    dx_batch_size=int(round(dx_batch_size/2,0))
                else:
                    ou_batch_size=int(round(ou_batch_size/2,0))
                    
                if dx_batch_size==0 or ou_batch_size==0:
                    break
                print(f'New cycle dx_batch_size={dx_batch_size} ;ou_batch_size={ou_batch_size}')  
                
        try:
            analyticsData_df=pd.concat(analyticsData_df_list_cycles,ignore_index=True)
            if not all_extractions_done:
                print("Unresolved calls")
                print(dx_descriptor,ou_descriptor)
        except ValueError:
            if all_extractions_done:
                print("No data has been found for the whole range of metadata")
            else:
                print("Unresolved calls")
                print(dx_descriptor,ou_descriptor)
                
            analyticsData_df=pd.DataFrame(columns=['OU_UID','PERIOD','DE_UID','COC_UID','VALUE'])
            
            
                    
        #Make sure we filter on original requested data:
            
        analyticsData_df=self._filter_on_requested_uids(fetched_dx,analyticsData_df)
        
        print('-- End of requests--',datetime.datetime.now())
        t_end=time.time()
        print('Total time:',round((t_end-t_start)/60,2),'min')
        return analyticsData_df
                              
                              
        
        
    def extract_data_db(self,dx_descriptor,pe_start_date,pe_end_date,frequency,ou_descriptor,coc_default_name="default",silent=False,expand_coc=True):
        path="dataValues.json"
        periods=Periods.split([pe_start_date,pe_end_date],frequency)
        
        #TODO Filter on valid data type elements
        #Take DE and filter them 
        
        if expand_coc and ('DX' in dx_descriptor.keys()):
            dx_descriptor['DX']=self._dx_coc_expander(dx_descriptor)
        ous=ou_descriptor['OU']
        de_list=dx_descriptor['DX']
        
        if self.optional_prefix:
            url_db_base = self.baseurl+self.optional_prefix+"/api/"+path
        else:
            url_db_base = self.baseurl+"/api/"+path
            
        database_Data_df=[]
        de_list_len=len(de_list)
        de_index=1
        print('-- Start requests--',datetime.datetime.now())
        t_start=time.time()
        for de in de_list:
            print(f'{de} requested {de_index}/{de_list_len} of DE list')
            de_start=time.time()
            database_Data_df.append(self._db_extract_de_query_subcomposer(url_db_base,de,periods,ous,silent=silent))
            de_end=time.time()
            de_index +=1
            print('Batch query took:',round((de_end-de_start)/60,2),'min')
            
        if not database_Data_df:
            print("No Data in DB for any combination")
            database_Data_df=pd.DataFrame(columns=['DE_UID','PERIOD','OU_UID','VALUE','COC_UID'])
        else:
            database_Data_df=pd.concat(database_Data_df,ignore_index=True)

        print('-- End of requests--',datetime.datetime.now())
        t_end=time.time()
        print('Total time:',round((t_end-t_start)/60,2),'min')
        return database_Data_df
            
    def post_data_aggregate(self,df,endpoint="dataValues",data_label='VALUE',postDataset=True):
        
        if postDataset:
            http_list=self._json_post_aggregate_generator_from_df_on_ds(df,data_label=data_label)
        else:
            http_list=self._json_generator_from_df_raw(df,data_label=data_label)
        
        postAnswers=[]
        for http in http_list:
            postAnswers.append([http['period'],http['orgUnit'],self.session.post(endpoint,json=http)])
        return postAnswers
    
    def _ou_composer_feed(self,ou_descriptor):
        for key in ou_descriptor.keys():
            if key=='OUG':
                groups=';'.join(['OU_GROUP-'+group for group in ou_descriptor['OUG'][0]])
                ancestor=ou_descriptor['OUG'][1]
                return 'ou:'+groups+';'+ancestor
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
            cc_default=[None]
            de_name=[None]
            domain=[None]
            de_uid=[de['id']]
            if 'domainType' in de.keys():
                domain=[de['domainType']]
            if 'name' in de.keys():
                de_name=[de['name']]
            if 'categoryCombo' in de.keys():
                if de['categoryCombo']:
                    cc_default=[de['categoryCombo']['id']]
            if 'dataSetElements' in de.keys():
                if de['dataSetElements']:
                    for ds_de in de['dataSetElements']:
                        if 'categoryCombo' in ds_de.keys():
                            df_list.append(pd.DataFrame({'DE_UID':de_uid,
                                                         'DE_NAME':de_name,
                                                         'CC_UID':[ds_de['categoryCombo']['id']],
                                                         'DS_UID':[ds_de['dataSet']['id']],
                                                         'DOMAIN':domain}))
                        else:
                            df_list.append(pd.DataFrame({'DE_UID':de_uid,
                                                         'DE_NAME':de_name,
                                                         'CC_UID':cc_default,
                                                         'DS_UID':[ds_de['dataSet']['id']],
                                                         'DOMAIN':domain}))
            else:
                df_list.append(pd.DataFrame({'DE_UID':de_uid,'DE_NAME':de_name,'CC_UID':cc_default,'DS_UID':[None]}))
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
            frequency=[None]
            if 'periodType' in ds.keys():
                frequency=[ds['periodType']]
            if 'name' in ds.keys():
                ds_name=[ds['name']]
            if 'organisationUnits' in ds.keys():
                if ds['organisationUnits']:
                    for ou in ds['organisationUnits']:
                        df_list_ou.append(pd.DataFrame({'DS_UID':ds_uid,'DS_NAME':ds_name,'OU_UID':[ou['id']],'FREQUENCY':frequency}))
            else:
                df_list_ou.append(pd.DataFrame({'DS_UID':ds_uid,'DS_NAME':ds_name,'OU_UID':[None],'FREQUENCY':frequency}))
            if 'dataSetElements' in ds.keys():
                if ds['dataSetElements']:
                    for ds_de in ds['dataSetElements']:
                        df_list_de.append(pd.DataFrame({'DS_UID':ds_uid,'DS_NAME':ds_name,'DE_UID':[ds_de['dataElement']['id']],'FREQUENCY':frequency}))
            else:
                df_list_de.append(pd.DataFrame({'DS_UID':ds_uid,'DS_NAME':ds_name,'DE_UID':[None],'FREQUENCY':frequency}))
        
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
    
    def _cc_uid_json_to_df(self,cc_json):
        cc_coc_df_list=[]
        cc_uid=[None]
        coc_uid=[None]
        if 'id' in cc_json.keys():
            cc_uid=[cc_json['id']]
        if 'categoryOptionCombos' in cc_json.keys():
            for coc_option in cc_json['categoryOptionCombos']:
                coc_uid=[coc_option['id']]
                cc_coc_df_list.append(pd.DataFrame({'COC_UID':coc_uid,'CC_UID':cc_uid}))
            
        else:
            cc_coc_df_list.append(pd.DataFrame({'COC_UID':coc_uid,'CC_UID':cc_uid}))
            
        return pd.concat(cc_coc_df_list,ignore_index=True)
    
    
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
            ancestors_list=[]
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

    def _program_tei_json_to_df(self,json):
    
        prog_id=[json['id']]
        if 'name' in json:
            prog_name=[json['name']]
        else:
            prog_name=[None]
    
        if 'programTrackedEntityAttributes' in json:
            program_df_attributes_list=[]
            for attribute in json['programTrackedEntityAttributes']:
                prog_eti_att_id=[attribute['id']]
                prog_eti_att_name=[attribute['name']]
                eti_att_id=[attribute['trackedEntityAttribute']['id']]
                eti_att_name=[attribute['trackedEntityAttribute']['name']]
                eti_att_value_type=[attribute['trackedEntityAttribute']['valueType']]
                program_df_attributes_list.append(pd.DataFrame({'PROGRAM_T_UID':prog_id,
                                                                'PROGRAM_T_NAME':prog_name,
                                                                'PROGRAM_T_ETI_DATA_UID':prog_eti_att_id,'PROGRAM_T_ETI_DATA_NAME':prog_eti_att_name,
                                                                'ETI_ATT_UID':eti_att_id,'ETI_ATT_NAME':eti_att_name,'ETI_ATT_VALUE_TYPE':eti_att_value_type                                                               
                                                               }))
        if 'programStages' in json:
            program_df_program_stage_list=[]
            for ps in json['programStages']:
                prog_ps_id=[ps['id']]
                prog_ps_name=[ps['name']]
                for ps_de in ps['programStageDataElements']:
                    de=ps_de['dataElement']
                    de_name=[de['name']]
                    de_id=[de['id']]
                    de_code=[None]
                    if 'code' in de:
                        de_code=[de['code']]
                    de_value_type=[de['valueType']]
    
                    de_dict={'PROGRAM_T_UID':prog_id,
                             'PROGRAM_T_NAME':prog_name,
                             'PROGRAM_T_STAGE_UID':prog_ps_id,'PROGRAM_T_STAGE_NAME':prog_ps_name,
                             'DE_UID':de_id,'DE_NAME':de_name,'DE_CODE':de_code,'DE_VALUE_TYPE':de_value_type,                                                          
                            }
                    if 'optionSet'in de:
                        option_dict={}
                        i=1
                        for option in de['optionSet']['options']:
                            option_name=[option['name']]
                            option_id=[option['id']]
                            option_code=[None]
                            if 'code' in option:
                                option_code=[option['code']]
                            
                            option_dict.update({'OPTION_'+str(i)+'_UID':option_id,
                                                'OPTION_'+str(i)+'_NAME':option_name,
                                                'OPTION_'+str(i)+'_CODE':option_code})
                            i=i+1 
                            
                            
                        de_dict.update(option_dict)
                    program_df_program_stage_list.append(pd.DataFrame(de_dict))
        return {"programTracker_programTrackedEntityAttributes":pd.concat(program_df_attributes_list,ignore_index=True),"programTracker_programStagesDataElements":pd.concat(program_df_program_stage_list,ignore_index=True)}

    def _program_data_json_to_df(self,json):
        program_df_data_ins_df=[]
        for ins in json:
            tei_type=ins['trackedEntityType']
            tei_instance_id=ins['trackedEntityInstance']
            org_unit_id=ins['orgUnit']
            date_register=ins['created']
            storedby=ins['storedBy']
            enroll=ins['enrollments'][0]
        #    enroll_date=enroll['enrollmentDate']
        #    enroll_status=enroll['status']
    
            ins_dict={'TEI_UID':tei_type,'TEI_INS_UID':tei_instance_id,'OU_UID':org_unit_id,'DATE_REGISTER':date_register,'STORED_BY':storedby}
    
            att_dict={}
            for ins_att in ins['attributes']:
                ins_att_id=ins_att['attribute']
                #ins_att_code=ins_att['code'],
                #ins_att_name=ins_att['displayName']
                ins_att_value=ins_att['value']
                att_dict.update({ins_att_id:[ins_att_value]})
    
            event_dict={}
            for event in enroll['events']:
    
                #event_id=event['id']
                ins_event_ps=event['programStage']
    
                for de in event['dataValues']:
                    de_id=de['dataElement']
                    de_value=de['value']
                    event_dict.update({ins_event_ps+'_'+de_id:de_value})
            ins_dict.update(att_dict)
            ins_dict.update(event_dict)
            program_df_data_ins_df.append(pd.DataFrame(ins_dict))
        return program_df_data_ins_df
    
    def _json_post_aggregate_generator_from_df_on_ds(self,global_df,data_label='VALUE'):
        periods=global_df.PERIOD.unique()
        ous=global_df.OU_UID.unique()
        ds_uids=global_df.DS_OUT_UID.unique()
        http_lists=[]
        for ds in ds_uids:
            for period in periods:
                for ou in ous:
                    subdf=global_df.query('OU_UID=="'+ou+'"').query('PERIOD=="'+period+'"')
                    dataValueList=[]
                    for ind,row in subdf.iterrows():
                        dataValueList.append({"dataElement": row['DE_OUT_UID'], 
                                                  "value": row[data_label]})
                    
                    http_unit={
                              "dataSet": ds,
                              "period": period,
                              "orgUnit": ou,
                              "dataValues": dataValueList
                              }
                    http_lists.append(http_unit)
        return http_lists
    
    
    
    def _json_generator_from_df_raw(self,global_df,data_label='VALUE'):

        http_lists=[]
        for ind,row in global_df.iterrows():
            http_lists.append({"dataElement": row['DE_OUT_UID'],
                                  "period":row['PERIOD'],
                                  "orgUnit":row['OU_UID'],
                                  "value": row[data_label]})
        return http_lists
    
    def _batch_splitter(self,batch_size,dimension_descriptor):#items_list,key_label):
        if batch_size:
            batchted_descriptors=[]
            for key_label,items_list in dimension_descriptor.items():
                for i in range(0,len(items_list),batch_size):
                    j=i+batch_size
                    if j> len(items_list):
                        j==len(items_list) 
                    batchted_descriptors.append( {key_label:items_list[i:j]} )
        else:
            for key_label,items_list in dimension_descriptor.items():
                batchted_descriptors=[{key_label:items_list}]
        return batchted_descriptors
    
    def _batch_rebuilder(self,batch_list):
        existing_keys=[]
        full_build_batch={}
        for batch in batch_list:
            b_key=list(batch.keys())[0]
            
            if b_key in existing_keys:
                current_list=list(full_build_batch[b_key])
                new_list=list(batch[b_key])
                current_list.extend(new_list)
                full_build_batch[b_key]=current_list
            else:
                existing_keys.append(b_key)
                full_build_batch[b_key]=list(batch[b_key])
        
        for key,item in full_build_batch.items():
            full_build_batch[key]=list(set(item))
        return full_build_batch
    
    def _max_len_descriptor_estimator(self,descriptor):
        item_lens=[]
        for key,item in descriptor.items():
            item_lens.append(len(item))
        return max(item_lens)
    
    
    def _dx_coc_expander(self,dx_descriptor):
        de_specified_coc=[ de for de  in dx_descriptor['DX'] if '.' in de]
        de_unspecified_coc=[de.split('.')[0] for de in dx_descriptor['DX'] if '.' not in de]
        
        de_structure_=self.fetch_data_elements_structure()
        de_coc_table=de_structure_.query('DE_UID in @de_unspecified_coc')[['DE_UID','COC_UID']].drop_duplicates()
        
        indicators_list=[de for de in de_unspecified_coc if de not in de_coc_table.DE_UID.unique()]
        
        de_coc_table['IND_UID']=de_coc_table.DE_UID+'.'+de_coc_table.COC_UID
        de_built_coc=de_coc_table.IND_UID.tolist()
        
        de_built_coc=de_built_coc+de_specified_coc+indicators_list
        return de_built_coc
    
    def _url_query_list_generator (self,url_analytics_base, formula_key,dx_batchted_descriptors,ou_batchted_descriptors,time_descriptor):
        
        def _formula_query_text_maker(url_analytics_base,formula_key,dx_batch_descriptor,ou_batch_descriptor,time_descriptor):
            if formula_key=="analytics_extract":
                url_analytics =url_analytics_base+'?dimension='+self._dx_composer_feed(dx_batch_descriptor)
                url_analytics =url_analytics+'&dimension='+self._ou_composer_feed(ou_batch_descriptor)
                url_analytics =url_analytics+'&dimension='+self._pe_composer_feed(time_descriptor['pe_start_date'],time_descriptor['pe_end_date'],time_descriptor['frequency'])
            return url_analytics
            
        url_queries_list=[]
        for dx_batch_descriptor in dx_batchted_descriptors:
            for ou_batch_descriptor in ou_batchted_descriptors:
                url_queries_list.append(_formula_query_text_maker(url_analytics_base,formula_key,dx_batch_descriptor,ou_batch_descriptor,time_descriptor))
        return url_queries_list
    
    def _query_caller_manager(self,url_analytics_base,formula_key,
                              dx_batchted_descriptors,ou_batchted_descriptors,
                              time_descriptor,coc_default_uid,silent=False):
        analyticsData_df_list=[]
        dx_uncalled_batchs=[]
        ou_uncalled_batchs=[]
        batch_index=1
        total_queries=len(dx_batchted_descriptors)*len(ou_batchted_descriptors)
        
        if total_queries<=30:
            printing_batching_denominator=1
        elif total_queries<=200:
            printing_batching_denominator=10
        elif total_queries<=1000:
            printing_batching_denominator=25
        else:
            printing_batching_denominator=50
            
                    
        if len(dx_batchted_descriptors)>1 or len(ou_batchted_descriptors)>1: 
            printedText="Batch processing"
        else:
            printedText="Call processing"
        
        for dx_batch_descriptor in dx_batchted_descriptors:
            for ou_batch_descriptor in ou_batchted_descriptors:
                
                if batch_index % printing_batching_denominator == 0:
                    print(printedText, f' : {batch_index}/{total_queries}')
                
                url_query=self._formula_query_text_maker(url_analytics_base,formula_key,
                                                         dx_batch_descriptor,ou_batch_descriptor,time_descriptor)
                batch_index +=1

                try:
                    resp_analytics = self.session.get(url_query)
                    if not silent:
                        print(resp_analytics.request.path_url)
                    analyticsData_batch=resp_analytics.json()['rows']
                except (ValueError, KeyError):
                    
                    #We save the failed calls to be recycle in new future calls with smaller batches 
                    dx_uncalled_batchs.append(dx_batch_descriptor)
                    ou_uncalled_batchs.append(ou_batch_descriptor)
                else:
                    if not analyticsData_batch:
                        print( "No Data in DB for:",dx_batch_descriptor,ou_batch_descriptor)
                        pass 
                    else:
                        analyticsData_df_list.append(
                                                     self._analytics_json_to_df(analyticsData_batch,
                                                                                coc_default_uid=coc_default_uid)
                                                     )

        return dx_uncalled_batchs,ou_uncalled_batchs,analyticsData_df_list

                    
    def _formula_query_text_maker(self,url_analytics_base,formula_key,dx_batch_descriptor,ou_batch_descriptor,time_descriptor):
        if formula_key=="analytics_extract":
            url_analytics =url_analytics_base+'?dimension='+self._dx_composer_feed(dx_batch_descriptor)
            url_analytics =url_analytics+'&dimension='+self._ou_composer_feed(ou_batch_descriptor)
            url_analytics =url_analytics+'&dimension='+self._pe_composer_feed(time_descriptor['pe_start_date'],time_descriptor['pe_end_date'],time_descriptor['frequency'])
        return url_analytics
        
    def _db_extract_de_query_subcomposer(self,url_db_base,de,periods,ous,silent=True):
        database_decycle_Data=[]
        total_sub_len=len(periods)*len(ous)
        if '.' in de:
            coc=de.split('.')[1]
            de=de.split('.')[0]
        else:
            coc=None
        sub_index=1
        for period in periods:
            for ou in ous:
                if sub_index % 500 == 0:
                    print(f'------- {sub_index}/{total_sub_len}')
                sub_index +=1
                url_db =url_db_base+'?de='+de+'&pe='+period+'&ou='+ou
                if coc:
                    url_db=url_db+'&co='+coc
                
                resp_db = self.session.get(url_db)
                
                if not silent:
                    print(resp_db.request.path_url)
                try:
                    value_dict={'DE_UID':[de],
                                'PERIOD':[period],
                                'OU_UID':[ou],
                                'VALUE':[resp_db.json()[0]]}
                    if coc: 
                        value_dict.update({'COC_UID':[coc]})
                    database_decycle_Data.append(value_dict)
                except:
                    pass
        if not database_decycle_Data:
            print(f"No data has been found for the whole range of metadata in this batch for {de}")
            database_decycle_Data_df=pd.DataFrame(columns=['DE_UID','PERIOD','OU_UID','VALUE','COC_UID'])
        else:
            database_decycle_Data_df=[]
            for batch in database_decycle_Data:
                database_decycle_Data_df.append(pd.DataFrame(batch))
            database_decycle_Data_df=pd.concat(database_decycle_Data_df,ignore_index=True)
        return database_decycle_Data_df
    
    def _filter_on_requested_uids(self,dx_list,df):
        indicators_uids=[dx for dx in dx_list if '.' not in dx]
        decoc_uids=[dx for dx in dx_list if '.' in dx]
        if decoc_uids:
            expected_de_uids_df=pd.DataFrame({'DE_UID':decoc_uids})
            expected_de_uids_df['COC_UID']=expected_de_uids_df['DE_UID'].str.split('.',expand=True)[1]
            expected_de_uids_df['DE_UID']=expected_de_uids_df['DE_UID'].str.split('.',expand=True)[0]
            de_uids=expected_de_uids_df['DE_UID'].unique().tolist()
        

            df_no_indicators=df.query('DE_UID in @de_uids')
            df_no_indicators=df_no_indicators.merge(expected_de_uids_df)
        else:
            df_no_indicators=pd.DataFrame(columns=['DE_UID','PERIOD','OU_UID','VALUE','COC_UID'])
        if indicators_uids:
            df_indicators=df.query('DE_UID in @indicators_uids')
        else:
            df_indicators=pd.DataFrame(columns=['DE_UID','PERIOD','OU_UID','VALUE','COC_UID'])
        
        df_filtered=pd.concat([df_no_indicators,
                               df_indicators],
                                ignore_index=True)
        return df_filtered
    
    def _http_extract_error_handler(self,error_code,query_answer):
        #TODO FINISH it prorperly
        try:
            analyticsData_batch_answer=query_answer.json()
            error_code=analyticsData_batch_answer['errorCode']
            if error_code=="E7115":
#                "message":"Data elements must be of a value and aggregation type that allow aggregation: `[SqmMlZdvCZJ, OxAb92h9i3r, cQiW8TWZQWC]`"
                text=analyticsData_batch_answer["message"]
        except:
            pass
        return 

    def _fetch_program_events(self, program_id, orgunit_id,page_size=40, fetch_all = False):
    
        event_url='events.json'
        event_url=event_url+"?ou="+orgunit_id+"&ouMode=DESCENDANTS"
        event_url=event_url+"&fields=program,event,programStage,programType,status,orgUnit,orgUnitName,eventDate,completedBy,dataValues[dataElement,value]"
        event_url=event_url+"&program="+program_id
        event_url=event_url+"&totalPages=true&pageSize="+str(page_size)
    
        events = self.get(event_url)
        num_pages = events["pager"]["pageCount"]
    
        print(events["pager"])
        if fetch_all :
            for page in range(2, num_pages + 1) : 
                page_url_suffix="&page="+str(page)
                other_pages_instances = self.get(event_url+page_url_suffix) 
                events["events"].extend(other_pages_instances["events"])
    
        return events["events"]

    def _program_description_json_to_df(self,json):
        program_df_program_stage_list=[]
        program_df_attributes_list=[]
        for program in json:
            prog_id=[program['id']]
            if 'name' in program:
                prog_name=[program['name']]
            else:
                prog_name=[None]
        
            if 'programTrackedEntityAttributes' in program:
                
                for attribute in program['programTrackedEntityAttributes']:
                    prog_eti_att_id=[attribute['id']]
                    prog_eti_att_name=[attribute['name']]
                    eti_att_id=[attribute['trackedEntityAttribute']['id']]
                    eti_att_name=[attribute['trackedEntityAttribute']['name']]
                    eti_att_value_type=[attribute['trackedEntityAttribute']['valueType']]
                    program_df_attributes_list.append(pd.DataFrame({'PROGRAM_T_UID':prog_id,
                                                                    'PROGRAM_T_NAME':prog_name,
                                                                    'PROGRAM_T_ETI_DATA_UID':prog_eti_att_id,'PROGRAM_T_ETI_DATA_NAME':prog_eti_att_name,
                                                                    'ETI_ATT_UID':eti_att_id,'ETI_ATT_NAME':eti_att_name,'ETI_ATT_VALUE_TYPE':eti_att_value_type                                                               
                                                                   }))
            if 'programStages' in program:
                
                for ps in program['programStages']:
                    prog_ps_id=[ps['id']]
                    prog_ps_name=[ps['name']]
                    for ps_de in ps['programStageDataElements']:
                        de=ps_de['dataElement']
                        de_name=[de['name']]
                        de_id=[de['id']]
                        de_code=[None]
                        if 'code' in de:
                            de_code=[de['code']]
                        de_value_type=[de['valueType']]
        
                        de_dict={'PROGRAM_T_UID':prog_id,
                                 'PROGRAM_T_NAME':prog_name,
                                 'PROGRAM_T_STAGE_UID':prog_ps_id,'PROGRAM_T_STAGE_NAME':prog_ps_name,
                                 'DE_UID':de_id,'DE_NAME':de_name,'DE_CODE':de_code,'DE_VALUE_TYPE':de_value_type,                                                          
                                }
                        if 'optionSet'in de:
                            for option in de['optionSet']['options']:
                                option_name=[option['name']]
                                option_id=[option['id']]
                                option_code=[None]
                                if 'code' in option:
                                    option_code=[option['code']]
                                
                                
                                option_dict={'OPTION_UID':option_id,
                                            'OPTION_NAME':option_name,
                                            'OPTION_CODE':option_code}
                                
                                program_df_program_stage_list.append(pd.DataFrame({**de_dict,**option_dict}))
                        else:
                            option_dict={'OPTION_UID':[None],
                                        'OPTION_NAME':[None],
                                        'OPTION_CODE':[None]}
                            program_df_program_stage_list.append(pd.DataFrame({**de_dict,**option_dict}))
                            
        if program_df_attributes_list:
            program_df_attributes=pd.concat(program_df_attributes_list,
                                            ignore_index=True)
        else:
            program_df_attributes=pd.DataFrame(columns=['PROGRAM_T_UID','PROGRAM_T_NAME','PROGRAM_T_ETI_DATA_UID',
                                                          'PROGRAM_T_ETI_DATA_NAME','ETI_ATT_UID','ETI_ATT_NAME',
                                                          'ETI_ATT_VALUE_TYPE'])
        if program_df_program_stage_list:
            program_df_program_stage=pd.concat(program_df_program_stage_list,
                                            ignore_index=True)
        else:
            program_df_program_stage=pd.DataFrame(columns=['PROGRAM_T_UID','PROGRAM_T_NAME','PROGRAM_T_STAGE_UID',
                                                           'PROGRAM_T_STAGE_NAME','DE_UID','DE_NAME','DE_CODE',
                                                           'DE_VALUE_TYPE'])
        return {"programTracker_programTrackedEntityAttributes":program_df_attributes,
                "programTracker_programStagesDataElements":program_df_program_stage}
    
    
    def _program_event_data_json_to_df(self,json):
        
        empty_de_dict={'DE_UID':[None],'VALUE':[None]}
        event_dict_labels={
                            'PROGRAM_T_UID':'program',
                            'PROGRAM_T_STAGE_UID':'programStage',
                            'PROGRAM_TYPE':'programType',
                            'STATUS':'status',
                            'OU_UID':'orgUnit',
                            'DATE_REGISTER':'eventDate',
                            'STORED_BY':'completedBy',
                            'EVENT_UID':'event'
                            }
        
        program_df_data_event_df_list=[]
        for event in json:
            event_dict={}
            for key,item in event_dict_labels.items():
                if item in event:
                    event_dict[key]=[event[item]]
                else:
                    event_dict[key]=[None]
    
    
            if 'dataValues' in event:
                if event['dataValues']:
                    for de_info in event['dataValues']:
                        program_df_data_event_df_list.append(pd.DataFrame({**event_dict,**{'DE_UID':[de_info['dataElement']],'VALUE':[de_info['value']]}}))
                else:
                    program_df_data_event_df_list.append(pd.DataFrame({**event_dict,**empty_de_dict}))
            else:
                program_df_data_event_df_list.append(pd.DataFrame({**event_dict,**empty_de_dict}))


            
        if program_df_data_event_df_list:
            program_df_data_event_df=pd.concat(program_df_data_event_df_list,
                                               ignore_index=True)
        else:
            program_df_data_event_df=pd.DataFrame(columns=['PROGRAM_T_UID','PROGRAM_T_STAGE_UID','PROGRAM_TYPE','STATUS',
                                                            'OU_UID','DATE_REGISTER','STORED_BY','EVENT_UID',
                                                           'DE_UID','VALUE'])
        return program_df_data_event_df