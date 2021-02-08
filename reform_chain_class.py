"""
run example:
res=run_reform_chain(filepath='', filename=chain,
                     output_filepath='',
                      test_output_filename='test_check.csv',
                      output_filename='test.csv',
                      type_report='agg',
                      chain_sep='=>',
                      channel_sep='_>>_',
                      len_ga_channel=5,
                    save_check=1)
"""


import csv
# import itertools
import pandas as pd
import numpy as np
# from functools import reduce
from tqdm import tqdm_notebook as tqdm

class ReformChain:
    def __init__(self,filepath='',
                     filename='',
                     output_filepath='',
                     output_filename='',
                     test_output_filename='',
                     type_report='agg',
                     chain_sep='=>',
                     channel_sep='_>>_',
                     len_ga_channel=5,
                    save_check=1):
        self.filepath=filepath
        self.filename=filename
        self.output_filepath=output_filepath
        self.output_filename=output_filename
        self.test_output_filename=test_output_filename
        self.type_report=type_report
        self.chain_sep=chain_sep
        self.channel_sep=channel_sep
        self.len_ga_channel=len_ga_channel
        self.save_check=save_check
        
        
        
    def open_csv_with_clid(self,path=''):
        with open(path) as f:
            reader = csv.reader(f)
            listrow=[]
            for row in reader:
                listrow.append(row)
        return pd.DataFrame(listrow[1:], columns=listrow[0])
    
    def get_new_chain_list_for_agg(self,goal_data=None):
        new_chain_list=[]
        del_inx_list_upper=[]
        for chain in tqdm(list(goal_data.user_path)):
            list_chan=chain.split(self.chain_sep)
            #получаем список кликовых каналов 
            index_list=self.get_index(list_chan)
            #     если в цепи нет кликовых каналов:
            if index_list==[]:
                new_chain_list.append(chain)
                del_inx_list_upper.append([])
            else:
                del_inx_l=[]# индексы каналов, которые будут удалены из цепи
                for inx in index_list:                
                    ga_name=(self.channel_sep).join(list_chan[inx].split(self.channel_sep)[1:self.len_ga_channel+1])# вытаскиваем имя источника га(source/medium/(campaign))
                    if inx!=len(list_chan)-1:# если кликовый канал не является последний элементом цепи
                        if list_chan[inx+1]==ga_name:
                            del_inx_l.append(inx+1)
                del_inx_list_upper.append(del_inx_l)
                for di in reversed(del_inx_l):#удаляем каналы из цепи
                    del list_chan[di]
                res_chain=(self.chain_sep).join(list_chan) # собираем обратно    
                new_chain_list.append(res_chain)
        return new_chain_list,del_inx_list_upper

    
    def get_new_chain_list(self,goal_data=None ):
        new_chain_list=[]
        new_timeline_list=[]
        del_inx_list_upper=[]
        for chain,hittime in tqdm(zip(list(goal_data.user_path), list(goal_data.timeline))):
            list_chan=chain.split(self.chain_sep)
            list_timeline=hittime.split(self.chain_sep)
        #     print(list_chan)
            #получаем список кликовых каналов 
            index_list=self.get_index(list_chan)
            #     если в цепи нет кликовых каналов:
            if index_list==[]:
                new_chain_list.append(chain)
                new_timeline_list.append(hittime)
                del_inx_list_upper.append([])
            else:
                del_inx_l=[]# индексы каналов, которые будут удалены из цепи
                for inx in index_list:                
                    ga_name=(channel_sep).join(list_chan[inx].split(channel_sep)[1:self.len_ga_channel+1])# вытаскиваем имя источника га(source/medium/(campaign))
                    if inx!=len(list_chan)-1:# если кликовый канал не является последний элементом цепи
                        if list_chan[inx+1]==ga_name:
                            del_inx_l.append(inx+1)
                del_inx_list_upper.append([del_inx_l])
                for di in reversed(del_inx_l):#удаляем каналы из цепи
                    del list_chan[di]
                    del list_timeline[di]
                res_chain=(self.chain_sep).join(list_chan) # собираем обратно    
                res_timeline=(self.chain_sep).join(list_timeline)
                new_chain_list.append(res_chain)
                new_timeline_list.append(res_timeline)
        return new_chain_list,new_timeline_list,del_inx_list_upper
    
    ## поиск канала в цели который начинаеттся с "click:"
    def get_index(self,list_chan):
        """
        list_chan - цепь рассплитованная по сепаратору
        return - список индексов "кликовых" каналов
        """
        index_list=[]
        for ind,chan in enumerate(list_chan):
            if chan.find('click_')==0:
                index_list.append(ind)
        return index_list
    
    def get_verification_output(self,data=None,del_inx=[],new_chain=[]):
        data['new_path']=new_chain
        data['len_orig']=data.user_path.apply(lambda row:len(row.split(self.chain_sep)))
        data['len_new_chain']=data.new_path.apply(lambda row:len(row.split(self.chain_sep)))
        data['index_to_dalite']=del_inx
        data['check']=data.len_orig!=data.len_new_chain
        data.check=data.check.astype(int)
#         if self.save_check==1:
        data.to_csv(self.output_filepath+self.test_output_filename, sep=',', index=False,float_format='%.100f')
        return data
    
    
    def safe_result_data(self,
                         data=None, new_chain=[],new_timeline_list=[],
                          ):
        new_data=data.copy()
        new_data.user_path=new_chain
        if self.type_report=='detailed':
            new_data.timeline=new_timeline_list

        elif self.type_report=='agg':
            new_data=self.regroup_data(new_data)
        new_data.to_csv(self.output_filepath+self.output_filename,sep=',', index=False, float_format='%.100f')
        return data
    
    def regroup_data(self,res=None):
        a=res.groupby('user_path').sum()
        a.reset_index(inplace=True)
        return a
    
def run_reform_chain(filepath='',# папка с исходными данными
                     filename='', #название исходного файла
                     output_filepath='',# путь выходного файла
                     output_filename='', #название выходного файла
                     test_output_filename='', #название проверочного файла
                     type_report='agg', #тип обрабатываемого отчета
                     chain_sep='=>',
                     channel_sep='_>>_',
                     len_ga_channel=5,
                    save_check=1,):

    rch=ReformChain(filepath,filename,output_filepath,output_filename,test_output_filename,type_report,
                    chain_sep,channel_sep,len_ga_channel,save_check)
    chain_data=rch.open_csv_with_clid(path=filepath+filename)
    chain_data['count']=chain_data['count'].astype(int)
    if type_report=='agg':
        new_chain,del_inx_list_upper=rch.get_new_chain_list_for_agg(goal_data=chain_data)
        result=rch.safe_result_data(
                                data=chain_data,
                                new_chain=new_chain,)
        if rch.save_check==1:
            rch.get_verification_output(data=chain_data,del_inx=del_inx_list_upper,new_chain=new_chain,)
    elif type_report=='detailed':
        new_chain,new_timeline_list,del_inx_list_upper=rch.get_new_chain_list(goal_data=chain_data)
        
        result=rch.safe_result_data(data=chain_data, new_chain=new_chain,new_timeline_list=new_timeline_list,)
        if rch.save_check==1:
            rch.get_verification_output(data=chain_data,del_inx=del_inx_list_upper,new_chain=new_chain,)
    return result
