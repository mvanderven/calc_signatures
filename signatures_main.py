# -*- coding: utf-8 -*-
"""
Created on Fri Apr 30 16:00:48 2021

@author: mvand
"""

import pandas as pd 
from pathlib import Path

from signatures_utils import pa_calc_signatures_1, read_gauge_data, pa_calc_signatures_0
import pathos as pa 
import dask.dataframe as dd 
import numpy as np 

import time 

#%% Define pathos functions 

def run_parallel(input_dir):
    
    print(' [INFO] start run')
    
    ## set paths     
    fn_gauges = input_dir / "V1_grdc_efas_selection-cartesius.csv" 
    dir_gauges = input_dir / 'V1_gauge_obs'
    
    #### load model data with dask   
    files = [file for file in input_dir.glob('efas_timeseries_*_buffer_4.csv')] 
    df_model = dd.read_csv(files) 
        
    ## get list of gauge ids 
    df_gauges = pd.read_csv(fn_gauges, index_col=0) 
    
    ## TEST PC
    gauge_ids = df_gauges.sample(n=10).index.values
    splitted_gauge_ids = np.array_split(gauge_ids, 5)
    
    ## TEST SCALE     
    # gauge_ids = df_gauges.sample(n=50).index.values
    # splitted_gauge_ids = np.array_split(gauge_ids, 5) 
    
    ## LARGE SCALE  
    #gauge_ids = df_gauges.index.values
    #splitted_gauge_ids = np.array_split(gauge_ids, 75)  
    
    ## get empty dataframe for total collection 
    df_out = pd.DataFrame()
    
    print('\n [START] parellel run')
    time_parallel = time.time() 
    
    ## set up pool 
    p = pa.pools.ParallelPool(n_nodes=2)
    
    ## loop over small groups 
    for i, gauge_list in enumerate(splitted_gauge_ids):
        
        print(' [INFO] loop {} - process {} gauges'.format(int(i+1), len(gauge_list)))
        
        start_loop = time.time() 
        
        #list_sim_files = [files] * len(gauge_list)  
        list_model = [df_model] * len(gauge_list)
        list_dir = [input_dir] * len(gauge_list) 
        list_gauge_dir = [dir_gauges] * len(gauge_list) 
        
        #### results_parallel = p.map(pa_calc_signatures_0, gauge_ids, list_sim_files, list_gauge_dir, list_dir) 
        results = p.map(pa_calc_signatures_1, gauge_list, list_model, list_gauge_dir, list_dir) 
        
        print(' [INFO] Finished loop {}/{} in {:.2f} mins'.format(int(i+1), len(splitted_gauge_ids),
                                                          (time.time()-start_loop)/60 ))
        _df = pd.concat(results) 
        
        ## save intermediate results 
        tmp_fn = input_dir / 'signatures_loop_{}.csv'.format(int(i+1))
        _df.to_csv(tmp_fn)
        
        ## append to main output file 
        df_out.append(_df) 
            
    print(' [END] parellel run - finished in {:.2f} minutes'.format( (time.time()-time_parallel)/60.  )) 
    return df_out 


if __name__ == '__main__':
    
    time_total = time.time() 

    ## cartesius environment 
    #input_dir = Path("/scratch-shared/mizzivdv/signatures_V1_input/")
    input_dir = Path(r"C:\Users\mvand\Documents\Master EE\Year 4\Thesis\data\dev") 
    
    ## run 
    results = run_parallel(input_dir) 
    
    ## output 
    fn_out = input_dir / 'signatures_tw-test_new.csv'
    results.to_csv(fn_out)
    
    print(' [INFO] Total time: {:.2f} minutes'.format( (time.time() - time_total)/60. ))

















