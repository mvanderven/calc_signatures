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

import time 

#%% Define pathos functions 

def run_parallel(input_dir):
    
    print(' [INFO] start run')
    
    #### 1 - PATHS     
    fn_gauges = input_dir / "V1_grdc_efas_selection-cartesius.csv" 
    dir_gauges = input_dir / 'V1_gauge_obs'
    
    #### 2 - LOAD MODEL DATA  
    print('\n [INFO] load model data')
    time_load_model = time.time() 
    files = [file for file in input_dir.glob('efas_timeseries_*_buffer_4.csv')] 
    ## DASK 
    df_model = dd.read_csv(files) 
    print(' [INFO] model data loaded - finished in {:.2f} minutes'.format( (time.time()-time_load_model)/60 ))
        
    #### 3 - GAUGE METADATA
    df_gauges = pd.read_csv(fn_gauges, index_col=0) 
    # gauge_ids = df_gauges.index.values   
    gauge_ids = df_gauges.sample(n=10).index.values
        
    #### 4 - SETUP PATHOS POOL
    p = pa.pools.ParallelPool(n_nodes=2) 
    
    #### 5 - SET RUN PARAMETERS 
    list_model = [df_model] * len(gauge_ids)        ## 1 
    list_dir = [input_dir] * len(gauge_ids)         ## 0, 1 
    # list_sim_files = [files] * len(gauge_ids)       ## 0 
    list_gauge_dir = [dir_gauges] * len(gauge_ids)  ## 0 1 
        
    #### 6 - RUN POOL
    print('\n [START] parellel run')
    time_parallel = time.time() 
    
    # results_parallel = p.map(pa_calc_signatures_0, gauge_ids, list_sim_files, list_gauge_dir, list_dir) 
    results_parallel = p.map(pa_calc_signatures_1, gauge_ids, list_model, list_gauge_dir, list_dir) 
    
    print(' [END] parellel run - finished in {:.2f} minutes'.format( (time.time()-time_parallel)/60.  )) 
    
    #### 7 - COLLECT RESULTS 
    return  pd.concat(results_parallel)


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
    
    print('Total time: {:.2f} minutes'.format( (time.time() - time_total)/60. ))

















