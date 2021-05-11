# -*- coding: utf-8 -*-
"""
Created on Fri Apr 30 16:00:48 2021

@author: mvand
"""

import pandas as pd 
from pathlib import Path
import xarray as xr 

from signatures_utils import pa_calc_signatures
# from signatures_utils_nc import pa_calc_signatures 

# import pathos as pa 
# import dask.dataframe as dd 
# import numpy as np 

import time 

from pathos.threading import ThreadPool as Pool

## TO DO -- rewrite voor nc files 


#%% Define pathos functions 

def run_parallel(input_dir):
    
    print(' [INFO] start run')
    
    ## set paths     
    fn_gauges = input_dir / "V1_grdc_efas_selection-cartesius.csv" 
    dir_gauges = input_dir / 'V1_gauge_obs'

    ## TEST PC
    gauge_ids = [6221500, 6731010] 
    
    ## get list of gauge ids 
    # df_gauges = pd.read_csv(fn_gauges, index_col=0)   
    
    ## TEST SCALE     
    # gauge_ids = df_gauges.sample(n=50).index.values
    # splitted_gauge_ids = np.array_split(gauge_ids, 5) 
    
    ## LARGE SCALE  
    #gauge_ids = df_gauges.index.values
    #splitted_gauge_ids = np.array_split(gauge_ids, 75)  
    
    ## get empty dataframe for total collection 
    # df_out = pd.DataFrame()
    
    print('\n [START] parellel run')
    time_parallel = time.time() 
    
    list_wkdir = [input_dir] * len(gauge_ids) 
    list_gauge_dir = [dir_gauges] * len(gauge_ids)
    list_gauge_file = [fn_gauges] * len(gauge_ids)
    
    ## set up pool 
    p = Pool() 
    
    ## conduct calculations
    results = p.map(pa_calc_signatures, gauge_ids, list_wkdir, list_gauge_dir, list_gauge_file) 
                
    print(' [END] parellel run - finished in {:.2f} minutes'.format( (time.time()-time_parallel)/60.  )) 
    return results


if __name__ == '__main__':
    
    time_total = time.time() 

    ## cartesius environment 
    #input_dir = Path("/scratch-shared/mizzivdv/signatures_V1_input/")
    input_dir = Path(r"C:\Users\mvand\Documents\Master EE\Year 4\Thesis\data\dev") 
    
    ## run 
    results = run_parallel(input_dir) 
    
    ## output 
    # fn_out = input_dir / 'signatures_tw-test_drop.csv'
    # results.to_csv(fn_out)
    
    print(' [INFO] Total time: {:.2f} minutes'.format( (time.time() - time_total)/60. ))

















