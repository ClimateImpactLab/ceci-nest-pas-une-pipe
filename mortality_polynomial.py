'''

Marginal impact of temperature on mortality 

Values are annual/daily expected damage resolved to GCP hierid/country level region. 

'''

import os
import click
import pprint
import logging
import xarray as xr
import pandas as pd
import datafs


import utils
from impact_toolbox import (
        compute_gdp_covariates,
        compute_climate_covariates,
        compute_betas,
        get_annual_climate,
        get_gammas,
        )


FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)

logger = logging.getLogger('uploder')
logger.setLevel('DEBUG')


__author__ = 'Justin Simcock'
__contact__ = 'jsimcock@rhg.com'
__version__ = '0.1.0'



BCSD_orig_files = (
    '/global/scratch/jiacany/nasa_bcsd/raw_data/{scenario}/{model}/tas/' +
    'tas_day_BCSD_{scenario}_r1i1p1_{model}_{year}.nc')

GDP_FILE = ('/global/scratch/jsimcock/data_files/covars/gdppc-merged-baseline.csv')

GAMMAS_FILE = ('/global/scratch/jsimcock/data_files/covars/global_interaction_Tmean-POLY-4-AgeSpec.csvv')

baseline_climate_path = ('/global/scratch/jiacany/nasa_bcsd/raw_data/{scenario}/{model}/tas/' +
    'tas_day_BCSD_{scenario}_r1i1p1_{model}_2015.nc')

WRITE_PATH = (
    '/global/scratch/jsimcock/gcp/mortality/{scenario}/mortality_impacts_{model}_{year}.nc')



ADDITIONAL_METADATA = dict(    
    oneline=oneline,
    description=description,
    author=__author__,
    contact=__contact__,
    version=__version__,
    repo='https://github.com/ClimateImpactLab/ceci-nest-pas-une-pipe',
    file='/mortality_polynomial.py',
    execute='python mortality_polynomial.py --run',
    project='gcp', 
    team='impacts-mortality',
    frequency='daily',
    variable='',
    dependencies= [GDP_FILE, GAMMAS_FILE],
    pvale= [0.5]
    )


MODELS = list(map(lambda x: dict(model=x), [
    'ACCESS1-0',
    # 'bcc-csm1-1',
    # 'BNU-ESM',
    # 'CanESM2',
    # 'CCSM4',
    # 'CESM1-BGC',
    # 'CNRM-CM5',
    # 'CSIRO-Mk3-6-0',
    # 'GFDL-CM3',
    # 'GFDL-ESM2G',
    # 'GFDL-ESM2M',
    # 'IPSL-CM5A-LR',
    # 'IPSL-CM5A-MR',
    # 'MIROC-ESM-CHEM',
    # 'MIROC-ESM',
    # 'MIROC5',
    # 'MPI-ESM-LR',
    # 'MPI-ESM-MR',
    # 'MRI-CGCM3',
    # 'inmcm4',
    # 'NorESM1-M'
    ]))



PERIODS = [dict(scenario='historical', year=y) for y in range(1981, 2006)] + 
            [dict(scenario='rcp85', year=y) for y in range(2006, 2100)]


#we want to do a realization of all models for the periods at a given set of periods
JOB_SPEC = [PERIODS, MODELS]



def mortality_annual(gammas_path, baseline_climate_path, gdp_data_path, annual_climate_paths, year=None):
    '''
    Calculates the IR level daily/annual effect of temperature on Mortality Rates

    Paramaters
    ----------
    gammas_path: str
        path to csvv

    climate_data: str
        path to baseline year climate dataset

    gdp_data: str
        path to gdp_dataset

    annual_climate_paths: list
        list of paths for climate data sets

    mortality_flags: dit
        set of methods optionally applied to adjust final impact 


    Returns
    -------

    Xarray Dataset 


    '''

    betas = compute_betas(baseline_climate_path,gdp_data_path, gammas_path)
    climate = get_annual_climate(annual_climate_paths,year, 4)

    impact = xr.Dataset()
    
    impact['mortality_impact'] = (betas['tas']*climate['tas'] + betas['tas-poly-2']*climate['tas-poly-2'] + 
            betas['tas-poly-3']*climate['tas-poly-3'] + betas['tas-poly-3']*climate['tas-poly-3'])


    return impact

def run_job(metadata, 
            model
            year, 
            scenario,
            WRITE_PATH,
            gamma_file,
            baseline_climate_path, 
            baseline_gdp_path,

            ):


    
    write_file = WRITE_PATH.format(scenario=scenario, model=model, year=year)

    if os.path.isfile(write_file):
        return


    logger.debug('calculating impact for {} {} {}'.format(scenario,model, year))
    impact_ds = mortality_annual(gamma_file, climate_data, gdp_data, year)

    logger.debug('udpate metadata for impact calculation {} {} {} '.format(scenario,model, year))
    impact_ds.attrs.update(ADDITIONAL_METADATA)


    # Write output
    logger.debug('attempting to write to file: {}'.format(write_file))
    if not os.path.isdir(os.path.dirname(write_file)):
        os.makedirs(os.path.dirname(write_file))
    

    impact_ds.to_netcdf(write_file)



def onfinish():
    print('done!')


main = utils.slurm_runner(
    filepath=__file__,
    job_spec=JOB_SPEC,
    run_job=run_job,
    onfinish=onfinish)

if __name__ == '__main__':
    main()
    










