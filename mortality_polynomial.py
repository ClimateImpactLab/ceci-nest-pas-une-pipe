'''

Marginal impact of temperature on mortality 

Values are annual/daily expected damage resolved to GCP hierid/country l
evel region. 

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
        do_socio_thing,
        do_gdp_thing,
        get_gammas,
        pval_thing,
        rebase1,
        clipping,
        goodmoney,
        adaptation,
        rescale,
        rebase2,
        combine,
        costs,
        aggregate
        )


FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)

logger = logging.getLogger('uploder')
logger.setLevel('DEBUG')


__author__ = 'Justin Simcock'
__contact__ = 'jsimcock@rhg.com'
__version__ = '0.1.0'



CLIMATE_DATA_FILE = ('/global/scratch/jiacany/gcp/...')

SSP_FILE = ('/global/scratch/jsimcock/...')

GDP_FILE = ('/global/scratch/jsimcock/...')

GAMMAS_FILE = ('/global/scratch/jsimcock/...')

WRITE_FILE = ('/global/scratch/jsimcock/...')



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
    variable=''
    )


mortality_flags = [
                dict()
                ]




pvals = pval_thing()


PERIODS = [dict(scenario='rcp45', year=y) for y in range(1982, 2100)]


#we want to do a realization of all models for the periods at a given set of periods
JOB_SPEC = [pvals, PERIODS]

def mortality(gammas, climate_data, gdp_data, mortality_flags):
    '''
    Calculates the IR level daily/annual effect of temperature on Mortality Rates

    Paramaters
    ----------
    gammas: Numpy Array 
        1xN array where N=#of covariates
        floats representing the coefficients on each of the covariates

    climate_data: Xarray Dataset
        IRX365 matrix representing daily climate data

    gdp_data: Numpy Array
        1xIR array

    mortality_flags: dit
        set of methods optionally applied to adjust final impact 


    Returns
    -------

    Xarray Dataset 


    '''

    age1 = gammas[0:12]
    age2 = gammas[12:24]
    age3 = gammas[24:35]

    impact = gammas*climate_data*gdp_data


    for flag in mortality_flags:
        impact = lambda impact: flag(impact)

    return impact





def run_job(metadata, 
            transformation, 
            variable,
            model
            year, 
            scenario,
            pval,
            SSP,
            socio,
            mortality_flags

    ):


    climate_file = CLIMATE_DATA_FILE.format(variable=variable,scenario= scenario, model=model, year=year)
    ssp_file = SSP_FILE.format(**metadata)
    gdp_file = GDP_FILE.format(**metadata)
    write_file = WRITE_FILE.format(**metadata)
    gamma_file = GAMMAS_FILE.format(**metadata)


    if os.path.isfile(write_file):
        return


    gammas = et_gammas(gamma_file, pval)
    gdp_data = do_gdp_thing(gdp_file)
    climate_data = xr.open_dataset(climate_file)



    logger.debug('calculating impact for {} {} {}'.format(scenario,model, year))
    impact_ds = mortality(gammas, climate_data, gdp_data, mortality_flags)

    logger.debug('udpate metadata for impact calculation {} {} {} '.format(scenario,model, year))
    impact_ds.attrs.update(ADDITIONAL_METADATA)
    impact_ds.attrs['pval'] = pval


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










