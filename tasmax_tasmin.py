import os
import click
import pprint
import logging
import xarray as xr
import pandas as pd

import utils
from climate_toolbox import (
    load_bcsd,
    load_baseline,
    weighted_aggregate_grid_to_regions)

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)

logger = logging.getLogger('uploader')
logger.setLevel('DEBUG')

__author__ = 'Michael Delgado'
__contact__ = 'mdelgado@rhg.com'
__version__ = '0.1.0'

BCSD_orig_files = os.path.join(
    '/global/scratch/jiacany/nasa_bcsd/raw_data/{rcp}/{model}/{variable}',
    '{variable}_day_BCSD_{rcp}_r1i1p1_{model}_{{year}}.nc')

WRITE_PATH = os.path.join(
    '/global/scratch/jsimcock/gcp/climate/{agglev}/{rcp}/{variable}/tasmax_over_95F',
    '{variable}_{agglev}_{aggwt}_{model}_{pername}.nc')

ADDITIONAL_METADATA = dict(
    description=__file__.__doc__,
    author=__author__,
    contact=__contact__,
    version=__version__,
    repo='https://github.com/ClimateImpactLab/ceci-nest-pas-une-pipe',
    file='/ceci-nest-pas-une-pipe/tasmax_tasmin.py',
    execute='',
    project='gcp', 
    team='climate',
    geography='',
    weighting='areawt',
    frequency='20 year')


def tasmax_over_95F(ds):
    '''
    Average seasonal tas
    '''
    
    return ds.tasmax.where((ds.tasmax - 273.15) > 35).count(dim='time')

def tasmin_under_32F(ds):
    '''
    Count of days with tasmin under 32F/0C
    '''
    return ds.tasmin.where((ds.tasmin- 273.15) < 0).count(dim='time')


JOBS = [
    dict(variable='tasmax', transformation=tasmax_over_95F), 
    #dict(variable='tasmin', transformation_name='tasmin_under_32F', transformation=tasmin_under_32F)
    ] 


PERIODS = [
    dict(rcp='rcp45', pername='2020', years=list(range(2020, 2040))),
    dict(rcp='rcp45', pername='2040', years=list(range(2040, 2060))),
    dict(rcp='rcp45', pername='2080', years=list(range(2080, 2100)))]


YEARS = []

MODELS = list(map(lambda x: dict(model=x), [
    'ACCESS1-0',
    'bcc-csm1-1',
    'BNU-ESM',
    'CanESM2',
    'CCSM4',
    'CESM1-BGC',
    'CNRM-CM5',
    'CSIRO-Mk3-6-0',
    'GFDL-CM3',
    'GFDL-ESM2G',
    'GFDL-ESM2M',
    'IPSL-CM5A-LR',
    'IPSL-CM5A-MR',
    'MIROC-ESM-CHEM',
    'MIROC-ESM',
    'MIROC5',
    'MPI-ESM-LR',
    'MPI-ESM-MR',
    'MRI-CGCM3',
    'inmcm4',
    'NorESM1-M']))


AGGREGATIONS = [
    {'agglev': 'ISO', 'aggwt': 'areawt'},
    {'agglev': 'hierid', 'aggwt': 'areawt'}]


JOB_SPEC = [JOBS, PERIODS, MODELS, AGGREGATIONS]

def run_job(
        metadata,
        variable,
        transformation,
        rcp,
        pername,
        years,
        model,
        agglev,
        aggwt,
        weights=None):

    # Add to job metadata
    metadata.update(dict(
        time_horizon='{}-{}'.format(years[0], years[-1])))

    bcsd_file = BCSD_orig_files.format(**metadata)
    write_file = WRITE_PATH.format(**metadata)

    # Get transformed data
    total = []

    for year in years:


        

        # Get transformed data
        bcsd_file = bcsd_file.format(year=year)
        logger.debug('attempting to load pattern file: {}'.format(bcsd_file))
        annual = load_bcsd(bcsd_file, variable, broadcast_dims=('time',))
        
        logger.debug('{} {} - applying transform'.format(model, year))
        annual = xr.Dataset({
            variable: annual.pipe(transformation)})

        logger.debug('{} {} - adding to running total'.format(model, year))
        total.append(annual)

    ds = xr.concat(total, dim=pd.Index(years, name='year')).mean(dim='year')

    # load baseline


    # Reshape to regions
    logger.debug('{} - reshaping to regions'.format(model))
    if not agglev.startswith('grid'):
        ds = weighted_aggregate_grid_to_regions(
                ds, variable, aggwt, agglev, weights=weights)

    # Update netCDF metadata
    logger.debug('{} udpate metadata'.format(model))
    ds.attrs.update(**metadata)

    # Write output
    logger.debug('attempting to write to file: {}'.format(write_file))
    if not os.path.isdir(os.path.dirname(write_file)):
        os.makedirs(os.path.dirname(write_file))

    ds.to_netcdf(write_file)


@click.command()
@click.option('--prep', is_flag=True, default=False)
@click.option('--run', is_flag=True, default=False)
@click.option('--job_id', type=int, default=None)
def main(prep=False, run=False, job_id=None):
    if prep:
        utils._prep_slurm(filepath=__file__, job_spec=JOB_SPEC)

    elif run:
        utils.run_slurm(filepath=__file__, job_spec=JOB_SPEC)

    if job_id is not None:
        job = utils.get_job_by_index(JOB_SPEC, job_id)

        logger.debug('Beginning job\nid:\t{}\nkwargs:\t{}'.format(
            job_id,
            pprint.pformat(job, indent=2)))

        metadata = {k: v for k, v in ADDITIONAL_METADATA.items()}
        metadata.update(job)
        print(job)
        run_job(metadata=metadata, **job)


if __name__ == '__main__':
    main()
