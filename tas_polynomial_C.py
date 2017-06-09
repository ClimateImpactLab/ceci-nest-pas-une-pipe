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

BASELINE_FILE = (
    '/global/scratch/jsimcock/gcp/climate/hierid/' +
    '{rcp}/tas/tas-polynomials/{model}/' +
    'tas_tas-polynomials_{model}_{year}.nc')

# BCSD_pattern_files = (
#     '/global/scratch/mdelgado/nasa_bcsd/pattern/SMME_surrogate/' +
#     '{rcp}/{variable}/{model}/' +
#     '{variable}_BCSD_{model}_{rcp}_r1i1p1_{season}_{{year}}.nc')

WRITE_PATH = (
    '/global/scratch/jsimcock/gcp/climate/hierid/{rcp}/{variable}/tas-polynomials-C/{model}/' +
    'tas_tas-polynomials_{model}_{year}.nc')

ADDITIONAL_METADATA = dict(
    description=__file__.__doc__,
    author=__author__,
    contact=__contact__,
    version=__version__,
    repo='https://github.com/ClimateImpactLab/ceci-nest-pas-une-pipe',
    file='/ceci-nest-pas-une-pipe/tas-polynomials-C.py',
    execute='',
    project='gcp', 
    team='climate',
    geography='hierid',
    weighting='popwt',
    frequency='daily')


def tas_k_to_c(ds):
    '''
    Average seasonal tas
    '''
    ds['tas'] = (ds['tas'] - 273.15)
    return ds


JOBS = [
    dict(variable='tas', transformation=tas_k_to_c)]

PERIODS = (
    #[dict(rcp='historical', pername='annual', year=y) for y in range(1981, 2006)]
    [dict(rcp='rcp85', pername='annual', year=y) for y in range(2006, 2010)]
    )

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
    {'agglev': 'hierid', 'aggwt': 'popwt'}]


JOB_SPEC = [JOBS, PERIODS, MODELS, AGGREGATIONS]

def run_job(
        metadata,
        variable,
        transformation,
        rcp,
        pername,
        year,
        model,
        agglev,
        aggwt,
        weights=None):

    # Add to job metadata
    # metadata.update(dict(
    #     time_horizon='{}-{}'.format(years[0], years[-1])))

    # pattern_file = BCSD_pattern_files.format(**metadata)
    write_file = WRITE_PATH.format(**metadata)

    # Get transformed data

    # Get transformed data
    base = BASELINE_FILE.format(rcp=rcp,model=model,year=year)
    
    logger.debug('{} {} - applying transform'.format(model, year))

    with xr.open_dataset(base) as ds:
        ds = ds.pipe(transformation)

        # Reshape to regions
        # logger.debug('{} - reshaping to regions'.format(model))
        # if not agglev.startswith('grid'):
        #     ds = weighted_aggregate_grid_to_regions(
        #             annual, variable, aggwt, agglev, weights=weights)

        # # Update netCDF metadata
        # logger.debug('{} udpate metadata'.format(model))
        # ds.attrs.update(**metadata)

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

        run_job(metadata=metadata, **job)


if __name__ == '__main__':
    main()
