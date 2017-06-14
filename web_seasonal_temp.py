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
    '{rcp}/tas/tas-polynomials/' +
    'tas_tas-polynomials_{baseline_model}_{year}.nc')

# BCSD_pattern_files = (
#     '/global/scratch/mdelgado/nasa_bcsd/pattern/SMME_surrogate/' +
#     '{rcp}/{variable}/{model}/' +
#     '{variable}_BCSD_{model}_{rcp}_r1i1p1_{season}_{{year}}.nc')

WRITE_PATH = (
    '/global/scratch/jsimcock/gcp/climate/hierid/{rcp}/{variable}/tas-polynomials-C/' +
    'tas_tas-polynomials_{model}_{year}.nc')

ADDITIONAL_METADATA = dict(
    description=__file__.__doc__,
    author=__author__,
    contact=__contact__,
    version=__version__,
    repo='https://github.com/ClimateImpactLab/ceci-nest-pas-une-pipe',
    project='gcp', 
    team='climate',
    geography='hierid',
    weighting='popwt',
    frequency='daily')


def average_seasonal_temp_pattern(ds):
    '''
    Average seasonal tas
    '''
    return (ds.tas - 273.15).mean(dim='day')


JOBS = [
    dict(variable='tas', transformation=average_seasonal_temp_pattern)]

PERIODS = [
    dict(rcp='historical', pername='1986', years=list(range(1986, 2006))),
    dict(rcp='rcp45', pername='2020', years=list(range(2020, 2040))),
    dict(rcp='rcp45', pername='2040', years=list(range(2040, 2060))),
    dict(rcp='rcp45', pername='2080', years=list(range(2080, 2100)))]

YEARS = []

MODELS = list(map(lambda x: dict(model=x[0], baseline_model=x[1]), [
        ('pattern1','MRI-CGCM3'),
        ('pattern2','GFDL-ESM2G'),
        ('pattern3','MRI-CGCM3'),
        ('pattern4','GFDL-ESM2G'),
        ('pattern5','MRI-CGCM3'),
        ('pattern6','GFDL-ESM2G'),
        ('pattern28','GFDL-CM3'),
        ('pattern29','CanESM2'),
        ('pattern30','GFDL-CM3'),
        ('pattern31','CanESM2'), 
        ('pattern32','GFDL-CM3'), 
        ('pattern33','CanESM2')]))

SEASONS = list(map(lambda x: dict(season=x),[ 'DJF', 'MAM', 'JJA', 'SON']))

AGGREGATIONS = [
    {'agglev': 'ISO', 'aggwt': 'areawt'},
    {'agglev': 'hierid', 'aggwt': 'areawt'}]


JOB_SPEC = [JOBS, PERIODS, MODELS, SEASONS, AGGREGATIONS]

def run_job(
        metadata,
        variable,
        transformation,
        rcp,
        pername,
        years,
        model,
        baseline_model,
        season,
        agglev,
        aggwt,
        weights=None):

    # Add to job metadata
    metadata.update(dict(
        time_horizon='{}-{}'.format(years[0], years[-1])))

    baseline_file = BASELINE_FILE.format(**metadata)
    pattern_file = BCSD_pattern_files.format(**metadata)
    write_file = WRITE_PATH.format(**metadata)

    # Get transformed data
    total = []

    for year in years:

        # Get transformed data
        pattf = pattern_file.format(year=year)
        logger.debug('attempting to load pattern file: {}'.format(pattf))
        annual = load_bcsd(pattf, variable, broadcast_dims=('day',))
        
        logger.debug('{} {} - applying transform'.format(model, year))
        annual = xr.Dataset({
            variable: annual.pipe(transformation)})

        logger.debug('{} {} - adding to running total'.format(model, year))
        total.append(annual)

    ds = xr.concat(total, dim=pd.Index(years, name='year')).mean(dim='year')

    # load baseline
    logger.debug('attempting to load baseline file: '.format(baseline_file))
    base = load_baseline(baseline_file, variable)

    logger.debug('{} - adding pattern residuals to baseline'.format(model))
    ds = (ds + base)

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

        run_job(metadata=metadata, **job)


if __name__ == '__main__':
    main()
