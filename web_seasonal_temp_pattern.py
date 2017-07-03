'''
Seasonal average temperature, calculated for pattern models

Values are expected seasonal average daily mean temperature for 20-year
periods, aggregated to regions (impact regions/hierids or country/ISO) using
spatial/area weights. Data is aggregated to annual values using a 365-day
calendar (leap years excluded).
'''

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

logger = logging.getLogger('uploader')

__author__ = 'Michael Delgado'
__contact__ = 'mdelgado@rhg.com'
__version__ = '0.1.0'

BASELINE_FILE = (
    '/global/scratch/jiacany/nasa_bcsd/pattern/baseline/' +
    '{baseline_model}/{variable}/' +
    '{variable}_baseline_1986-2005_r1i1p1_{baseline_model}_{season}.nc')

BCSD_pattern_files = (
    '/global/scratch/{read_acct}/nasa_bcsd/pattern/SMME_surrogate/' +
    '{rcp}/{variable}/{model}/' +
    '{variable}_BCSD_{model}_{rcp}_r1i1p1_{season}_{{year}}.nc')

WRITE_PATH = (
    '/global/scratch/mdelgado/web/gcp/climate/{rcp}/{agglev}/{transformation_name}/' +
    '{transformation_name}_{agglev}_{aggwt}_{model}_{season}_{pername}.nc')

description = '\n\n'.join(
        map(lambda s: ' '.join(s.split('\n')),
            __doc__.strip().split('\n\n')))

ADDITIONAL_METADATA = dict(
    description=description,
    author=__author__,
    contact=__contact__,
    version=__version__,
    repo='https://github.com/ClimateImpactLab/pipelines',
    file='/pipelines/climate/jobs/impactlab_website/job_pattern_bcsd_ir_slurm.py',
    execute='job_pattern_bcsd_ir_slurm.job_pattern_bcsd_ir_slurm.run_slurm()',
    project='gcp',
    team='climate',
    geography='hierid',
    weighting='areawt',
    frequency='20yr')


def average_seasonal_temp_pattern(ds):
    '''
    Average seasonal tas
    '''
    return (ds.tas - 273.15).mean(dim='day')


JOBS = [
    dict(
        transformation_name='tas-seasonal',
        variable='tas',
        transformation=average_seasonal_temp_pattern)]

per20 = list(range(2020, 2040))
per40 = list(range(2040, 2060))
per60 = list(range(2060, 2080))
per80 = list(range(2080, 2100))

PERIODS = [
    dict(rcp='rcp45', read_acct='mdelgado', pername='2020', years=per20),
    dict(rcp='rcp45', read_acct='mdelgado', pername='2040', years=per40),
    # dict(rcp='rcp45', read_acct='mdelgado', pername='2060', years=per60),
    dict(rcp='rcp45', read_acct='mdelgado', pername='2080', years=per80),
    dict(rcp='rcp85', read_acct='jiacany', pername='2020', years=per20),
    dict(rcp='rcp85', read_acct='jiacany', pername='2040', years=per40),
    # dict(rcp='rcp85', read_acct='jiacany', pername='2060', years=per60),
    dict(rcp='rcp85', read_acct='jiacany', pername='2080', years=per80)
    ]

rcp_models = {
    'rcp45':
        list(map(lambda x: dict(model=x[0], baseline_model=x[1]), [
            ('pattern1', 'MRI-CGCM3'),
            ('pattern2', 'GFDL-ESM2G'),
            ('pattern3', 'MRI-CGCM3'),
            ('pattern4', 'GFDL-ESM2G'),
            ('pattern5', 'MRI-CGCM3'),
            ('pattern6', 'GFDL-ESM2G'),
            ('pattern27', 'GFDL-CM3'),
            ('pattern28', 'CanESM2'),
            ('pattern29', 'GFDL-CM3'),
            ('pattern30', 'CanESM2'),
            ('pattern31', 'GFDL-CM3'),
            ('pattern32', 'CanESM2')])),

    'rcp85':
        list(map(lambda x: dict(model=x[0], baseline_model=x[1]), [
            ('pattern1', 'MRI-CGCM3'),
            ('pattern2', 'GFDL-ESM2G'),
            ('pattern3', 'MRI-CGCM3'),
            ('pattern4', 'GFDL-ESM2G'),
            ('pattern5', 'MRI-CGCM3'),
            ('pattern6', 'GFDL-ESM2G'),
            ('pattern28', 'GFDL-CM3'),
            ('pattern29', 'CanESM2'),
            ('pattern30', 'GFDL-CM3'),
            ('pattern31', 'CanESM2'),
            ('pattern32', 'GFDL-CM3'),
            ('pattern33', 'CanESM2')]))}

MODELS = []

for spec in PERIODS:
    for model in rcp_models[spec['rcp']]:
        job = {}
        job.update(spec)
        job.update(model)
        MODELS.append(job)

SEASONS = list(map(lambda x: dict(season=x),[ 'DJF', 'MAM', 'JJA', 'SON']))

AGGREGATIONS = [
    {'agglev': 'ISO', 'aggwt': 'areawt'},
    {'agglev': 'hierid', 'aggwt': 'areawt'}]


JOB_SPEC = [JOBS, MODELS, SEASONS, AGGREGATIONS]

def run_job(
        metadata,
        variable,
        transformation_name,
        transformation,
        rcp,
        pername,
        read_acct,
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

    # do not duplicate
    if os.path.isfile(write_file):
        return

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
    ds.attrs.update(**ADDITIONAL_METADATA)

    # Write output
    logger.debug('attempting to write to file: {}'.format(write_file))
    if not os.path.isdir(os.path.dirname(write_file)):
        os.makedirs(os.path.dirname(write_file))

    ds.to_netcdf(write_file)

def onfinish():
    print('all done!')


main = utils.slurm_runner(
    filepath=__file__,
    job_spec=JOB_SPEC,
    run_job=run_job,
    onfinish=onfinish)

if __name__ == '__main__':
    main()
