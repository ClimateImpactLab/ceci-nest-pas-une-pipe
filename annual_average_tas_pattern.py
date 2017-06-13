'''
Annual average temperature, calculated for pattern models

Values are expected annual average daily mean temperature for 20-year periods,
aggregated to regions (impact regions/hierids or country/ISO) using
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

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)

logger = logging.getLogger('uploader')
logger.setLevel('DEBUG')

__author__ = 'Michael Delgado'
__contact__ = 'mdelgado@rhg.com'
__version__ = '0.1.0'

BASELINE_FILE = (
    '/global/scratch/jiacany/nasa_bcsd/pattern/baseline/' +
    '{baseline_model}/{variable}/' +
    '{variable}_baseline_1986-2005_r1i1p1_{baseline_model}_{{season}}.nc')

BCSD_pattern_files = (
    '/global/scratch/{read_acct}/nasa_bcsd/pattern/SMME_surrogate/' +
    '{rcp}/{variable}/{model}/' +
    '{variable}_BCSD_{model}_{rcp}_r1i1p1_{{season}}_{{year}}.nc')

WRITE_PATH = (
    '/global/scratch/mdelgado/web/gcp/climate/{rcp}/{agglev}/{variable}/' +
    '{variable}_{agglev}_{aggwt}_{model}_{pername}.nc')

description = '\n\n'.join(
        map(lambda s: ' '.join(s.split('\n')),
            __file__.__doc__.strip().split('\n\n')))

oneline = description.split('\n')[0]

ADDITIONAL_METADATA = dict(
    oneline=oneline,
    description=description,
    author=__author__,
    contact=__contact__,
    version=__version__,
    repo='https://github.com/ClimateImpactLab/ceci-nest-pas-une-pipe',
    file='/annual_average_tas_pattern.py',
    execute='python annual_average_tas_pattern.py --run',
    project='gcp', 
    team='climate',
    geography='hierid',
    weighting='areawt',
    frequency='20yr')


def annual_average_tas(ds):
    '''
    Mean of daily average temperature in degrees C
    '''
    return (ds.tas - 273.15).mean(dim='time')


JOBS = [
    dict(variable='tasmax', transformation=annual_average_tas, unit='degreesC')]

per20 = list(range(2020, 2040))
per40 = list(range(2040, 2060))
per80 = list(range(2080, 2100))

PERIODS = [
    dict(rcp='rcp45', read_acct='mdelgado', pername='2020', years=per20),
    dict(rcp='rcp45', read_acct='mdelgado', pername='2040', years=per40),
    dict(rcp='rcp45', read_acct='mdelgado', pername='2080', years=per80),
    dict(rcp='rcp85', read_acct='jiacany', pername='2020', years=per20),
    dict(rcp='rcp85', read_acct='jiacany', pername='2040', years=per40),
    dict(rcp='rcp85', read_acct='jiacany', pername='2080', years=per80)]

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

SEASONS = [{'seasons': [ 'DJF', 'MAM', 'JJA', 'SON']}]

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
        read_acct,
        baseline_model,
        seasons,
        unit,
        agglev,
        aggwt,
        weights=None):

    # Add to job metadata
    metadata.update(dict(
        time_horizon='{}-{}'.format(years[0], years[-1])))

    baseline_file = BASELINE_FILE.format(**metadata)
    pattern_file = BCSD_pattern_files.format(**metadata)
    write_file = WRITE_PATH.format(**metadata)
    
    del metadata['read_acct']

    # Get transformed data
    total = None

    seasonal_baselines = {}
    for season in seasons:
        basef = baseline_file.format(season=season)
        logger.debug('attempting to load baseline file: {}'.format(basef))
        seasonal_baselines[season] = load_baseline(basef, variable)

    season_month_start = {'DJF': 12, 'MAM': 3, 'JJA': 6, 'SON': 9}

    for year in years:
        seasonal = []

        for s, season in enumerate(seasons):

            pattf = pattern_file.format(year=year, season=season)
            logger.debug('attempting to load pattern file: {}'.format(pattf))
            patt = load_bcsd(pattf, variable, broadcast_dims=('day',))

            logger.debug(
                '{} {} {} - reindexing coords day --> time'.format(
                    model, year, season))

            patt = (
                patt.assign_coords(
                        time=xr.DataArray(
                            pd.period_range(
                                '{}-{}-1'.format(
                                    year-int(season == 'DJF'),
                                    season_month_start[season]),
                                periods=len(patt.day),
                                freq='D'),
                            coords={'day': patt.day}))
                    .swap_dims({'day': 'time'})
                    .drop('day'))

            logger.debug(
                '{} {} {} - adding pattern residuals to baseline'.format(
                    model, year, season))

            seasonal.append(patt + seasonal_baselines[season])
            
        logger.debug((
            '{} {} - concatenating seasonal data and ' +
            'applying transform').format(model, year))

        annual = xr.Dataset({
            variable: xr.concat(seasonal, dim='time')
                            .pipe(transformation)})

        if total is None:
            total = annual
        else:
            total += annual

    ds = total / len(years)

    # Reshape to regions

    logger.debug('{} reshaping to regions'.format(model))
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

        metadata = {k: str(v) for k, v in ADDITIONAL_METADATA.items()}
        metadata.update({k: str(v) for k, v in job.items()})

        run_job(metadata=metadata, **job)


if __name__ == '__main__':
    main()
