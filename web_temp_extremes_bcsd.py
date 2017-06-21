'''
Counts of expected daily extremes (tasmax > 95F or tasmin < 32F)

Values are expected daily extremes per year for 20-year periods, aggregated to
regions (impact regions/hierids or country/ISO) using spatial/area weights.
Data is aggregated to annual values using a 365-day calendar (leap years
excluded).
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


BCSD_orig_files = (
    '/global/scratch/jiacany/nasa_bcsd/raw_data/{rcp}/{model}/{variable}/' +
    '{variable}_day_BCSD_{rcp}_r1i1p1_{model}_{{year}}.nc')

WRITE_PATH = (
    '/global/scratch/mdelgado/web/gcp/climate/{rcp}/{agglev}/{transformation_name}/' +
    '{transformation_name}_{agglev}_{aggwt}_{model}_{pername}.nc')

description = '\n\n'.join(
        map(lambda s: ' '.join(s.split('\n')),
            __doc__.strip().split('\n\n')))

oneline = description.split('\n')[0]

ADDITIONAL_METADATA = dict(
    oneline=oneline,
    description=description,
    author=__author__,
    contact=__contact__,
    version=__version__,
    repo='https://github.com/ClimateImpactLab/ceci-nest-pas-une-pipe',
    file='/web_temp_extremes.py',
    execute='python web_temp_extremes.py --run',
    project='gcp', 
    team='climate',
    geography='hierid',
    weighting='areawt',
    frequency='20yr')

DS_METADATA_FEILDS = [
    'rcp', 'pername', 'transformation_name',
    'unit', 'model', 'agglev', 'aggwt']


def tasmin_under_32F_365day(ds):
    '''
    Count of days with tasmin under 32F/0C
    '''
    ds = ds.loc[{'time': ~((ds['time.month'] == 2) & (ds['time.day'] == 29))}]
    return ds.tasmin.where((ds.tasmin- 273.15) < 0).count(dim='time')


def tasmax_over_95F_365day(ds):
    '''
    Count of days with tasmax over 95F/35C

    Leap years are removed before counting days (uses a 365 day calendar)
    '''
    ds = ds.loc[{'time': ~((ds['time.month'] == 2) & (ds['time.day'] == 29))}]
    return ds.tasmax.where((ds.tasmax- 273.15) > 35).count(dim='time')


def tasmax_over_118F_365day(ds):
    '''
    Count of days with tasmax over 118F/47.8C

    Leap years are removed before counting days (uses a 365 day calendar)
    '''
    ds = ds.loc[{'time': ~((ds['time.month'] == 2) & (ds['time.day'] == 29))}]
    return ds.tasmax.where(
        (ds.tasmax- 273.15) > ((118. - 32.) * 5. / 9.)).count(dim='time')


JOBS = [
    # dict(transformation_name='tasmax-over-118F',
    #     unit='days-over-118F',
    #     variable='tasmax',
    #     transformation=tasmax_over_118F_365day),
    
    dict(transformation_name='tasmax-over-95F',
        unit='days-over-95F',
        variable='tasmax',
        transformation=tasmax_over_95F_365day),
    
    # dict(transformation_name='tasmin-under-32F',
    #     unit='days-under-32F',
    #     variable='tasmin',
    #     transformation=tasmin_under_32F_365day)
    ]

PERIODS = [
    dict(rcp='historical', pername='1986', years=list(range(1986, 2006))),
    dict(rcp='rcp85', pername='2020', years=list(range(2020, 2040))),
    dict(rcp='rcp85', pername='2040', years=list(range(2040, 2060))),
    dict(rcp='rcp85', pername='2060', years=list(range(2060, 2080))),
    dict(rcp='rcp85', pername='2080', years=list(range(2080, 2100))),
    dict(rcp='rcp45', pername='2020', years=list(range(2020, 2040))),
    dict(rcp='rcp45', pername='2040', years=list(range(2040, 2060))),
    dict(rcp='rcp45', pername='2060', years=list(range(2060, 2080))),
    dict(rcp='rcp45', pername='2080', years=list(range(2080, 2100)))
    ]

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
    # {'agglev': 'ISO', 'aggwt': 'areawt'},
    {'agglev': 'hierid', 'aggwt': 'areawt'}]


JOB_SPEC = [JOBS, PERIODS, MODELS, AGGREGATIONS]

def run_job(
        metadata,
        variable,
        transformation_name,
        transformation,
        unit,
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

    read_file = BCSD_orig_files.format(**metadata)
    write_file = WRITE_PATH.format(**metadata)

    # do not duplicate
    if os.path.isfile(write_file):
        return

    # Prepare annual transformed data
    annual = []
    for y in years:
        fp = read_file.format(year=y)
        
        logger.debug('attempting to load BCSD file: {}'.format(fp))
        annual.append(
            load_bcsd(fp, variable, broadcast_dims=('time',))
                .pipe(transformation))

    # Concatente years to single dataset and average across years
    logger.debug('{} - concatenating annual data'.format(model))
    ds = xr.Dataset({
        variable: xr.concat(annual, dim=pd.Index(years, name='year'))
                        .mean(dim='year')})
    
    # Reshape to regions
    logger.debug('{} reshaping to regions'.format(model))
    if not agglev.startswith('grid'):
        ds = weighted_aggregate_grid_to_regions(
                ds, variable, aggwt, agglev, weights=weights)

    # Update netCDF metadata
    logger.debug('{} udpate metadata'.format(model))
    ds.attrs.update(
        **{k: str(v) for k, v in metadata.items() if k in DS_METADATA_FEILDS})
    ds.attrs.update(**ADDITIONAL_METADATA)

    # Write output
    logger.debug('attempting to write to file: {}'.format(write_file))
    if not os.path.isdir(os.path.dirname(write_file)):
        os.makedirs(os.path.dirname(write_file))

    ds.to_netcdf(write_file)
    logger.debug('done')


def onfinish():
    print('all done!')


main = utils.slurm_runner(
    filepath=__file__,
    job_spec=JOB_SPEC,
    run_job=run_job,
    onfinish=onfinish)


if __name__ == '__main__':
    main()
