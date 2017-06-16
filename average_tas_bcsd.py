'''
Average annual or seasonal temperature, calculated for CMIP models

Values are expected average daily mean temperature for 20-year periods,
aggregated to regions (impact regions/hierids or country/ISO) using
spatial/area weights. Data is aggregated to annual values using a 365-day
calendar (leap years excluded).

tas-annual values represent the annual average daily mean temperature
(degrees C). tas-seasonal values represent the seasonal average daily mean
temperature (degrees C).
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


BCSD_orig_files = (
    '/global/scratch/jiacany/nasa_bcsd/raw_data/{rcp}/{model}/{variable}/' +
    '{variable}_day_BCSD_{rcp}_r1i1p1_{model}_{{year}}.nc')

WRITE_PATH = (
    '/global/scratch/mdelgado/web/gcp/climate/{rcp}/{agglev}/{transformation_name}/' +
    '{transformation_name}_{agglev}_{aggwt}_{model}_{pername}.nc')

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
    file='/web_temp_extremes.py',
    execute='python web_temp_extremes.py --run',
    project='gcp', 
    team='climate',
    geography='hierid',
    weighting='areawt',
    frequency='20yr')


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

def annual_average_tas(ds):
    '''
    Mean of daily average temperature in degrees C
    '''
    ds = ds.loc[{'time': ~((ds['time.month'] == 2) & (ds['time.day'] == 29))}]
    return (ds.tas - 273.15).mean(dim='time')


def average_seasonal_temp(ds):
    '''
    Average seasonal tas
    '''
    ds = ds.loc[{'time': ~((ds['time.month'] == 2) & (ds['time.day'] == 29))}]
    return (ds.tas - 273.15).mean(dim='time')


JOBS = [
    dict(
        transformation_name='tas-seasonal',
        variable='tas',
        transformation=average_seasonal_temp,
        unit='degreesC'),

    dict(
        transformation_name='tas-annual',
        variable='tas',
        transformation=annual_average_tas,
        unit='degreesC'),

    dict(
        transformation_name='tasmax-over-95F',
        variable='tasmax',
        transformation=tasmax_over_95F,
        unit='days'),

    dict(
        transformation_name='tasmin-under-32F',
        variable='tasmin',
        transformation=tasmin_under_32F,
        unit='days')]

PERIODS = [
    dict(rcp='historical', pername='1986', years=list(range(1986, 2006))),
    dict(rcp='rcp85', pername='2020', years=list(range(2020, 2040))),
    dict(rcp='rcp85', pername='2040', years=list(range(2040, 2060))),
    dict(rcp='rcp85', pername='2080', years=list(range(2080, 2100))),
    dict(rcp='rcp45', pername='2020', years=list(range(2020, 2040))),
    dict(rcp='rcp45', pername='2040', years=list(range(2040, 2060))),
    dict(rcp='rcp45', pername='2080', years=list(range(2080, 2100)))]

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
        transformation_name,
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

    read_file = BCSD_orig_files.format(**metadata)
    write_file = WRITE_PATH.format(**metadata)

    # Get transformed data
    ds = xr.Dataset({variable: xr.concat([
        (load_bcsd(
                read_file.format(year=y),
                variable,
                broadcast_dims=('time',))
            .pipe(transformation))
        for y in years],
        dim=pd.Index(years, name='year')).mean(dim='year')})
    
    # Reshape to regions
    if not agglev.startswith('grid'):
        ds = weighted_aggregate_grid_to_regions(
                ds, variable, aggwt, agglev, weights=weights)

    # Update netCDF metadata
    ds.attrs.update(**metadata)

    # Write output
    if not os.path.isdir(os.path.dirname(write_file)):
        os.makedirs(os.path.dirname(write_file))

    ds.to_netcdf(write_file)


def onfinish():
    print('all done!')


def job_test_filepaths(
        metadata,
        variable,
        transformation_name,
        transformation,
        read_acct,
        rcp,
        pername,
        years,
        model,
        baseline_model,
        seasons,
        agglev,
        aggwt,
        weights=None):

    # make sure the input data exist

    read_file = BCSD_orig_files.format(**metadata)

    for y in years:
        fp = read_file.format(year=y)
        assert os.path.isfile(fp), "No such file: '{}'".format(fp)
    
    # make sure the output file has sufficient metadata
    WRITE_PATH.format(**metadata)


main = utils.slurm_runner(
    filepath=__file__,
    job_spec=JOB_SPEC,
    run_job=run_job,
    test_job=job_test_transformations,
    onfinish=onfinish,
    additional_metadata=ADDITIONAL_METADATA)


if __name__ == '__main__':
    main()
