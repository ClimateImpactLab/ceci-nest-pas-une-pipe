'''
Formatted SMME "Surrogate" Synthetic Model Results

Data are synthetic model outputs produced from pattern scaling 0.25-degree
downscaled CMIP5 model outputs. Pattern scaling uses the SMME method.
'''

import os
import utils
import numpy as np
import pandas as pd
import xarray as xr
import logging

from climate_toolbox import load_baseline

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)

logger = logging.getLogger('uploader')
logger.setLevel('DEBUG')

__author__ = 'Michael Delgado'
__contact__ = 'mdelgado@rhg.com'
__version__ = '1.5'

BASELINE_FILE = (
    '/global/scratch/jiacany/nasa_bcsd/pattern/baseline/' +
    '{baseline_model}/{source_variable}/' +
    '{source_variable}_baseline_1986-2005_r1i1p1_' +
    '{baseline_model}_{{season}}.nc')

BCSD_pattern_files = (
    '/global/scratch/{read_acct}/nasa_bcsd/pattern/SMME_surrogate/' +
    '{scenario}/{source_variable}/{model}/' +
    '{source_variable}_BCSD_{model}_{scenario}_r1i1p1_{{season}}_{year}.nc')

WRITE_PATH = (
    '/global/scratch/mdelgado/nasa_bcsd/SMME_formatted/{scenario}/{model}/{source_variable}/' +
    '{source_variable}_SMME-formatted_{scenario}_{model}_{year}.nc')

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
    file='/reshape.py',
    execute='python reshape.py run',
    project='gcp',
    team='climate',
    probability_method='SMME',
    frequency='daily',
    dependencies='climate-tas-NASA_BCSD-originals.1.0')

# Calibration data
season_month_start = {'DJF': 12, 'MAM': 3, 'JJA': 6, 'SON': 9}
years = range(1982, 2100)
INVALID = 9.969209968386869e+36  # invalid data found in pattern data sets

JOBS = [
    {'source_variable': 'tas', 'units': 'Kelvin'},
    {'source_variable': 'tasmin', 'units': 'Kelvin'},
    {'source_variable': 'tasmax', 'units': 'Kelvin'}]

PERIODS = (
    [dict(scenario='rcp45', read_acct='mdelgado', year=y) for y in years] +
    [dict(scenario='rcp85', read_acct='jiacany', year=y) for y in years])

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
    for model in rcp_models[spec['scenario']]:
        job = {}
        job.update(spec)
        job.update(model)
        MODELS.append(job)

JOB_SPEC = (JOBS, MODELS)

INCLUDED_METADATA = [
    'variable', 'source_variable', 'units', 'scenario',
    'year', 'model', 'agglev', 'aggwt']


def reshape_days_to_datetime(surrogate, year, season):
    return (
        surrogate.assign_coords(
                time=xr.DataArray(
                    pd.period_range(
                        '{}-{}-1'.format(
                            year-int(season == 'DJF'),
                            season_month_start[season]),
                        periods=len(surrogate.day),
                        freq='D'),
                    coords={'day': surrogate.day},
                    dims=('day',)))
            .swap_dims({'day': 'time'})
            .drop('day'))


@utils.slurm_runner(filepath=__file__, job_spec=JOB_SPEC)
def reshape_to_annual(
        metadata,
        source_variable,
        units,
        scenario,
        year,
        model,
        read_acct,
        baseline_model):

    baseline_file = BASELINE_FILE.format(**metadata)

    seasonal_baselines = {}
    for season in ['DJF', 'MAM', 'JJA', 'SON']:
        basef = baseline_file.format(season=season)
        seasonal_baselines[season] = (
            load_baseline(
                basef,
                source_variable)
            .set_coords('lat')
            .set_coords('lon')
            .swap_dims({'nlat': 'lat', 'nlon': 'lon'}))

    seasonal_data = []

    for i, season in enumerate(['DJF', 'MAM', 'JJA', 'SON', 'DJF']):
        fp = (BCSD_pattern_files
                    .format(year=year+(i//4), **{k: v for k, v in metadata.items() if k != 'year'})
                    .format(season=season))

        if not os.path.isfile(fp):
            print('skipping {}'.format(fp))
            continue

        with xr.open_dataset(fp) as ds:
            ds.load()

        # drop invalid value from DataArray
        ds[source_variable] = (
            ds[source_variable].where(ds[source_variable] != INVALID))

        msg = "value out of bounds (100) in file {}".format(fp)
        assert (ds[source_variable].fillna(0) < 100).all(), msg

        patt = reshape_days_to_datetime(ds, year+(i//4), season)            
        seasonal_data.append(patt + seasonal_baselines[season])

    ds = xr.concat(seasonal_data, 'time')

    ds = ds.sel(time=ds['time.year'] == year)

    return ds


if __name__ == '__main__':
    reshape_to_annual()
