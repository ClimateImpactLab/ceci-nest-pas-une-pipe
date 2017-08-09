'''
Formatted SMME "Surrogate" Synthetic Model Results

Data are synthetic model outputs produced from pattern scaling 0.25-degree
downscaled CMIP5 model outputs. Pattern scaling uses the SMME method.
'''

import os
import logging

from jrnr import slurm_runner


FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)

logger = logging.getLogger('uploader')
logger.setLevel('DEBUG')

__author__ = 'Michael Delgado'
__contact__ = 'mdelgado@rhg.com'
__version__ = '1.0'

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
    '/global/scratch/mdelgado/nasa_bcsd/SMME_formatted/{scenario}/{model}/' +
    '{source_variable}/{year}/{version}.nc')

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
    frequency='daily')

# Calibration data
season_month_start = {'DJF': 12, 'MAM': 3, 'JJA': 6, 'SON': 9}
years = range(1981, 2100)
INVALID = 9.969209968386869e+36  # invalid data found in pattern data sets

JOBS = [
    {
        'source_variable': var,
        'units': 'Kelvin',
        'dependencies': [
            'climate-{}-BCSD.SMME_surrogate.2017-03-21'.format(var),
            'climate-{}-BCSD.residual.2017-01-30'.format(var)]}
    for var in ['tas', 'tasmin', 'tasmax']]

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
    'year', 'model', 'dependencies']


def reshape_days_to_datetime(surrogate, year, season):
    import xarray as xr
    import pandas as pd

    ds = (
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

    return ds


def validation(ds):

    # check for expected dimensions. should be missing Jan+Feb 1981, Dec 2099.
    # we also expect to be missing all leap years.
    year = int(ds.attrs['year'])

    msg = 'unexpected dimensions: {}'.format(ds.dims)
    if (year > 1981) and (year < 2099):
        assert ds.dims == {'time': 365, 'lon': 1440, 'lat': 720}, msg
    elif year == 1981:
        assert ds.dims == {'time': 306, 'lon': 1440, 'lat': 720}, msg
    elif year == 2099:
        assert ds.dims == {'time': 334, 'lon': 1440, 'lat': 720}, msg
    else:
        raise ValueError(
            "I didn't realize we had downscaled the 22nd century!!" +
            "\nyear: {}\ndims:{}".format(year, ds.dims))
    
    varname = ds.attrs['source_variable']
    assert ds[varname].mean() > 5
    assert ds[varname].mean() < 30


@slurm_runner(filepath=__file__, job_spec=JOB_SPEC)
def reshape_to_annual(
        metadata,
        source_variable,
        units,
        scenario,
        year,
        model,
        read_acct,
        dependencies,
        baseline_model,
        interactive=False):

    import xarray as xr
    import numpy as np
    from climate_toolbox.climate_toolbox import (load_baseline, load_bcsd)

    metadata.update(ADDITIONAL_METADATA)

    baseline_file = BASELINE_FILE.format(**metadata)
    write_file = WRITE_PATH.format(**metadata)

    # do not duplicate
    if os.path.isfile(write_file):
        try:
            with xr.open_dataset(write_file) as ds:
                validation(ds)
                return
        except Exception:
            pass

    seasonal_baselines = {}
    for season in ['DJF', 'MAM', 'JJA', 'SON']:
        basef = baseline_file.format(season=season)

        logger.debug('year {} season {} - attempting to read baseline file "{}"'.format(year, season, basef))
        with xr.open_dataset(basef) as ds:
            ds.load()

        logger.debug('year {} season {} - reshaping baseline'.format(year, season, basef))

        if 'nlat' in ds.dims and 'lat' in ds.data_vars:
            ds = ds.set_coords('lat').swap_dims({'nlat': 'lat'})
        
        if 'nlon' in ds.dims and 'lon' in ds.data_vars:
            ds = ds.set_coords('lon').swap_dims({'nlon': 'lon'})

        seasonal_baselines[season] = load_baseline(ds, source_variable)

    seasonal_data = []

    for i, season in enumerate(['DJF', 'MAM', 'JJA', 'SON', 'DJF']):

        # read in values for all seasons from DJF the previous year to DJF the
        # following year
        fp = (BCSD_pattern_files
                    .format(
                        year=year+(i//4),
                        **{k: v for k, v in metadata.items() if k != 'year'})
                    .format(season=season))

        if not os.path.isfile(fp):
            logger.debug('year {} season {} - no pattern file "{}" found. skipping.'.format(year, season, fp))
            continue

        logger.debug('year {} season {} - attempting to read pattern file "{}"'.format(year, season, fp))
        with xr.open_dataset(fp) as ds:
            ds.load()
        
        logger.debug('year {} season {} - reshaping pattern'.format(year, season, fp))
        ds = load_bcsd(ds, source_variable, broadcast_dims=('day',))

        # drop invalid value from DataArray
        logger.debug('year {} season {} - dropping invalid values in pattern'.format(year, season, fp))
        ds[source_variable] = (
            ds[source_variable].where(ds[source_variable] != INVALID))

        msg = "value out of bounds (100) in file {}".format(fp)
        assert (ds[source_variable].fillna(0) < 100).all(), msg

        logger.debug('year {} season {} - reshaping days to datetime in pattern'.format(year, season, fp))
        patt = reshape_days_to_datetime(ds, year+(i//4), season)            
        seasonal_data.append(patt + seasonal_baselines[season])

    logger.debug('combining data sets')
    ds = xr.concat(seasonal_data, 'time')

    # pandas 20.0 compatible 
    ds = ds.sel(time=(np.vectorize(lambda t: t.year)(ds.time) == year))

    # correct for pandas 20.0 + PeriodIndex incompatability
    ds['time'].values = ds.time.values.astype(np.dtype('datetime64[ns]'))

    # Update netCDF metadata
    logger.debug('udpating metadata')
    ds.attrs.update(**{
        k: str(v) for k, v in metadata.items() if k in INCLUDED_METADATA})

    ds.attrs.update(ADDITIONAL_METADATA)

    if interactive:
        return ds

    # Write output
    logger.debug('attempting to write to file: {}'.format(write_file))
    if not os.path.isdir(os.path.dirname(write_file)):
        os.makedirs(os.path.dirname(write_file))

    ds.to_netcdf(write_file+'~')

    logger.debug('running validation tests')
    with xr.open_dataset(write_file+'~') as ds:
        validation(ds)

    os.rename(write_file+'~', write_file)

    logger.debug('job done')


if __name__ == '__main__':
    reshape_to_annual()
