'''
Hot Degree Days and Cold Degree Days for daily tasmax

Values are daily Hot Degree Days above 30 degrees C and Cold Degree Days below 10 degrees C.

version 1.0 - initial release (corresponds to hdd_cdd_pattern.py v10)
'''

import os
import logging

from toolz import memoize

from jrnr import slurm_runner

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)

logger = logging.getLogger('uploader')
logger.setLevel('DEBUG')

__author__ = 'J Simcock'
__contact__ = 'jsimcock@rhg.com'
__version__ = '1.1'

BCSD_pattern_files = (
    '/global/scratch/mdelgado/nasa_bcsd/SMME_formatted/{scenario}/' +
    '{model}/{source_variable}/{year}/{source_version}.nc')

BCSD_pattern_archive = (
    'GCP/climate/nasa_bcsd/SMME_formatted/{scenario}/{model}/' +
    '{source_variable}/{year}.nc')

WRITE_PATH = (
    '/global/scratch/jsimcock/projection/gcp/climate/' +
    '{agglev}/{aggwt}/{frequency}/{variable}/{scenario}/{model}/{year}/' +
    '{version}.nc4')

WEIGHTS_FILE = (
    '/global/scratch/mdelgado/config/GCP/spatial/world-combo-new/' +
    'segment_weights/' +
    'agglomerated-world-new_BCSD_grid_segment_weights_area_pop.pkl')

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
    repo=(
        'https://gitlab.com/ClimateImpactLab/Climate/' +
        'hdd_cdd_pattern.py'),
    file=str(__file__),
    execute='python {} run'.format(__file__),
    project='gcp',
    team='climate',
    probability_method='SMME',
    frequency='daily',
    dependencies='GCP-climate-nasa_bcsd-SMME_formatted.1.0')


def format_docstr(docstr):
    pars = docstr.split('\n\n')
    pars = [
        ' '.join(map(lambda s: s.strip(), par.split('\n'))) for par in pars]

    return '\n\n'.join(pars)

def tasmax_cdd10(ds):

    import xarray as xr
    import numpy as np

    description = format_docstr('''
        Daily Cold Degree Days with daily max temperature below 10 (degrees C) 

        Leap years are removed before counting days (uses a 365 day
        calendar).
        ''')

    varname = 'tasmax_hdd10'
    result = xr.Dataset()

    # remove leap years
    ds = ds.loc[{
        'time': ~((ds['time.month'] == 2) & (ds['time.day'] == 29))}]

    # do transformation
    result[varname] = xr.ufuncs.fmax(10 - (ds.tasmax-273.15), 0).sum(dim='time')

    # Replace datetime64[ns] 'time' with YYYYDDD int 'day'

    result.coords['day'] = ds['time.year']*1000 + np.arange(1, len(ds.time)+1)
    result = result.swap_dims({'time': 'day'})
    result = result.drop('time')

    result = result.rename({'day': 'time'})

    # document variable
    result[varname].attrs['long_title'] = description.splitlines()[0]
    result[varname].attrs['description'] = description
    result[varname].attrs['variable'] = varname

    return result

def tasmax_hdd30(ds):

    import xarray as xr
    import numpy as np

    description = format_docstr('''
        Daily Hot Degree Days with daily max temperature above 30 (degrees C) 

        Leap years are removed before counting days (uses a 365 day
        calendar).
        ''')

    varname = 'tasmax_cdd30'
    result = xr.Dataset()

    # remove leap years
    ds = ds.loc[{
        'time': ~((ds['time.month'] == 2) & (ds['time.day'] == 29))}]

    # do transformation
    result[varname] = xr.ufuncs.fmax((ds.tasmax-273.15) - 30, 0).sum(dim='time')

    # Replace datetime64[ns] 'time' with YYYYDDD int 'day

    result.coords['day'] = ds['time.year']*1000 + np.arange(1, len(ds.time)+1)
    result = result.swap_dims({'time': 'day'})
    result = result.drop('time')

    result = result.rename({'day': 'time'})

    # document variable
    result[varname].attrs['long_title'] = description.splitlines()[0]
    result[varname].attrs['description'] = description
    result[varname].attrs['variable'] = varname

    return result


def validate_hdd_cdd(ds):

    # check for expected dimensions. should be missing Jan+Feb 1981, Dec 2099.
    # we also expect to be missing all leap years.
    print(ds)
    msg = 'unexpected dimensions: {}'.format(ds.dims)
    year = int(ds.attrs['year'])
    if year > 1981 and year < 2099:
        assert ds.dims == {'hierid': 24378}, msg
    elif year == 1981:
        assert ds.dims == {'hierid': 24378}, msg
    elif year == 2099:
        assert ds.dims == {'hierid': 24378}, msg
    else:
        raise ValueError(
            "I didn't realize we had downscaled the 22nd century!!" +
            "\nyear: {}\ndims:{}".format(year, ds.dims))

    # check for unexpected values, accounting for polynomial terms
    assert not ds[ds.variable].isnull().any()
    assert ds[ds.variable].min() >= 0
        

transformation_cdd10 = {
    'variable': 'coldd_agg',
    'source_variable': 'tasmax',
    'transformation': tasmax_cdd10,
    'validation': validate_hdd_cdd,
    'units': 'Degrees C'
}

transformation_hdd30 = {
    'variable': 'hotdd_agg',
    'source_variable': 'tasmax',
    'transformation': tasmax_hdd30,
    'validation': validate_hdd_cdd,
    'units': 'Degrees C'
}


JOBS = [transformation_cdd30, transformation_hdd10]

hist = range(1981, 2006)
proj = range(2006, 2100)

PERIODS = ([
    dict(scenario=rcp, read_acct='mdelgado', source_version='1.0', year=y)
    for y in proj for rcp in ['rcp45', 'rcp85']])

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

AGGREGATIONS = [
    {'agglev': 'hierid', 'aggwt': 'popwt'}]

JOB_SPEC = [JOBS, MODELS, AGGREGATIONS]

INCLUDED_METADATA = [
    'variable', 'source_variable', 'units', 'scenario',
    'year', 'model', 'agglev', 'aggwt']


def onfinish():
    print('all done!')


@memoize
def get_weights(weights_file=WEIGHTS_FILE):
    import pandas as pd

    return pd.read_pickle(weights_file)


@slurm_runner(filepath=__file__, job_spec=JOB_SPEC, onfinish=onfinish)
def tasmax_hdd_cdd(
        metadata,
        variable,
        transformation,
        validation,
        source_variable,
        source_version,
        units,
        scenario,
        read_acct,
        year,
        model,
        baseline_model,
        agglev,
        aggwt,
        interactive=False):

    import xarray as xr
    import metacsv

    from climate_toolbox import (
        weighted_aggregate_grid_to_regions)

    # Add to job metadata
    metadata.update(ADDITIONAL_METADATA)

    file_dependencies = {}

    read_file = BCSD_pattern_files.format(**metadata)
    source_archive = BCSD_pattern_archive.format(**metadata)
    write_file = WRITE_PATH.format(**metadata)

    # do not duplicate
    if os.path.isfile(write_file):
        return

    # Get transformed data
    logger.debug(
        'attempting to read file "{}"'.format(read_file))

    with xr.open_dataset(read_file) as ds:
        ds.load()

    logger.debug(
        'running transformation "{}"'.format(transformation))

    ds = transformation(ds)

    varattrs = {var: dict(ds[var].attrs) for var in ds.data_vars.keys()}

    file_dependencies[source_archive] = source_version

    # Reshape to regions

    logger.debug(
        'attempting to read weights file "{}"'.format(WEIGHTS_FILE))
    weights = get_weights(WEIGHTS_FILE)

    logger.debug('{} reshaping to regions'.format(model))
    if not agglev.startswith('grid'):
        ds = weighted_aggregate_grid_to_regions(
                ds, variable, aggwt, agglev, weights=weights)

    # Update netCDF metadata
    logger.debug('{} udpate metadata'.format(model))
    ds.attrs.update(**{
        k: str(v) for k, v in metadata.items() if k in INCLUDED_METADATA})

    ds.attrs.update(ADDITIONAL_METADATA)

    attrs = dict(ds.attrs)
    attrs['file_dependencies'] = file_dependencies

    for var, vattrs in varattrs.items():
        ds[var].attrs.update(vattrs)

        if ds[var].dims == ('hierid', 'time'):
            ds[var] = ds[var].transpose('time', 'hierid')

    if interactive:
        return ds

    # Write output
    logger.debug('attempting to write to temp file: {}~'.format(write_file))
    if not os.path.isdir(os.path.dirname(write_file)):
        os.makedirs(os.path.dirname(write_file))

    header_file = os.path.splitext(write_file)[0] + '.fgh'

    ds.to_netcdf(write_file + '~')
    logger.debug(
        'attempting to write to temp header file: {}~'.format(header_file))

    metacsv.to_header(
        header_file +' ~',
        attrs=dict(attrs),
        variables=varattrs)

    logger.debug('running validation tests')
    with xr.open_dataset(write_file+'~') as ds:
        validation(ds)

    logger.debug('moving tmp files to final output destination')
    os.rename(header_file+' ~', header_file)
    os.rename(write_file+'~', write_file)

    logger.debug('job done')


if __name__ == '__main__':
    tasmax_hdd_cdd()
