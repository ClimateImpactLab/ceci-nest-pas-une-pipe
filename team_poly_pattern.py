'''
Powers of daily average temperature, calculated for pattern models

Values are daily mean temperature raised to various powers for use in
polynomial response functions, aggregated to impact regions/hierids using
population weights. Data is reported at the daily level using a 365-day
calendar (leap years excluded) in the format YYYYDDD.

version 1.5 - initial release (corresponds to tas_poly_bcsd.py v1.5)
'''

import os
import logging

import utils

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)

logger = logging.getLogger('uploader')
logger.setLevel('DEBUG')

__author__ = 'Michael Delgado'
__contact__ = 'mdelgado@rhg.com'
__version__ = '1.5'

BCSD_pattern_files = (
    '/global/scratch/{read_acct}/nasa_bcsd/SMME_formatted/{scenario}/{model}/' +
    '{source_variable}/{year}/{source_version}.nc')

BCSD_pattern_archive = (
    'GCP/climate/nasa_bcsd/SMME_formatted/{scenario}/{model}/' +
    '{source_variable}/{year}.nc')

WRITE_PATH = (
    '/global/scratch/mdelgado/projection/gcp/climate/' +
    '{agglev}/{aggwt}/{frequency}/{variable}/{scenario}/{model}/{year}/' +
    '{version}.nc4')

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
    file='/team_poly_pattern.py',
    execute='python team_poly_pattern.py run',
    project='gcp',
    team='climate',
    probability_method='SMME',
    frequency='daily',
    dependencies='GCP-climate-nasa_bcsd-SMME_formatted.1.0')


def ordinal(n):
    ''' converts numbers into ordinal strings '''

    return (
        "%d%s" %
        (n, "tsnrhtdd"[(n // 10 % 10 != 1) * (n % 10 < 4) * n % 10::4]))


def format_docstr(docstr):
    pars = docstr.split('\n\n')
    pars = [
        ' '.join(map(lambda s: s.strip(), par.split('\n'))) for par in pars]

    return '\n\n'.join(pars)


def create_polynomial_transformation(power=2):
    '''
    Creates a polynomial spec from a power

    '''

    powername = ordinal(power)

    description = format_docstr(('''
            Daily average temperature (degrees C){raised}

            Leap years are removed before counting days (uses a 365 day
            calendar).
            '''.format(
                raised='' if power == 1 else (
                    ' raised to the {powername} power'
                    .format(powername=powername)))).strip())

    varname = 'tas-poly-{}'.format(power) if power > 1 else 'tas'

    def tas_poly(ds):

        import xarray as xr
        import numpy as np

        ds1 = xr.Dataset()

        # remove leap years
        ds = ds.loc[{
            'time': ~((ds['time.month'] == 2) & (ds['time.day'] == 29))}]

        # do transformation
        ds1[varname] = (ds.tas - 273.15)**power

        # Replace datetime64[ns] 'time' with YYYYDDD int 'day'
        if ds.dims['time'] > 365:
            raise ValueError

        ds1.coords['day'] = ds['time.year']*1000 + np.arange(1, len(ds.time)+1)
        ds1 = ds1.swap_dims({'time': 'day'})
        ds1 = ds1.drop('time')

        ds1 = ds1.rename({'day': 'time'})

        # document variable
        ds1[varname].attrs['units'] = 'C^{}'.format(power) if power > 1 else 'C'
        ds1[varname].attrs['long_title'] = description.splitlines()[0]
        ds1[varname].attrs['description'] = description
        ds1[varname].attrs['variable'] = varname

        return ds1

    tas_poly.__doc__ = description

    transformation_spec = {
        'variable': varname,
        'source_variable': 'tas',
        'transformation': tas_poly,
        'units': 'C^{}'.format(power) if power > 1 else 'C'
    }

    return transformation_spec


JOBS = [create_polynomial_transformation(i) for i in range(1, 10)]

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


@utils.slurm_runner(filepath=__file__, job_spec=JOB_SPEC, onfinish=onfinish)
def run_job(
        metadata,
        variable,
        transformation,
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
        weights=None,
        interactive=False):

    import xarray as xr
    import pandas as pd
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
        'year {} - attempting to read file "{}"'.format(year, read_file))

    with xr.open_dataset(read_file) as ds:
        ds.load()

    ds = transformation(ds)

    varattrs = {var: dict(ds[var].attrs) for var in ds.data_vars.keys()}

    file_dependencies[source_archive] = source_version

    # Reshape to regions

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
    logger.debug('attempting to write to file: {}'.format(write_file))
    if not os.path.isdir(os.path.dirname(write_file)):
        os.makedirs(os.path.dirname(write_file))

    ds.to_netcdf(write_file)

    metacsv.to_header(
        os.path.splitext(write_file)[0] + '.fgh',
        attrs=dict(attrs),
        variables=varattrs)

    logger.debug('job done')



if __name__ == '__main__':
    main()
