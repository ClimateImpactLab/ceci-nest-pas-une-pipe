'''
Powers of daily average temperature

Values are daily mean temperature raised to various powers for use in
polynomial response functions, aggregated to impact regions/hierids using
population weights. Data is reported at the daily level using a 365-day
calendar (leap years excluded) in the format YYYYDDD.
'''

import os
import pprint
import logging

import utils

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)

logger = logging.getLogger('uploader')
logger.setLevel('DEBUG')

__author__ = 'Michael Delgado'
__contact__ = 'mdelgado@rhg.com'
__version__ = '1.1'


BCSD_orig_files = (
    '/global/scratch/{read_acct}/nasa_bcsd/raw_data/{scenario}/{model}/' +
    '{source_variable}/' +
    '{source_variable}_day_BCSD_{scenario}_r1i1p1_{model}_{year}.nc')

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
    file='/team_poly_bcsd.py',
    execute='python team_poly_bcsd.py run',
    project='gcp',
    team='climate',
    probability_method='SMME',
    frequency='daily',
    dependencies='climate-tas-NASA_BCSD-originals.1.0')


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
        ds1[varname] = (ds.tas - 237.15)**power

        # Replace datetime64[ns] 'time' with YYYYDDD int 'day'
        if ds.dims['time'] > 365:
            raise ValueError

        ds1.coords['day'] = ds['time.year']*1000 + np.arange(1, len(ds.time)+1)
        ds1 = ds1.swap_dims({'time': 'day'})
        ds1 = ds1.drop('time')

        # document variable
        ds1[varname].attrs['unit'] = 'C^{}'.format(power) if power > 1 else 'C'
        ds1[varname].attrs['oneline'] = description.splitlines()[0]
        ds1[varname].attrs['description'] = description
        ds1[varname].attrs['variable'] = varname

        return ds1

    tas_poly.__doc__ = description

    transformation_spec = {
        'variable': varname,
        'source_variable': 'tas',
        'transformation': tas_poly,
        'unit': 'C^{}'.format(power) if power > 1 else 'C'
    }

    return transformation_spec


JOBS = [
    create_polynomial_transformation(1),
    # create_polynomial_transformation(2),
    # create_polynomial_transformation(3),
    # create_polynomial_transformation(4),
    # create_polynomial_transformation(5),
    # create_polynomial_transformation(6),
    # create_polynomial_transformation(7),
    # create_polynomial_transformation(8),
    # create_polynomial_transformation(9),
    ]

hist = range(1981, 2006)
proj1 = range(2006, 2050)
proj2 = range(2050, 2080)
proj3 = range(2080, 2100)

PERIODS = (
    # [dict(scenario='historical', read_acct='jiacany', year=y) for y in hist] +
    [dict(scenario='rcp45', read_acct='jiacany', year=y) for y in proj1] +
    # [dict(scenario='rcp45', read_acct='jiacany', year=y) for y in proj2] +
    # [dict(scenario='rcp45', read_acct='jiacany', year=y) for y in proj3] +
    # [dict(scenario='rcp85', read_acct='jiacany', year=y) for y in proj1] +
    # [dict(scenario='rcp85', read_acct='jiacany', year=y) for y in proj2] +
    # [dict(scenario='rcp85', read_acct='jiacany', year=y) for y in proj3] +
    [])

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

INCLUDED_METADATA = [
    'variable', 'source_variable', 'unit', 'scenario',
    'year', 'model', 'agglev', 'aggwt']


def run_job(
        metadata,
        variable,
        transformation,
        source_variable,
        unit,
        scenario,
        read_acct,
        year,
        model,
        agglev,
        aggwt,
        weights=None):

    import xarray as xr
    import metacsv

    from climate_toolbox import (
        load_bcsd,
        weighted_aggregate_grid_to_regions)

    # Add to job metadata
    metadata.update(ADDITIONAL_METADATA)

    file_dependencies = {}

    read_file = BCSD_orig_files.format(**metadata)
    write_file = WRITE_PATH.format(**metadata)

    # do not duplicate
    if os.path.isfile(write_file):
        return

    # Get transformed data
    fp = read_file.format(year=year)

    with xr.open_dataset(fp) as ds:
        ds.load()

    file_dependencies[os.path.splitext(os.path.basename(fp))[0]] = (
        str(ds.attrs.get('version', '1.0')))

    logger.debug('year {} - attempting to read file "{}"'.format(year, fp))
    ds = (
            load_bcsd(ds, source_variable, broadcast_dims=('time',))
            .pipe(transformation))

    varattrs = {var: dict(ds[var].attrs) for var in ds.data_vars.keys()}

    # Reshape to regions
    if not agglev.startswith('grid'):
        logger.debug('aggregating to "{}" using "{}"'.format(agglev, aggwt))
        ds = weighted_aggregate_grid_to_regions(
                ds, variable, aggwt, agglev, weights=weights)

    # Update netCDF metadata
    ds.attrs.update(**{
        k: str(v) for k, v in metadata.items() if k in INCLUDED_METADATA})
    ds.attrs.update(ADDITIONAL_METADATA)

    # Write output
    if not os.path.isdir(os.path.dirname(write_file)):
        logger.debug(
            'attempting to create_directory "{}"'
            .format(os.path.dirname(write_file)))

        os.makedirs(os.path.dirname(write_file))

    logger.debug('attempting to write to file "{}"'.format(write_file))

    attrs = dict(ds.attrs)
    attrs['file_dependencies'] = file_dependencies

    for var, vattrs in varattrs.items():
        ds[var].attrs.update(vattrs)

    ds.to_netcdf(write_file)

    metacsv.to_header(
        write_file.replace('.nc', '.fgh'),
        attrs=dict(attrs),
        variables=varattrs)

    logger.debug('job done')


def onfinish():
    logger.info('all done!')


main = utils.slurm_runner(
    filepath=__file__,
    job_spec=JOB_SPEC,
    run_job=run_job,
    onfinish=onfinish)


if __name__ == '__main__':
    main()
