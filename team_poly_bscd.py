'''
Powers of daily average temperature

Values are daily mean temperature raised to various powers for use in 
polynomial response functions, aggregated to impact regions/hierids using
population weights. Data is reported at the daily level using a 365-day
calendar (leap years excluded) in the format YYYYDDD.
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
__version__ = '1.0'


BCSD_orig_files = (
    '/global/scratch/{read_acct}/nasa_bcsd/raw_data/{rcp}/{model}/{variable}/' +
    '{variable}_day_BCSD_{rcp}_r1i1p1_{model}_{year}.nc')

WRITE_PATH = (
    '/global/scratch/mdelgado/web/gcp/climate/{scenario}/{agglev}/' +
    '{variable}/' +
    '{variable}_{frequency}_{unit}_{scenario}_{agglev}_{aggwt}_{model}_{year}.nc')

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
    frequency='daily')

ordinal = lambda n: "%d%s" % (n,"tsnrhtdd"[(n//10%10!=1)*(n%10<4)*n%10::4])
''' converts numbers into ordinal strings '''


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

    description = format_docstr('''
        Daily average temperature (degrees C) raised to the {powername} power

        Leap years are removed before counting days (uses a 365 day 
        calendar). 
        '''.format(powername=powername))

    varname = 'tas-poly-{}'.format(power)

    def tas_poly(ds):

        ds1 = xr.Dataset()

        # remove leap years
        ds = ds.loc[{
            'time': ~((ds['time.month'] == 2) & (ds['time.day'] == 29))}]
        
        # do transformation
        ds1[varname] = (ds.tasmin - 237.15)**power

        # document variable
        ds1[varname].attrs['unit'] = 'degrees C'
        ds1[varname].attrs['oneline'] = description.splitlines()[0]
        ds1[varname].attrs['description'] = description
        ds1[varname].attrs['variable'] = varname

    tas_poly.__doc__ = description

    transformation_spec = {
        'transformation_name': varname,
        'variable': 'tas',
        'transformation': tas_poly,
        'unit': 'degreesC-pow{}'.format(power)
    }

    return transformation_spec


JOBS = [create_polynomial_transformation(i) for i in range(1, 9)]

PERIODS = (
    [dict(rcp='rcp45', read_acct='jiacany', year=y) for y in range(1981, 2100)] +
    [dict(rcp='rcp85', read_acct='jiacany', year=y) for y in range(1981, 2100)])

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
    'variable', 'source_variable', 'transformation', 'unit', 'scenario',
    'year', 'model', 'agglev', 'aggwt']


def run_job(
        metadata,
        variable,
        transformation,
        source_variable,
        unit,
        rcp,
        pername,
        year,
        model,
        agglev,
        aggwt,
        weights=None):

    # Add to job metadata
    metadata.update(dict(
        time_horizon='{}-{}'.format(years[0], years[-1])))
    metadata.update(ADDITIONAL_METADATA)

    logger.debug('Beginning job:\n\tkwargs:\t{}'.format(
        pprint.pformat(metadata, indent=2)))

    read_file = BCSD_orig_files.format(**metadata)
    write_file = WRITE_PATH.format(**metadata)
    
    # do not duplicate
    if os.path.isfile(write_file):
        return

    # Get transformed data
    fp = read_file.format(year=year)
    
    logging.debug('year {} - attempting to read file "{}"'.format(year, fp))
    ds = (load_bcsd(fp, source_variable, broadcast_dims=('time',))
                .pipe(transformation))

    # Reshape to regions
    if not agglev.startswith('grid'):
        logger.debug('aggregating to "{}" using "{}"'.format(agglev, aggwt))
        ds = weighted_aggregate_grid_to_regions(
                ds, variable, aggwt, agglev, weights=weights)

    # Update netCDF metadata
    ds.attrs.update(**{k: str(v)
        for k, v in metadata.items() if k in INCLUDED_METADATA})
    ds.attrs.update(ADDITIONAL_METADATA)

    # Write output
    if not os.path.isdir(os.path.dirname(write_file)):
        logger.debug('attempting to create_directory "{}"'
            .format(os.path.dirname(write_file)))

        os.makedirs(os.path.dirname(write_file))

    logger.debug('attempting to write to file "{}"'.format(write_file))

    ds.to_netcdf(write_file)

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
