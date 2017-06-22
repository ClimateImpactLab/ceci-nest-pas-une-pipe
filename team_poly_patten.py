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
    '{baseline_model}/{source_variable}/' +
    '{source_variable}_baseline_1986-2005_r1i1p1_{baseline_model}_{{season}}.nc')

BCSD_pattern_files = (
    '/global/scratch/{read_acct}/nasa_bcsd/pattern/SMME_surrogate/' +
    '{scenario}/{source_variable}/{model}/' +
    '{source_variable}_BCSD_{model}_{scenario}_r1i1p1_{{season}}_{year}.nc')

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
    file='/annual_average_tas_pattern.py',
    execute='python annual_average_tas_pattern.py --run',
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
        ds1[varname].attrs['unit'] = 'degreesC-poly{}'.format(power)
        ds1[varname].attrs['oneline'] = description.splitlines()[0]
        ds1[varname].attrs['description'] = description
        ds1[varname].attrs['variable'] = varname

    tas_poly.__doc__ = description

    transformation_spec = {
        'variable': varname,
        'source_variable': 'tas',
        'transformation': tas_poly,
        'unit': 'degreesC-poly{}'.format(power)
    }

    return transformation_spec


JOBS = [create_polynomial_transformation(i) for i in range(1, 10)]

PERIODS = (
    [dict(scenario='rcp45', read_acct='mdelgado', year=y) for y in range(1981, 2100)] +
    [dict(scenario='rcp85', read_acct='jiacany', year=y) for y in range(1981, 2100)])

rcp_models = {
    'rcp45': 
        list(map(lambda x: dict(model=x[0], baseline_model=x[1]), [
            ('pattern1','MRI-CGCM3'),
            ('pattern2','GFDL-ESM2G'),
            ('pattern3','MRI-CGCM3'),
            ('pattern4','GFDL-ESM2G'),
            ('pattern5','MRI-CGCM3'),
            ('pattern6','GFDL-ESM2G'),
            ('pattern27','GFDL-CM3'),
            ('pattern28','CanESM2'),
            ('pattern29','GFDL-CM3'),
            ('pattern30','CanESM2'), 
            ('pattern31','GFDL-CM3'), 
            ('pattern32','CanESM2')])),

    'rcp85':
        list(map(lambda x: dict(model=x[0], baseline_model=x[1]), [
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
            ('pattern33','CanESM2')]))}

MODELS = []

for spec in PERIODS:
    for model in rcp_models[spec['scenario']]:
        job = {}
        job.update(spec)
        job.update(model)
        MODELS.append(job)



SEASONS = [ 'DJF', 'MAM', 'JJA', 'SON']

AGGREGATIONS = [
    {'agglev': 'hierid', 'aggwt': 'areawt'}]


JOB_SPEC = [JOBS, MODELS, AGGREGATIONS]

INCLUDED_METADATA = [
    'variable', 'source_variable', 'transformation', 'unit', 'scenario',
    'year', 'model', 'agglev', 'aggwt']

def run_job(
        metadata,
        variable,
        source_variable,
        unit,
        transformation,
        scenario,
        year,
        model,
        read_acct,
        baseline_model,
        agglev,
        aggwt,
        weights=None):

    logger.debug('Beginning job\nkwargs:\t{}'.format(
        pprint.pformat(metadata, indent=2)))

    # Add to job metadata
    metadata.update(dict(
        time_horizon='{}-{}'.format(years[0], years[-1])))
    metadata.update(ADDITIONAL_METADATA)


    baseline_file = BASELINE_FILE.format(**metadata)
    pattern_file = BCSD_pattern_files.format(**metadata)
    write_file = WRITE_PATH.format(**metadata)
    
    # do not duplicate
    if os.path.isfile(write_file):
        return
    
    # Get transformed data
    total = None

    seasonal_baselines = {}
    for season in SEASONS:
        basef = baseline_file.format(season=season)
        logger.debug('attempting to load baseline file: {}'.format(basef))
        seasonal_baselines[season] = load_baseline(basef, source_variable)

    season_month_start = {'DJF': 12, 'MAM': 3, 'JJA': 6, 'SON': 9}

    seasonal = []

    for s, season in enumerate(SEASONS):

        pattf = pattern_file.format(year=year, season=season)
        logger.debug('attempting to load pattern file: {}'.format(pattf))
        patt = load_bcsd(pattf, source_variable, broadcast_dims=('day',))

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

    ds = xr.concat(seasonal, dim='time').pipe(transformation)

    # Reshape to regions

    logger.debug('{} reshaping to regions'.format(model))
    if not agglev.startswith('grid'):
        ds = weighted_aggregate_grid_to_regions(
                ds, variable, aggwt, agglev, weights=weights)

    # Update netCDF metadata
    logger.debug('{} udpate metadata'.format(model))
    ds.attrs.update(**{k: str(v)
        for k, v in metadata.items() if k in INCLUDED_METADATA})
    ds.attrs.update(ADDITIONAL_METADATA)

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
