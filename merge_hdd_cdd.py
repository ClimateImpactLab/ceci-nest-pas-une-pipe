'''
Script to merge single year outputs into multi-year output
'''
import os
import logging

from toolz import memoize

from jrnr import slurm_runner

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)

logger = logging.getLogger('uploader')
logger.setLevel('DEBUG')


__author__='J Simcock'
__contact__='jsimcock@rhg.com'
__version__= '1.0'


READ_PATH_PATTERN_COLDD = (
    '/global/scratch/jsimcock/projection/gcp/climate/hierid/popwt/daily/' +
    'tasmax_hdd10/{scenario}/{model}/{year}/1.0.nc4')


READ_PATH_PATTERN_HOTDD = (
    '/global/scratch/jsimcock/projection/gcp/climate/hierid/popwt/daily/' +
    'tasmax_cdd30/{scenario}/{model}/{year}/1.0.nc4')


BCSD_pattern_archive = (
    'GCP/climate/nasa_bcsd/SMME_formatted/{scenario}/{model}/' +
    'tasmax/{year}.nc')

WRITE_FILE = '/global/scratch/jsimcock/projection/gcp/climate/hierid/popwt/tasmax_degree_days/{scenario}/{model}/1.0.nc4'

# READ_PATH_BCSD = ( 
#                   )


ADDITIONAL_METADATA = dict(
    # oneline=oneline,
    # description=description,
    author=__author__,
    contact=__contact__,
    version=__version__,
    repo=(
        'https://github.com/ClimateImpactLab/ceci-nest-pas-une-pipe/' +
        'merge_hdd_cdd.py'),
    file=str(__file__),
    execute='python {} run'.format(__file__),
    project='gcp',
    team='climate',
    probability_method='SMME',
    dependencies='GCP-climate-nasa_bcsd-SMME_formatted.1.0')


hotdd_agg_METADATA= dict(
    long_title = "aggregation of Hot degree days that tasmax is greater than 30 degree celsuis in impact regions",
    units = "degree days", 
    source = "Regional aggregated hotdd from /global/scratch/jiacany/nasa_bcsd/Labor/degree_days/tasmax/rcp85/ACCESS1-0/tasmax_exceedance_degree_days_rcp85_r1i1p1_ACCESS1-0.nc", 
                        )



coldd_agg_METADATA= dict(
                long_title = "aggregation of Cold degree days that tasmax is less than 10 degree celsuis in impact regions", 
                units = "degree days",
                source = "Regional aggregated colddd from /global/scratch/jiacany/nasa_bcsd/Labor/degree_days/tasmax/rcp85/ACCESS1-0/tasmax_exceedance_degree_days_rcp85_r1i1p1_ACCESS1-0.nc"
                )

hierid_METADATA = dict(
              long_title = "Impact Region", 
              units = "None", 
              source = "aggregated-world-many.2016-02-17" 
                )

time_METADATA = dict(
                long_title='calendar years',
                units='YYYY'
                )

SCEN = [dict(scenario='rcp45'), dict(scenario='rcp85')]

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

years = range(2006, 2100)

JOB_SPEC =[]

for spec in SCEN:
    for model in rcp_models[spec['scenario']]:
        job = {}
        job.update(spec)
        job.update(model)
        JOB_SPEC.append(job)

def onfinish():
    print('all done!')


def merge_future_years(job):


  import xarray as xr
  import pandas as pd
  import metacsv

  #update metadata dict
  job.update(ADDITIONAL_METADATA)

  #set write_path
  write_path = WRITE_FILE.format(**job)

  logger.debug(
        'attempting to read model {}, scenario {} combo'.format(job['model'], job['scenario']))

  #load paths
  paths_hdd = [READ_PATH_PATTERN_HOTDD.format(scenario=job['scenario'], model=job['model'], year=y) for y in years]

  paths_cdd = [READ_PATH_PATTERN_COLDD.format(scenario=job['scenario'], model=job['model'], year=y) for y in years]

  #open files and set index to values in years
  ds_hdd = xr.open_mfdataset(paths_hdd, concat_dim=pd.Index(years, name='time', dtype='float32'))
  ds_cdd = xr.open_mfdataset(paths_cdd, concat_dim=pd.Index(years, name='time', dtype='float32'))

  logger.debug(
        'opening files for model {}, scenario {} combo'.format(job['model'], job['scenario']))

  #merge datasets
  merged = xr.merge([ds_cdd[ds_cdd.variable], ds_hdd[ds_hdd.variable]])

  logger.debug(
        'attempting to merge datasets for model {}, scenario {} combo'.format(job['model'], job['scenario']))
  
  #update internal metadata for each variable and dimension
  merged.hierid.attrs.update(hierid_METADATA)
  merged.time.attrs.update(time_METADATA)
  merged[ds_hdd.variable].attrs.update(hotdd_agg_METADATA)
  merged[ds_cdd.variable].attrs.update(coldd_agg_METADATA)

  logger.debug(
        'Updating metadata and variable names datasets for model {}, scenario {} combo'.format(job['model'], job['scenario']))

  #rename variables
  merged.rename({ds_cdd.variable: 'coldd_agg'}, inplace=True)
  merged.rename({ds_hdd.variable: 'hotdd_agg'}, inplace=True)
  merged.coldd_agg.attrs.update({'variable': 'coldd_agg'})
  merged.coldd_agg.attrs.pop('description')
  merged.hotdd_agg.attrs.update({'variable': 'hotdd_agg'})
  merged.hotdd_agg.attrs.pop('description')


  if not os.path.isdir(os.path.dirname(write_path)):
        os.makedirs(os.path.dirname(write_path))

  merged.to_netcdf(write_path)

  logger.debug(
        'Attempting to write to {}'.format(write_path))

  header_file = os.path.splitext(write_path)[0] + '.fgh'

  merged.attrs.update(ADDITIONAL_METADATA)

  attrs = dict(merged.attrs)
  attrs['file_dependencies'] = str([BCSD_pattern_archive.format(scenario=job['scenario'], model=job['model'], year=y) for y in years])

  varattrs = {var: dict(merged[var].attrs) for var in merged.data_vars.keys()}

  metacsv.to_header(
        header_file,
        attrs=dict(attrs),
        variables=varattrs)

  logger.debug(
        'Attempting to write to metacsv header file {}'.format(header_file))


if __name__ == '__main__':

  for job in JOB_SPEC:
    merge_future_years(job)


