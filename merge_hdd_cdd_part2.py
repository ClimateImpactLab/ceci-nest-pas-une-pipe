import os
import logging
import datetime


__author__='J Simcock'
__contact__='jsimcock@rhg.com'
__version__= '1.1 '


rcp_baseline = '/global/scratch/jsimcock/projection/gcp/climate/hierid/popwt/tasmax_degree_days/historical/{baseline}/1.0.nc4'

rcp_pattern =  '/global/scratch/jsimcock/projection/gcp/climate/hierid/popwt/tasmax_degree_days/{rcp}/{pattern}/1.0.nc4'


WRITE_FILE = '/global/scratch/jsimcock/projection/gcp/climate/hierid/popwt/tasmax_degree_days/Degreedays_aggregated_{rcp}_r1i1p1_{pattern}.nc'
  

ADDITIONAL_METADATA = dict(
    # oneline=oneline,
    # description=description,
    author=__author__,
    contact=__contact__,
    version=__version__,
    repo=(
        'gitlab.com:ClimateImpactLab/Climate/climate-transforms-tas-poly' +
        'merge_hdd_cdd.py'),
    file=str(__file__),
    execute='python {} '.format(__file__),
    project='gcp',
    team='climate',
    probability_method='SMME',
    description= 'GCP regional agrregated data', 
    dependencies= str(['GCP-climate-nasa_bcsd-SMME_formatted.1.0', 'Agglomerated-Many.2016-02-17 NASA-GDDP']), 
    created= str(datetime.datetime.now())
    )


hotdd_agg_METADATA= dict(
          long_title = "aggregation of Hot degree days where tasmax is greater than 30 degree celsuis in impact regions",
          units = "degree days", 
          source = ''
                        )



coldd_agg_METADATA= dict(
                long_title = "aggregation of Cold degree days where tasmax is less than 10 degree celsuis in impact regions", 
                units = "degree days", 
                source= ''
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




def merge_patterns(rcp,combo):


  import xarray as xr
  import pandas as pd
  import metacsv


  file_bcsd = rcp_baseline.format(baseline=combo['baseline_model'])
  file_pattern = rcp_pattern.format(rcp=rcp, pattern=combo['model'])
  

  write_path = WRITE_FILE.format(rcp=rcp, pattern=combo['model'])

  ds_bcsd = xr.open_dataset(file_bcsd)

  ds_pattern = xr.open_dataset(file_pattern)

  
  ds = xr.merge([ds_bcsd, ds_pattern])


  #update metadata for ds and vars
  ds.attrs.update(ADDITIONAL_METADATA)
  ds.attrs.update({'model': combo['model']})
  ds.attrs.update({'baseline_model': combo['baseline_model']})

  hotdd_agg_METADATA.update({'source': file_pattern})
  coldd_agg_METADATA.update({'source': file_pattern})

  print(hotdd_agg_METADATA)
  print(coldd_agg_METADATA)
 
  
  ds['hotdd_agg'] = ds.hotdd_agg.astype('float32')
  ds['coldd_agg'] = ds.coldd_agg.astype('float32')
  ds['time'] = ds.time.astype('float32')

  ds['hotdd_agg'].attrs.update(hotdd_agg_METADATA)
  ds['coldd_agg'].attrs.update(coldd_agg_METADATA)
  ds['time'].attrs.update(time_METADATA)
  ds['hierid'].attrs.update(hierid_METADATA)


  print(ds['hotdd_agg'].attrs)
  print(ds['coldd_agg'].attrs)

  varattrs = {var: dict(ds[var].attrs) for var in ds.data_vars.keys()}


  header_file = os.path.splitext(write_path)[0] + '.fgh'

  metacsv.to_header(
        header_file,
        attrs=dict(ds.attrs),
        variables= varattrs)

  if not os.path.isdir(os.path.dirname(write_path)):
      os.path.makedir(os.path.dirname(write_path))

  ds.to_netcdf(write_path)


if __name__ == '__main__':

  for rcp, d in rcp_models.items():
    for combo in d:

      merge_patterns(rcp, combo)