import os
import logging
import datetime


__author__='J Simcock'
__contact__='jsimcock@rhg.com'
__version__= '1.0'


rcp_baseline = os.path.expanduser('~/data/Degreedays_aggregated_{rcp}_r1i1p1_{baseline}.nc')

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
        'merge_hdd_cdd_part2.py'),
    file=str(__file__),
    execute='python {} '.format(__file__),
    project='gcp',
    team='climate',
    probability_method='SMME',
    description= 'GCP regional agrregated data', 
    dependencies= str(['GCP-climate-nasa_bcsd-SMME_formatted.1.0', 'Agglomerated-Many.2016-02-17 NASA-GDDP']), 
    created= str(datetime.datetime.now())
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


  file_bcsd = rcp_baseline.format(rcp=rcp, baseline=combo['baseline_model'])
  file_pattern = rcp_pattern.format(rcp=rcp, pattern=combo['model'])

  ds_bcsd = xr.open_dataset(file_bcsd)

  ds_pattern = xr.open_dataset(file_pattern)

  ds_bcsd.rename({'SHAPENUM': 'hierid'}, inplace=True)
  ds_bcsd['hierid'] = ds_pattern.hierid
  ds_bcsd['time'] = range(1981, 2100)
  
  ds = xr.merge([ds_bcsd.isel(time=slice(0,25)), ds_pattern])
  ds.attrs.update(ADDITIONAL_METADATA)
  print(ds)

  print(ds.hotdd_agg)
  print(ds.cold_agg)




if __name__ == '__main__':

  for rcp, d in rcp_models.items():
    for combo in d:

      merge_patterns(rcp, combo)