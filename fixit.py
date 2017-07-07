
import xarray as xr
import numpy as np
import glob

pattern_f = '/shares/gcp/climate/BCSD/Mortality/polynomial-fix/hierid/popwt/daily/*/*/*/*/1.1.nc4'
sample_f = '/shares/gcp/climate/BCSD/Mortality/polynomial-fix/hierid/popwt/daily/tas/rcp85/BNU-ESM/2024/1.1.nc4'


def fix_dataset(ds):

    if 'day' in ds.dims:
        ds = ds.rename({'day': 'time'})

    ds = ds.transpose('time', 'hierid')

    var = ds.data_vars.keys()[0]

    for attrs in [ds.attrs, ds[var].attrs]:
        unit = attrs.pop('unit', None)
        if unit:
            attrs['units'] = unit

    oneline = ds[var].attrs.pop('oneline', None)
    if oneline:
        ds[var].attrs['long_title'] = oneline.split('  ')[0]

    return ds


def validate(ds, jiacan):
    var = ds.data_vars.keys()[0]

    for k in jiacan.attrs.keys():
        assert k in ds.attrs, k

    for k in jiacan.data_vars.values()[0].attrs.keys():
        assert k in ds[var].attrs, k


def postprocess(f, jiacan):

    with xr.open_dataset(f) as ds:
        ds.load()

    ds = fix_dataset(ds)
    validate(ds, jiacan)

    ds.to_netcdf(f)


def main():
    jiacan = xr.open_dataset(
        '/shares/gcp/climate/BCSD/Mortality/degree_days/tas/' +
        'rcp85/inmcm4/tas_exceedance_degree_days_r1i1p1_inmcm4_2032.nc')

    for f in glob.iglob(pattern_f):
        print(f)
        try:
            postprocess(f, jiacan)
        except AssertionError as e:
            print('nope. {} {}'.format(f, e))


if __name__ == "__main__":
    main()
