
import os
import itertools
import pandas as pd

inpaths = (
    '../playground/csv3/global-quants-v2.3/hierid/' +
    'global_{var}_{rcp}_{per}_{rel}_{unit}_percentiles.csv')

outpaths = (
    '../playground/csv3/cities-v2.3/{var}/' +
    'city-summary_{var}-{city}.csv')

cities = [
    ('India-NewDelhi', 'IND.10.121.371'),
    ('USA-WashingtonDC', 'USA.9.317'), 
    ('USA-PhoenixAZ', 'USA.3.101'),
    ('Egypt-Cairo', 'EGY.11'),
    ('Spain-Madrid', 'ESP.8.33.235.5912'),
    ('China-Beijing', 'CHN.2.18.78'), #Includes (XiCheng, DongCheng, Chaoyang, HaiDian, FengTai and ShiJingShan districts)
    ('USA-Houston', 'USA.44.2640'),
    ('USA-Dallas', 'USA.1.24'),
    ('India-Bangalore', 'IND.17.218.765'),
    ('UAE-Dubai', 'ARE.3'),
    ('SuadiArabia-Riyadh', 'SAU.7'),
    ('France-Paris', 'FRA.11.75'),
    ('Greece-Athens', 'GRC.2.6'),
    ('China-Shanghai-Puxi', 'CHN.25.262.1764'),
    # ('Japan-Tokyo', 'JPN.41'),
    ('Somalia-Mogadishu', 'SOM.3.10'),
    ('Brazil-Brasilia', 'BRA.7.804.1862'),
    ('Mexico-MexicoCity','MEX.9.264'),
    ('USA-NewYorkNY', 'USA.33.1862'),
    ('USA-ChicagoIL', 'USA.14.608')
    ]

rcps = ['rcp45', 'rcp85']
pers = ['1986-2005', '2020-2039', '2040-2059', '2080-2099']
rels = ['absolute', 'change-from-hist']

index_names = ['rcp', 'per', 'rel']
keys = list(itertools.product(rcps, pers, rels))
formatters = list(map(lambda val: dict(zip(index_names, val)), keys))

for var, unit in [('tasmax-over-118F', 'days-over-118F')]:
    bigdf = pd.concat([
        pd.read_csv(inpaths.format(var=var, unit=unit, **val), index_col=0) for val in formatters],
        keys=pd.MultiIndex.from_tuples(keys, names=['rcp', 'per', 'rel']),
        axis=0)


    for city, hierid in cities:
        fp = outpaths.format(city=city, var=var)
        if not os.path.isdir(os.path.dirname(fp)):
            os.makedirs(os.path.dirname(fp))

        bigdf.xs(hierid, level='hierid').to_csv(fp)
