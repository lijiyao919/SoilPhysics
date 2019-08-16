import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
import pandas as pd
import numpy as np

LOW_LEFT_CORNR_LONGITUDE = -114.5
LOW_LEFT_CORNER_LATITUDE = 36.5
UP_RIGHT_CORNER_LONGITUDE = -105
UP_RIGHT_CORNER_LATITUDE = 42.3
MIN_NYC_ISLAND_TO_VISUALIZ = 0.0001

df = pd.read_csv('Soil_2017-09-25T00-00.csv')
df.replace(-999999, -0.1, inplace=True)

sm_2 = np.array(df['sm_2'])
sm_4 = np.array(df['sm_4'])
sm_8 = np.array(df['sm_8'])
sm_20 = np.array(df['sm_20'])
sm_40 = np.array(df['sm_40'])
Lat = np.array(df['Latitude'])
Lon = np.array(df['Longitude'])
TOTAL_STAT_NUM = len(sm_2)

def plotBarChart(stat_num=TOTAL_STAT_NUM):
    index = np.arange(stat_num)
    bar_width = 0.1

    plt.bar(index, sm_2[0:stat_num], bar_width, color='b', label='sm_2')
    plt.bar(index + bar_width, sm_4[0:stat_num], bar_width, color='r', label='sm_4')
    plt.bar(index + 2*bar_width, sm_8[0:stat_num], bar_width, color='g', label='sm_8')
    plt.bar(index + 3*bar_width, sm_20[0:stat_num], bar_width, color='y', label='sm_20')
    plt.bar(index + 4*bar_width, sm_40[0:stat_num], bar_width, color='pink', label='sm_40')

    plt.xlabel('Stations')
    plt.ylabel('Moisture')
    plt.title('Soil Moisuture')
    plt.xticks(index + bar_width, range(1,stat_num+1))
    plt.legend()

    plt.tight_layout()
    plt.show()

def plotSpatial(var='sm_2'):
    #fig, axes = plt.subplots(1, 2, figsize=(10,8))
    data={'sm_2':sm_2, 'sm_4':sm_4, 'sm_8':sm_8, 'sm_20':sm_20, 'sm_40':sm_40}
    map = Basemap(resolution='h',  # c, l, i, h, f or None
                     projection='merc',
                     lat_0=39.32, lon_0=-111.8,
                     area_thresh=MIN_NYC_ISLAND_TO_VISUALIZ,
                     llcrnrlon=LOW_LEFT_CORNR_LONGITUDE,
                     llcrnrlat=LOW_LEFT_CORNER_LATITUDE,
                     urcrnrlon=UP_RIGHT_CORNER_LONGITUDE,
                     urcrnrlat=UP_RIGHT_CORNER_LATITUDE)
    map.drawcoastlines()
    map.drawcountries()
    #map.fillcontinents(color='white', lake_color='aqua') #cannot go with hexbin
    map.drawmapboundary(fill_color='white')
    map.drawrivers()
    map.drawparallels(np.arange(36, 43, 1), labels=[1, 0, 0, 0], )
    map.drawmeridians(np.arange(-114.8, -105, 1), labels=[0, 0, 0, 1])
    #map.readshapefile(shapefile='./puma/utah_administrative', name='comarques')

    Lon_m, Lat_m = map(Lon, Lat)
    thiscmap = plt.cm.get_cmap('viridis')

    map.hexbin(Lon_m, Lat_m, C=data[var], gridsize=[20,20], cmap=thiscmap)
    map.colorbar(location='right')
    plt.show()


plotBarChart()
plotSpatial('sm_2')