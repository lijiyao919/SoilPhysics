# -*- coding: utf-8 -*
#Import suds for connecting to the web service.
from suds.client import Client
import csv
import datetime
import numpy as np
from MesoPy import *
from multiprocessing.pool import ThreadPool as Pool
import traceback
import time

#* Precipitation time setting *#
class Precipitation(object):
    beginDate = ""
    beginDate_snortel=""
    beginDate_Meso=""
    def __init__(self, beginDate ,beginDate_snortel, beginDate_Meso):
        self.beginDate = beginDate
        self.beginDate_snortel=beginDate_snortel #Begin Date in Snortel Format
        self.beginDate_Meso=beginDate_Meso #Begin Date in MesoWest Format


NumberOfDays = 5 # Number of days precipitation accumulation is needed
defaultValue=-999999 # says value is missing or abnormal
delay_iutah=20
header=[["Serial Number", "Station Name", "Station Id", "Network", "Elevation(meter)", "Latitude", "Longitude", "Wind Speed(m/s)", "Air Temperature(C)",
         "Start Date", "Precipitation for 1 day", "Precipitation for 2 days", "Precipitation for 3 days", "Precipitation for 4 days", "Precipitation for 5 days",
         "sm_2", "sm_4", "sm_8", "sm_20", "sm_40", "st_2", "st_4", "st_8", "st_20", "st_40"]]
PrecipitationPeriod = []


def isActive(x): #check if a station is still active or not
    # * time setting *#
    dt = datetime.datetime
    today = dt.today()
    if dt.strptime(x.endDate, "%Y-%m-%d %H:%M:%S").date() > today.date():
        return True

def getStationDataFromIUtah(serviceArray, siteCode, data_array_iUtah, i, count, startDate):
    try:
        print('Start IUtah Station: {}'.format(i+1))
        startDate_iutah_str = str(startDate.strftime("%Y-%m-%dT%H:%M"))
        # * retrieving info & parameters *#
        networkCode = 'iutah'
        PREC = 'Precip_Tot_Avg'  # Total Precipitation, cm
        qcLevelCode = '0'  # quality contral level

        if (siteCode[0] == 'R'): # distinguishing Rebutte and Logan river regions
            service = serviceArray[1]
        else:
            service = serviceArray[0]
        data_array_iUtah[i][0] = i+count  # serial number

        time.sleep(i * delay_iutah)
        #print('Thread {} access all sites'.format(i+1))
        SiteObject = service.GetValuesForASiteObject(networkCode + ':' + siteCode, startDate_iutah_str, startDate_iutah_str)
        #Site Info
        data_array_iUtah[i][1] = SiteObject.timeSeries[0].sourceInfo.siteName # site name
        data_array_iUtah[i][2] = siteCode  # site short name
        data_array_iUtah[i][3] = "iUtah"  # network Name
        data_array_iUtah[i][4] = SiteObject.timeSeries[0].sourceInfo.elevation_m  # elevation
        data_array_iUtah[i][5] = SiteObject.timeSeries[0].sourceInfo.geoLocation.geogLocation.latitude  # latitude
        data_array_iUtah[i][6] = SiteObject.timeSeries[0].sourceInfo.geoLocation.geogLocation.longitude #longitude

        #Wind Speed
        variable_wind_speed = SiteObject.timeSeries[3].values[0].value[0].value
        if variable_wind_speed == "" or variable_wind_speed == -9999:
            value_wind_speed = defaultValue  # missing data
        else:
            data_array_iUtah[i][7] = variable_wind_speed  # wind speed

        #Air Temp
        variable_air_temp = SiteObject.timeSeries[54].values[0].value[0].value
        if variable_air_temp == "" or variable_air_temp == -9999:
            variable_air_temp = defaultValue  # missing data
        else:
            data_array_iUtah[i][8] = variable_air_temp  # air temp

        #time
        data_array_iUtah[i][9] = str(startDate)

        # Soil Moisture
        col_sms = 15
        k = 0
        for j in range(5):
            value_SMS = float(SiteObject.timeSeries[28+k].values[0].value[0].value)
            data_array_iUtah[i][col_sms+j] = defaultValue if value_SMS == -9999.0 else value_SMS / 100  # sms, convert pct to decimal
            k=k+4

        # Soil Temperature
        col_st = 20
        k=0
        for j in range(5):
            value_ST = float(SiteObject.timeSeries[29+k].values[0].value[0].value)
            data_array_iUtah[i][col_st+j] = defaultValue if value_ST == -9999.0 else round(value_ST, 2)
            k=k+4

        # Precipitation
        prec = SiteObject.timeSeries[6].values[0].value[0].value

        for j in range(0, NumberOfDays):
            if prec == "" :
                data_array_iUtah[i][10 + j] = defaultValue
            else:
                #print('Thread {} access precs'.format(i+1))
                dayPrecipitate = service.GetValuesObject(networkCode + ':' + siteCode,
                                                         networkCode + ':' + PREC
                                                         + '/qualityControlLevelCode=' + qcLevelCode,
                                                         PrecipitationPeriod[j + 1].beginDate,
                                                         PrecipitationPeriod[j + 1].beginDate)
                # validating the data obtained and if failed assigning default value
                data_array_iUtah[i][10 + j] = defaultValue if dayPrecipitate.timeSeries[0].values[0] == "" or \
                                                              dayPrecipitate.timeSeries[0].values[0].value[0] == "" or \
                                                              float(prec) < float(dayPrecipitate.timeSeries[0].values[0].value[0].value) \
                                                           else round((float(prec) - float(dayPrecipitate.timeSeries[0].values[0].value[0].value)) * 10, 4)

        print('---Finish IUtah Station: {}'.format(i + 1))
        '''print (data_array_iUtah[i][0],data_array_iUtah[i][1],data_array_iUtah[i][2],data_array_iUtah[i][3],data_array_iUtah[i][4],
               data_array_iUtah[i][5],data_array_iUtah[i][6],data_array_iUtah[i][7],data_array_iUtah[i][8], data_array_iUtah[i][9],
               data_array_iUtah[i][10], data_array_iUtah[i][11], data_array_iUtah[i][12], data_array_iUtah[i][13], data_array_iUtah[i][14],
               data_array_iUtah[i][15], data_array_iUtah[i][16], data_array_iUtah[i][17], data_array_iUtah[i][18], data_array_iUtah[i][19],
               data_array_iUtah[i][20], data_array_iUtah[i][21], data_array_iUtah[i][22], data_array_iUtah[i][23],data_array_iUtah[i][24])'''
    except Exception as e:
        print('IUtah Station {} Fail'.format(i+1))
        print(traceback.format_exc())


def getIUtahData(count, startDate):
    # Create a new object named "service_LoganRiver" for calling the web service methods
    wsdlURL = 'http://data.iutahepscor.org/loganriverwof/cuahsi_1_1.asmx?WSDL'
    service_LoganRiver = Client(wsdlURL).service

    # Create a new object named "serviceRebutte" for calling the web service methods
    wsdlrebutteURL = 'http://data.iutahepscor.org/redbuttecreekwof/cuahsi_1_1.asmx?WSDL'
    service_Rebutte = Client(wsdlrebutteURL).service

    siteCodes = ['LR_FB_C', 'LR_GC_C', 'LR_TG_C', 'LR_TWDEF_C', 'RB_ARBR_C', 'RB_GIRF_C', 'RB_KF_C', 'RB_TM_C']
    num_sites = len(siteCodes)
    serviceArray = [service_LoganRiver, service_Rebutte]
    data_array_iUtah = [[None] * 25 for row in range(num_sites)]

    pool = Pool(processes=3)
    for i, validSite in enumerate(siteCodes):
        pool.apply_async(getStationDataFromIUtah, (serviceArray, validSite, data_array_iUtah, i, count, startDate))
        #getStationDataFromIUtah(serviceArray, validSite, data_array_iUtah, i, count)
    pool.close()
    pool.join()

    return data_array_iUtah

def getStationDataFromSnotel(awdb, validSite, heights, data_array_snortel, i, count, startDate):
    try:
        print('Start Snortel Station: {}'.format(i+1))
        sensor_WSPDV = r"WSPDV"  # WSPDV, WIND SPEED AVERAGE (Hourly), mph
        sensor_TOBS = r"TOBS"  # TOBS, AIR TEMPERATURE OBSERVED, Fahrenheit
        sensor_PREC = r"PREC"  # PREC, PRECIPITATION ACCUMULATION, Inches
        sensor_SMS = r"SMS"  # SMS, SOIL MOISTURE PERCENT (Hourly), pct
        sensor_STO = r"STO"  # STO, SOIL TEMPERATURE OBSERVED, Fahrenheit
        starDate_Snortel_str = str(startDate)

        data_array_snortel[i][0] = count + i  # serial number
        geo = awdb.service.getStationMetadata(validSite.stationTriplet)  # geo-information: elevation (ft), longtitude, altitude
        data_array_snortel[i][1] = geo.name  # station name
        data_array_snortel[i][3] = "SNORTEL"
        data_array_snortel[i][4] = 0.3048 * geo.elevation  # elevation, convert ft to meter
        data_array_snortel[i][5] = geo.latitude  # latitude
        data_array_snortel[i][6] = geo.longitude  # longitude

        # wind speed (mph) of the retrieved date
        wind_speed = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_WSPDV, 1, None,
                                                       starDate_Snortel_str, starDate_Snortel_str, 'ALL',
                                                       'ENGLISH')
        data_array_snortel[i][7] = defaultValue if 'beginDate' not in wind_speed[0] or 'endDate' not in wind_speed[0] or \
                                                   wind_speed[0].values[0] == "" or 'value' not in wind_speed[0].values[0] \
                                                else 0.44704 * wind_speed[0].values[0].value  # wind_speed, convert mph to m/s

        # air temperature (Fahrenheit) of the retrieved date
        air_temp = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_TOBS, 1, None,
                                                     starDate_Snortel_str, starDate_Snortel_str, 'ALL',
                                                     'ENGLISH')
        data_array_snortel[i][8] = defaultValue if 'beginDate' not in air_temp[0] or 'endDate' not in air_temp[0] or \
                                                   air_temp[0].values[0] == "" or 'value' not in air_temp[0].values[0] \
                                                else (air_temp[0].values[0].value - 32) * 5 / 9  # air temp, convert Fahrenheit to Celsius

        # height variables at different depth*#
        for z in heights:
            if z.unitCd == "in":
                if z.value == -2:
                    heights_2 = [z]
                elif z.value == -4:
                    heights_4 = [z]
                elif z.value == -8:
                    heights_8 = [z]
                elif z.value == -20:
                    heights_20 = [z]
                elif z.value == -40:
                    heights_40 = [z]

        col_sm = 15
        col_st = 20
        for height in [heights_2, heights_4, heights_8, heights_20, heights_40]:
            # soil moisture (pct)
            sm = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_SMS, 1, height,
                                                   starDate_Snortel_str,
                                                   starDate_Snortel_str, 'ALL', 'ENGLISH')
            # soil temperature (Fahrenheit)
            st = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_STO, 1, height,
                                                   starDate_Snortel_str, starDate_Snortel_str, 'ALL',
                                                   'ENGLISH')
            data_array_snortel[i][col_sm] = defaultValue if 'beginDate' not in sm[0] or 'endDate' not in sm[0] or \
                                                            sm[0].values[0] == "" or 'value' not in sm[0].values[0] \
                                                         else sm[0].values[0].value / 100  # sms, convert pct to decimal
            data_array_snortel[i][col_st] = defaultValue if 'beginDate' not in st[0] or 'endDate' not in st[0] or \
                                                            st[0].values[0] == "" or 'value' not in st[0].values[0] \
                                                         else round((st[0].values[0].value - 32) * 5 / 9, 2)  # sto, convert Fahrenheit to Celsius
            col_sm = col_sm + 1
            col_st = col_st + 1

        # prec (inches) of the retrieved date
        prec = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_PREC, 1, None,
                                                 starDate_Snortel_str, starDate_Snortel_str, 'ALL',
                                                 'ENGLISH')
        data_array_snortel[i][2] = prec[0].stationId  # station ID
        data_array_snortel[i][9] = startDate
        for j in range(0, NumberOfDays):
            dayPrecipitate = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_PREC, 1, None,
                                                               PrecipitationPeriod[j + 1].beginDate_snortel,
                                                               PrecipitationPeriod[j + 1].beginDate_snortel, 'ALL',
                                                               'ENGLISH')  # prec (inches)

            data_array_snortel[i][10 + j] = defaultValue if 'beginDate' not in dayPrecipitate[0] or \
                                                            'endDate' not in dayPrecipitate[0] or \
                                                            'values' not in dayPrecipitate[0] or \
                                                            'values' not in prec[0] or \
                                                            prec[0].values[0].value < dayPrecipitate[0].values[0].value \
                                                         else 25.4 * (prec[0].values[0].value - dayPrecipitate[0].values[0].value)
        print('---Finish Snorel Station: {}'.format(i+1))
        '''print (data_array_snortel[i][0], data_array_snortel[i][1], data_array_snortel[i][2], data_array_snortel[i][3], data_array_snortel[i][4], data_array_snortel[i][5],
               data_array_snortel[i][6], data_array_snortel[i][7], data_array_snortel[i][8], data_array_snortel[i][9], data_array_snortel[i][10], data_array_snortel[i][11],
               data_array_snortel[i][12], data_array_snortel[i][13], data_array_snortel[i][14], data_array_snortel[i][15], data_array_snortel[i][16], data_array_snortel[i][17],
               data_array_snortel[i][18], data_array_snortel[i][19], data_array_snortel[i][20], data_array_snortel[i][21], data_array_snortel[i][22], data_array_snortel[i][23],data_array_snortel[i][24])'''
    except Exception as e:
        print('Snortel Station {} Fail'.format(i+1))
        print(traceback.format_exc())

def getSnortelData(count, startDate):
    # * AWDB (Air-Water Database) web service *#
    wsdl = r"https://wcc.sc.egov.usda.gov/awdbWebService/services?WSDL"
    awdb = Client(wsdl)
    network = r"SNTL"        # SNTL network
    state = r"UT"            # STATE

    # get stations' info from SCAN
    stations = awdb.service.getStations("*", state, network, "*", "*", -1000, 1000, -1000, 1000, 0, 29000, "*", 1, None, True)
    #print('station: ',stations)
    # get height format
    heights = awdb.service.getHeightDepths()
    # get metadata of the stations from SCAN *#
    meta = awdb.service.getStationMetadataMultiple(stations)  # get metadata of the stations from SCAN
    meta[:] = [x for x in meta if isActive(x)]  # eliminate the inactive stations
    num_stations_validate = len(meta)  # number of active stations

    # * a data array for storing retrieved data *#
    data_array_snortel = [[None]*25 for row in range(num_stations_validate)]  # a data array for storing retrieved data

    pool = Pool()
    for i, validSite in enumerate(meta):
        pool.apply_async(getStationDataFromSnotel, (awdb, validSite, heights, data_array_snortel, i, count, startDate))
    pool.close()
    pool.join()

    return data_array_snortel

def getStationDataFromScan(awdb, validSite, heights, data_array_scan, i, count, startDate):
    try:
        print('Start Scan Station: {}'.format(i+1))
        sensor_WSPDV = r"WSPDV"  # WSPDV, WIND SPEED AVERAGE (Hourly), mph
        sensor_TOBS = r"TOBS"  # TOBS, AIR TEMPERATURE OBSERVED, Fahrenheit
        sensor_PREC = r"PREC"  # PREC, PRECIPITATION ACCUMULATION, Inches
        sensor_SMS = r"SMS"  # SMS, SOIL MOISTURE PERCENT (Hourly), pct
        sensor_STO = r"STO"  # STO, SOIL TEMPERATURE OBSERVED, Fahrenheit
        startDate_str = str(startDate)

        data_array_scan[i][0] = count + i  # serial number
        geo = awdb.service.getStationMetadata(validSite.stationTriplet)  # geo-information: elevation (ft), longtitude, altitude
        data_array_scan[i][1] = geo.name  # station name
        data_array_scan[i][3] = "SCAN"
        data_array_scan[i][4] = 0.3048 * geo.elevation  # elevation, convert ft to meter
        data_array_scan[i][5] = geo.latitude  # latitude
        data_array_scan[i][6] = geo.longitude  # longitude

        # wind speed (mph) of the retrieved date
        wind_speed = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_WSPDV, 1, None,
                                                       startDate_str, startDate_str, 'ALL',
                                                       'ENGLISH')
        data_array_scan[i][7] = -999999 if 'beginDate' not in wind_speed[0] or 'endDate' not in wind_speed[0] or \
                                           wind_speed[0].values[0] == "" or 'value' not in wind_speed[0].values[0] \
            else 0.44704 * wind_speed[0].values[0].value  # wind_speed, convert mph to m/s

        # air temperature (Fahrenheit) of the retrieved date
        air_temp = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_TOBS, 1, None,
                                                     startDate_str, startDate_str, 'ALL',
                                                     'ENGLISH')
        data_array_scan[i][8] = -999999 if 'beginDate' not in air_temp[0] or 'endDate' not in air_temp[0] or \
                                           air_temp[0].values[0] == "" or 'value' not in air_temp[0].values[0] \
            else (air_temp[0].values[0].value - 32) * 5 / 9  # air temp, convert Fahrenheit to Celsius

        for z in heights:
            if z.unitCd == "in":
                if z.value == -2:
                    heights_2 = [z]
                elif z.value == -4:
                    heights_4 = [z]
                elif z.value == -8:
                    heights_8 = [z]
                elif z.value == -20:
                    heights_20 = [z]
                elif z.value == -40:
                    heights_40 = [z]

        col_sm = 15
        col_st = 20
        for height in [heights_2, heights_4, heights_8, heights_20, heights_40]:
            # soil moisture (pct)
            sm = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_SMS, 1, height, startDate_str, startDate_str, 'ALL', 'ENGLISH')
            # soil temperature (Fahrenheit)
            st = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_STO, 1, height, startDate_str, startDate_str, 'ALL', 'ENGLISH')
            data_array_scan[i][col_sm] = defaultValue if 'beginDate' not in sm[0] or 'endDate' not in sm[0] or \
                                                         sm[0].values[0] == "" or 'value' not in sm[0].values[0] \
                                                      else sm[0].values[0].value / 100  # sms, convert pct to decimal
            data_array_scan[i][col_st] = defaultValue if 'beginDate' not in st[0] or 'endDate' not in st[0] or \
                                                         st[0].values[0] == "" or 'value' not in st[0].values[0] \
                                                      else round((st[0].values[0].value - 32) * 5 / 9, 2)  # sto, convert Fahrenheit to Celsius
            col_sm = col_sm + 1
            col_st = col_st + 1

        prec = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_PREC, 1, None,
                                                 startDate_str, startDate_str, 'ALL',
                                                 'ENGLISH')  # prec (inches) of the retrieved date
        data_array_scan[i][2] = prec[0].stationId  # station ID
        data_array_scan[i][9] = startDate
        for j in range(0, NumberOfDays):
            dayPrecipitate = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_PREC, 1, None,
                                                               PrecipitationPeriod[j + 1].beginDate_snortel,
                                                               PrecipitationPeriod[j + 1].beginDate_snortel, 'ALL',
                                                               'ENGLISH')  # prec (inches) of 5 day before
            data_array_scan[i][10 + j] = defaultValue if 'beginDate' not in dayPrecipitate[0] or 'endDate' not in dayPrecipitate[0] or \
                                                         'beginDate' not in prec[0] or 'endDate' not in prec[0] or \
                                                          prec[0].values[0].value < dayPrecipitate[0].values[0].value \
                                                      else 25.4 * (prec[0].values[0].value - dayPrecipitate[0].values[0].value)
        print('---Finish Scan Station: {}'.format(i + 1))
        '''print (data_array_scan[i][0], data_array_scan[i][1], data_array_scan[i][2], data_array_scan[i][3], data_array_scan[i][4], data_array_scan[i][5],
               data_array_scan[i][6], data_array_scan[i][7], data_array_scan[i][8], data_array_scan[i][9], data_array_scan[i][10], data_array_scan[i][11],
               data_array_scan[i][12], data_array_scan[i][13], data_array_scan[i][14], data_array_scan[i][15], data_array_scan[i][16], data_array_scan[i][17],
               data_array_scan[i][18], data_array_scan[i][19], data_array_scan[i][20], data_array_scan[i][21], data_array_scan[i][22], data_array_scan[i][23])'''
    except Exception as e:
        print('Scan Station {} Fail'.format(i+1))
        print(traceback.format_exc())


def getScanData(count, startDate):
    wsdl = r"https://wcc.sc.egov.usda.gov/awdbWebService/services?WSDL"
    awdb = Client(wsdl)
    # * retrieving info & parameters *#
    network = r"SCAN"  # SCAN network
    state = r"UT"        # STATE

    # height variables at different depth*#
    heights = awdb.service.getHeightDepths()
    # * get stations' info from SCAN *#
    stations = awdb.service.getStations("*", state, network, "*", "*", -1000, 1000, -1000, 1000, 0, 29000, "*", 1, None, True)

    # * get metadata of the stations from SCAN *#
    meta = awdb.service.getStationMetadataMultiple(stations)  # get metadata of the stations from SCAN
    meta[:] = [x for x in meta if isActive(x)]  # eliminate the inactive stations
    num_stations_validate = len(meta)  # number of active stations

    # * a data array for storing retrieved data *#
    data_array_scan = [[None]*25 for row in range(num_stations_validate)]  # a data array for storing retrieved data

    pool = Pool()
    for i, validSite in enumerate(meta):
        pool.apply_async(getStationDataFromScan, (awdb, validSite, heights, data_array_scan, i, count, startDate))
    pool.close()
    pool.join()

    return data_array_scan

def getStationDataFromMesoWest(r, validSite, data_array_Meso, i, count, startDate):
    try:
        print('Start MesoWest Station: {}'.format(i + 1))
        starDate_Meso_str = str(startDate.strftime("%Y%m%d%H%M"))
        # * geo, wind_speed, and air temp info *#
        station_serialNo = i + count  # serial number
        station_name = validSite['NAME']  # station name
        station_id = validSite['STID']  # station ID
        station_elevation = validSite['ELEVATION']  # elevation (ft)
        station_latitude = validSite['LATITUDE']  # latitude
        station_longitude = validSite['LONGITUDE']  # longitude
        wind_speed = r.timeseries(state='UT', stid=station_id, start=starDate_Meso_str, end=starDate_Meso_str,
                                  vars='wind_speed')  # wind speed (m/s) of the retrieved date
        station_wind_speed = wind_speed['STATION'][0]['OBSERVATIONS']['wind_speed_set_1'][0]
        air_temp = r.timeseries(state='UT', stid=station_id, start=starDate_Meso_str, end=starDate_Meso_str,
                                vars='air_temp')  # air temp  (Celsius) of the retrieved date
        station_air_temp = air_temp['STATION'][0]['OBSERVATIONS']['air_temp_set_1'][0]


        data_array_Meso[i][0] = station_serialNo  # serial number
        data_array_Meso[i][1] = station_name
        data_array_Meso[i][2] = station_id
        data_array_Meso[i][3] = "MESOWEST"
        data_array_Meso[i][4] = 0.3048 * int(station_elevation.encode('utf-8'))  # convert ft to m   # unicode
        data_array_Meso[i][5] = station_latitude
        data_array_Meso[i][6] = station_longitude
        data_array_Meso[i][7] = station_wind_speed
        data_array_Meso[i][8] = station_air_temp
        data_array_Meso[i][9] = startDate
        for j in range(0, NumberOfDays):
            dayPrecipitate = r.precip(state='UT', stid=station_id, start=PrecipitationPeriod[j + 1].beginDate_Meso,
                                      end=starDate_Meso_str, units='precip|in', showemptystations=1)
            dayPrecipitate = dayPrecipitate['STATION'][0]
            data_array_Meso[i][10 + j] = defaultValue if 'OBSERVATIONS' not in dayPrecipitate or not dayPrecipitate[
                'OBSERVATIONS'] \
                else 25.4 * (dayPrecipitate['OBSERVATIONS']['total_precip_value_1'])

        data_array_Meso[i][15] = defaultValue
        data_array_Meso[i][17] = defaultValue
        data_array_Meso[i][18] = defaultValue
        data_array_Meso[i][19] = defaultValue
        data_array_Meso[i][20] = defaultValue
        data_array_Meso[i][22] = defaultValue
        data_array_Meso[i][23] = defaultValue
        data_array_Meso[i][24] = defaultValue
        # * SMS at the retrieving date *#
        num_sms_positions = len(validSite['SENSOR_VARIABLES']['soil_moisture'])
        sms_observations = validSite['OBSERVATIONS']

        metadata_sms = r.metadata(state='UT', stid=station_id, sensorvars=1,
                                  vars='soil_moisture')  # use metadata to retrieve depth information
        sms_depth = metadata_sms['STATION'][0]['SENSOR_VARIABLES']['soil_moisture']
        if num_sms_positions == 1:
            if 'position' not in sms_depth['soil_moisture_1'] or not sms_depth['soil_moisture_1']['position'] or \
                            sms_depth['soil_moisture_1']['position'] == "":  # all sms position are empty
                sms_position_1 = None
            else:
                sms_position_1 = sms_depth['soil_moisture_1']['position'].encode('utf-8')
            if sms_observations['soil_moisture_set_1'] == "" or not sms_observations[
                'soil_moisture_set_1']:  # all sms are empty
                sms_observations_1 = None
            else:
                sms_observation_1 = sms_observations['soil_moisture_set_1'][0]
                data_array_Meso[i][16] = sms_observation_1 / 100  # sms at the 1st depth, convert pct to decimal

                # * SMS at the retrieving date *#
        data_sto = r.timeseries(state='UT', stid=station_id, start=starDate_Meso_str, end=starDate_Meso_str,
                                vars='soil_temp')  # soil_temp at the retrieving date
        data_sto = data_sto['STATION'][0]
        num_sto_positions = len(data_sto['SENSOR_VARIABLES']['soil_temp'])
        sto_observations = data_sto['OBSERVATIONS']

        metadata_sto = r.metadata(state='UT', stid=station_id, sensorvars=1,
                                  vars='soil_temp')  # use metadata to retrieve depth information
        sto_depth = metadata_sto['STATION'][0]['SENSOR_VARIABLES']['soil_temp']

        if num_sto_positions == 1:
            if 'position' not in sto_depth['soil_temp_1'] or sto_depth['soil_temp_1']['position'] == "" or not \
                    sto_depth['soil_temp_1']['position']:  # all sto position are empty
                sto_position_1 = None
            else:
                sto_position_1 = sto_depth['soil_temp_1']['position']
                # sto_position_1 = int(sto_position_1.encode('utf-8'))  # unicode
            if sto_observations['soil_temp_set_1'] == "" or not sto_observations['soil_temp_set_1']:  # all sto are empty
                sto_observation_1 = None
            else:
                sto_observation_1 = sto_observations['soil_temp_set_1'][0]
                data_array_Meso[i][21] = sto_observation_1  # sms at the 1st depth

        print('---Finish MesoWest Station: {}'.format(i + 1))
        '''if num_sms_positions == 1 and num_sto_positions == 1:
            print(data_array_Meso[i][0], data_array_Meso[i][1], data_array_Meso[i][2], data_array_Meso[i][3], data_array_Meso[i][4], data_array_Meso[i][5], 
                   data_array_Meso[i][6], data_array_Meso[i][7], data_array_Meso[i][8], data_array_Meso[i][9], data_array_Meso[i][10], data_array_Meso[i][11], 
                   data_array_Meso[i][12], data_array_Meso[i][13], data_array_Meso[i][14], data_array_Meso[i][15], data_array_Meso[i][16],
                   data_array_Meso[i][17], data_array_Meso[i][18], data_array_Meso[i][19], data_array_Meso[i][20], data_array_Meso[i][21],
                   data_array_Meso[i][22], data_array_Meso[i][23])'''
    except Exception as e:
        print('MesoWest Station {} Fail'.format(i+1))
        print(traceback.format_exc())

def getMesoWestData(count, startDate):
    your_api_token = '2805d3f32c3446bbb7aef75f2d95dcae'  # my Mesowest API token#
    r = Meso(your_api_token)
    starDate_Meso_str = str(startDate.strftime("%Y%m%d%H%M"))

    # * retrieve stations that have SMS measurement in UT in Mesowest *#
    data_sms = r.timeseries(state='UT', start=starDate_Meso_str, end=starDate_Meso_str, vars='soil_moisture')  # sms data (%) in UT (stid ='CLSPT': 3-depth sms;
    stations_sms = data_sms['STATION']  # list of stations which have sms data
    validSite_sms = [x for x in stations_sms if int(x['MNET_ID']) != 29 and int(x['MNET_ID']) != 198
                     and x['STID'] != 'CLSPT' and x['STID'] != 'EDGUT' and x['STID'] != 'FG007'
                     and x['STID'] != 'FG014' and x['STID'] != 'NLGUT']  # exclude Mesowest stations that overlapped SCAN& UTAH by net ID;
    # NLGUT: 1 depth, no metadata; CLSPT: 3 depth; EDGUT: 2 depth
    num_validSite = len(validSite_sms)  # number of stations

    # * a data array for storing retrieved data *#
    data_array_Meso = [[None]*25 for row in range(num_validSite)]

    pool = Pool()
    for i, validSite in enumerate(validSite_sms):
        pool.apply_async(getStationDataFromMesoWest, (r, validSite, data_array_Meso, i, count, startDate))
    pool.close()
    pool.join()

    return data_array_Meso

def run(start_date):
    dt=datetime.datetime
    startTime_all = dt.now()  # for counting program running time

    SnortelArray_len = 0
    ScanArray_len = 0
    MesoWestArray_len = 0

    startDate = dt.strptime(start_date, "%Y-%m-%d %H:%M")  # date to retrieve soil moisture and precipitation
    startDate_str = str(startDate.strftime("%Y-%m-%dT%H:%M"))



    for i in range(0, NumberOfDays + 1):
            tdelta = datetime.timedelta(days=i)
            beginDate = str((startDate - tdelta).strftime("%Y-%m-%dT%H:%M"))
            beginDate_snortel=str(startDate - tdelta)
            beginDate_Meso= str((startDate - tdelta).strftime("%Y%m%d%H%M"))
            dayPrecipitation = Precipitation(beginDate, beginDate_snortel, beginDate_Meso)
            PrecipitationPeriod.append(dayPrecipitation) # adding all the dates needed for

    # Data from Snortel Network
    startTime_sntl = dt.now()
    try:
        SnortelArray=getSnortelData(1, startDate)
        SnortelArray_len = len(SnortelArray)
    except:
        print('The Snortel Network has been crashed down.')
        SnortelArray = [['Snortel Fail.'] * 25]
    endTime_sntl = dt.now()
    print ("SNTL Time: %s" % (endTime_sntl - startTime_sntl))

    #Data from Scan Network
    startTime_scan = dt.now()
    try:
        ScanArray=getScanData(1+SnortelArray_len, startDate)
        ScanArray_len=len(ScanArray)
    except:
        print('The Scan Network has been crashed down.')
        ScanArray = [['Scan Fail.'] * 25]
    endTime_scan = dt.now()
    print ("SCAN Time: %s" % (endTime_scan - startTime_scan))

    #Data from MesoWest Network
    startTime_meso = dt.now()
    try:
        MesoWestArray=getMesoWestData(1+SnortelArray_len+ScanArray_len, startDate)
        MesoWestArray_len=len(MesoWestArray)
    except:
        print('The MesoWest Network has been crashed down.')
        MesoWestArray=[['MesoWest Fail.']*25]
    endTime_meso = dt.now()
    print ("MESO Time: %s" % (endTime_meso - startTime_meso))

    #Data from IUtah Network
    startTime_iutah = dt.now()
    try:
        IUtahArray=getIUtahData(1+SnortelArray_len+ScanArray_len+MesoWestArray_len, startDate)
    except:
        print('The IUtah Network has been crashed down.')
        IUtahArray=[['IUtah Fail.']*25]
    endTime_iutah = dt.now()
    print ("IUTAH Time: %s" % (endTime_iutah - startTime_iutah))

    #Combining data from all networks
    data_array = np.vstack((header, SnortelArray, ScanArray, MesoWestArray, IUtahArray))

    startDate_str=startDate_str.replace(":","-")
    with open('Parallel_'+startDate_str+'.csv','wb') as f: #write in csv file
        writer = csv.writer(f)
        writer.writerows(data_array)  # data summary
    endTime_all = dt.now()  #for counting program running time
    print ("Overall Time: %s" % (endTime_all - startTime_all)) #output run time

run("2017-09-25 00:00")








