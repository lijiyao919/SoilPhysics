# -*- coding: utf-8 -*-

"""
Created on April 11, 2018
@author: Rong Zhou
"""

import sys
from inspect import getsourcefile
from os.path import abspath, basename
from suds.client import Client
import datetime
import os
from MesoPy import *
import time
# import plotly.graph_objs as go
# import plotly.offline as py
import numpy as np
# from itertools import chain
import csv
from time import sleep


#* time setting *#
dt = datetime.datetime
date = datetime.date
today = dt.today()
start_time  = dt.now() #for counting program running time

#created Precipitation Data object
class Precipitation(object):
    beginDate = ""
    endDate = ""
    beginDate_snortel=""
    endDate_snortel=""
    beginDate_Meso=""
    endDate_meso=""
    def __init__(self, beginDate, endDate,beginDate_snortel,endDate_snortel,beginDate_Meso,endDate_meso):
        self.beginDate = beginDate
        self.endDate = endDate
        self.beginDate_snortel=beginDate_snortel #Begin Date in Snortel Format
        self.endDate_snortel=endDate_snortel #End Date in Snortel Format
        self.beginDate_Meso=beginDate_Meso #Begin Date in MesoWest Format
        self.endDate_meso=endDate_meso #End Date in Mesowest Format

def isActive(x): #check if a station is still active or not
    if dt.strptime(x.endDate, "%Y-%m-%d %H:%M:%S").date() > today.date():
        return True

#* retrieving date. Please change date here to see data for soil moisture *#
startDate = dt.strptime("2017-09-9 0:00:00","%Y-%m-%d %H:%M:%S") #date to retrieve soil moisture
delta = datetime.timedelta(days=14) #Please change number here to increase or decrease the number of days to be considered currently its 14
finalDate=startDate-delta
directoryName=str(startDate).replace(":","-")
directory="./"+directoryName+"/"
os.makedirs(directory) # creates the directory to save each timestamps data for that particular date
while startDate>=finalDate:
    startDate_str = str(startDate.strftime("%Y-%m-%dT%H:%M"))
    starDate_Meso_str = str(startDate.strftime("%Y%m%d%H%M"))
    starDate_Snortel_str = str(startDate)
    endDate_str = startDate_str
    PrecipitationPeriod = []
    NumberOfDays = 5  # Number of days precipitation accumulation is needed
    defaultValue = -999999  # says value is missing or abnormal
    for i in range(0, NumberOfDays + 1):
        tdelta = datetime.timedelta(days=i)
        beginDate = str((startDate - tdelta).strftime("%Y-%m-%dT%H:%M"))
        endDate = beginDate
        beginDate_snortel = str(startDate - tdelta)
        beginDate_Meso = str((startDate - tdelta).strftime("%Y%m%d%H%M"))
        endDate_meso = beginDate_Meso
        endDate_snortel = beginDate_snortel
        dayPrecipitation = Precipitation(beginDate, endDate, beginDate_snortel, endDate_snortel, beginDate_Meso,
                                         endDate_meso)
        PrecipitationPeriod.append(dayPrecipitation)  # adding all the dates needed for
    data_array = [[defaultValue] * 25 for row in range(1, 200)]  # dataArray initialization with default values

    # def GetIUtahData(count):
    #     # * retrieving info & parameters *#
    #     networkCode = 'iutah'
    #     air_temp = 'AirTemp_ST110_Avg'  # avaeraged air temperature , degC
    #     wind_speed = 'WindSp_Avg'  # Wind speed, m/s
    #     SMS_5cm = 'VWC_5cm_Avg'  # Volumetric water content: -5cm, -2inch, pct
    #     SMS_10cm = 'VWC_10cm_Avg'  # Volumetric water content: -10cm, -4inch, pct
    #     SMS_20cm = 'VWC_20cm_Avg'  # Volumetric water content: -20cm, -8inch, pct
    #     SMS_50cm = 'VWC_50cm_Avg'  # Volumetric water content: -50cm, -20inch, pct
    #     SMS_100cm = 'VWC_100cm_Avg'  # Volumetric water content: -100cm, -40inch, pct
    #     ST_5cm = 'SoilTemp_5cm_Avg'  # soil temprature: -5cm, -2inch, degC
    #     ST_10cm = 'SoilTemp_10cm_Avg'  # soil temprature: -10cm, -4inch, degC
    #     ST_20cm = 'SoilTemp_20cm_Avg'  # soil temprature: -20cm, -8inch, degC
    #     ST_50cm = 'SoilTemp_50cm_Avg'  # soil temprature: -50cm, -20inch, degC
    #     ST_100cm = 'SoilTemp_100cm_Avg'  # soil temprature: -100cm, -40inch, degC
    #     PREC = 'Precip_Tot_Avg'  # Total Precipitation, cm
    #     qcLevelCode = '0'  # quality contral level
    #     # * ---1) wsdl URL of logan river watershed--- *#
    #     wsdlURL = 'http://data.iutahepscor.org/loganriverwof/cuahsi_1_1.asmx?WSDL'
    #     service_LoganRiver = Client(
    #         wsdlURL).service  # Create a new object named "service" for calling the web service methods
    #     # siteCode_loganriver = ['LR_FB_C','LR_GC_C','LR_TG_C','LR_TWDEF_C'] #sites in logan river watershed
    #     # num_sites_loganriver = len(siteCode_loganriver)
    #     wsdlrebutteURL = 'http://data.iutahepscor.org/redbuttecreekwof/cuahsi_1_1.asmx?WSDL'
    #     # Create a new object named "serviceRebutte" for calling the web service methods
    #     serviceRebutte = Client(wsdlrebutteURL).service
    #     siteCodes = ['LR_FB_C', 'LR_GC_C', 'LR_TG_C', 'LR_TWDEF_C', 'RB_ARBR_C', 'RB_GIRF_C', 'RB_KF_C', 'RB_TM_C']
    #     # siteCodes = ['LR_FB_C']
    #     num_sites = len(siteCodes)
    #     serviceArray = []
    #     serviceArray.append(service_LoganRiver)
    #     serviceArray.append(serviceRebutte)
    #     data_array_iUtah = [[defaultValue] * 25 for row in range(1, num_sites + 2)]
    #     # Adding headers to the excel
    #     data_array_iUtah[0][0] = "Serial Number"
    #     data_array_iUtah[0][1] = "Station Name"
    #     data_array_iUtah[0][2] = "Station Id"
    #     data_array_iUtah[0][3] = "Network"
    #     data_array_iUtah[0][4] = "Elevation(meter)"
    #     data_array_iUtah[0][5] = "Latitude"
    #     data_array_iUtah[0][6] = "Longitude"
    #     data_array_iUtah[0][7] = "Wind Speed(m/s)"
    #     data_array_iUtah[0][8] = "Air Temperature(C)"
    #     data_array_iUtah[0][9] = "Start Date"
    #     data_array_iUtah[0][10] = "Precipitation for 1 day"
    #     data_array_iUtah[0][11] = "Precipitation for 2 days"
    #     data_array_iUtah[0][12] = "Precipitation for 3 days"
    #     data_array_iUtah[0][13] = "Precipitation for 4 days"
    #     data_array_iUtah[0][14] = "Precipitation for 5 days"
    #     data_array_iUtah[0][15] = "sm_2"
    #     data_array_iUtah[0][16] = "sm_4"
    #     data_array_iUtah[0][17] = "sm_8"
    #     data_array_iUtah[0][18] = "sm_20"
    #     data_array_iUtah[0][19] = "sm_40"
    #     data_array_iUtah[0][20] = "st_2"
    #     data_array_iUtah[0][21] = "st_4"
    #     data_array_iUtah[0][22] = "st_8"
    #     data_array_iUtah[0][23] = "st_20"
    #     data_array_iUtah[0][24] = "st_40"
    #     i = 1
    #     for siteCode_loganriverIndex, siteCode_loganriverShort in enumerate(siteCodes):
    #         if (siteCode_loganriverShort[0] == 'R'):  # distinguishing Rebutte and Logan river regions
    #             service = serviceArray[1]
    #         else:
    #             service = serviceArray[0]
    #         data_array_iUtah[i][0] = i  # serial number
    #         siteInfoResult = service.GetSiteInfoObject(networkCode + ':' + siteCode_loganriverShort)
    #         siteName = siteInfoResult.site[0].siteInfo.siteName
    #         data_array_iUtah[i][1] = siteName  # site name
    #         data_array_iUtah[i][2] = siteCode_loganriverShort  # site short name
    #         data_array_iUtah[i][3] = "iUtah"  # network Name
    #         siteElevation = siteInfoResult.site[0].siteInfo.elevation_m
    #         data_array_iUtah[i][4] = siteElevation  # elevation
    #         siteLatitude = siteInfoResult.site[0].siteInfo.geoLocation.geogLocation.latitude
    #         data_array_iUtah[i][5] = siteLatitude  # latitude
    #         siteLongitude = siteInfoResult.site[0].siteInfo.geoLocation.geogLocation.longitude
    #         data_array_iUtah[i][6] = siteLongitude  # Longitude
    #
    #         # * retrieving wind speed at the retrieving day*#
    #         variable_wind_speed = service.GetValuesObject(networkCode + ':' + siteCode_loganriverShort,
    #                                                       networkCode + ':' + wind_speed
    #                                                       + '/qualityControlLevelCode=' + qcLevelCode, startDate_str,
    #                                                       startDate_str)  # retrieving wind speed at the retrieving day
    #         # exclude the missing data for wind speed
    #         if variable_wind_speed.timeSeries[0].values[0] == "" or variable_wind_speed.timeSeries[0].values[
    #             0].value == -9999:
    #             value_wind_speed = defaultValue  # missing data
    #         else:
    #             value_wind_speed = variable_wind_speed.timeSeries[0].values[0].value[0].value
    #             data_array_iUtah[i][7] = value_wind_speed  # wind speed
    #
    #         # retrieving air temp at the retrieving day*#
    #         variable_air_temp = service.GetValuesObject(networkCode + ':' + siteCode_loganriverShort,
    #                                                     networkCode + ':' + air_temp
    #                                                     + '/qualityControlLevelCode=' + qcLevelCode, startDate_str,
    #                                                     startDate_str)  # retrieving air temp at the retrieving day
    #         # exclude the missing data for air temp
    #         data_array_iUtah[i][8] = defaultValue if variable_air_temp.timeSeries[0].values[0] == "" or \
    #                                                  variable_air_temp.timeSeries[0].values[0].value == -9999 \
    #             else variable_air_temp.timeSeries[0].values[0].value[0].value
    #         prec = service.GetValuesObject(networkCode + ':' + siteCode_loganriverShort, networkCode + ':' + PREC
    #                                        + '/qualityControlLevelCode=' + qcLevelCode, startDate_str, startDate_str)
    #         data_array_iUtah[i][9] = str(startDate)
    #         for j in range(0, NumberOfDays):
    #             dayPrecipitate = service.GetValuesObject(networkCode + ':' + siteCode_loganriverShort,
    #                                                      networkCode + ':' + PREC
    #                                                      + '/qualityControlLevelCode=' + qcLevelCode,
    #                                                      PrecipitationPeriod[j + 1].beginDate,
    #                                                      PrecipitationPeriod[j + 1].beginDate)
    #             # validating the data obtained and if failed assigning default value
    #             data_array_iUtah[i][10 + j] = defaultValue if prec.timeSeries[0].values[0] == "" or \
    #                                                           prec.timeSeries[0].values[0].value[0] == "" or \
    #                                                           dayPrecipitate.timeSeries[0].values[0] == "" or \
    #                                                           dayPrecipitate.timeSeries[0].values[0].value[0] == "" or \
    #                                                           float(
    #                                                               prec.timeSeries[0].values[0].value[0].value) < float(
    #                                                               dayPrecipitate.timeSeries[0].values[0].value[0].value) \
    #                 else round((float(prec.timeSeries[0].values[0].value[0].value) - float(
    #                 dayPrecipitate.timeSeries[0].values[0].value[0].value)) * 10, 4)
    #
    #         # retrieving SMS of five depths at the retrieving day *#
    #         variable_SMS_5cm = service.GetValuesObject(networkCode + ':' + siteCode_loganriverShort,
    #                                                    networkCode + ':' + SMS_5cm  # SMS_5cm
    #                                                    + '/qualityControlLevelCode=' + qcLevelCode, startDate_str,
    #                                                    startDate_str)
    #         value_SMS5cm = float(variable_SMS_5cm.timeSeries[0].values[0].value[0].value)
    #         data_array_iUtah[i][15] = defaultValue if value_SMS5cm == -9999.0 \
    #             else value_SMS5cm / 100  # sms_5cm, convert pct to decimal
    #
    #         variable_SMS_10cm = service.GetValuesObject(networkCode + ':' + siteCode_loganriverShort,
    #                                                     networkCode + ':' + SMS_10cm  # SMS_10cm
    #                                                     + '/qualityControlLevelCode=' + qcLevelCode, startDate_str,
    #                                                     startDate_str)
    #         value_SMS10cm = float(variable_SMS_10cm.timeSeries[0].values[0].value[0].value)
    #         data_array_iUtah[i][16] = defaultValue if value_SMS10cm == -9999.0 \
    #             else value_SMS10cm / 100  # sms_5cm, convert pct to decimal
    #
    #         variable_SMS_20cm = service.GetValuesObject(networkCode + ':' + siteCode_loganriverShort,
    #                                                     networkCode + ':' + SMS_20cm  # SMS_20cm
    #                                                     + '/qualityControlLevelCode=' + qcLevelCode, startDate_str,
    #                                                     startDate_str)
    #         value_SMS20cm = float(variable_SMS_20cm.timeSeries[0].values[0].value[0].value)
    #         data_array_iUtah[i][17] = defaultValue if value_SMS20cm == -9999.0 \
    #             else value_SMS20cm / 100  # sms_5cm, convert pct to decimal
    #
    #         variable_SMS_50cm = service.GetValuesObject(networkCode + ':' + siteCode_loganriverShort,
    #                                                     networkCode + ':' + SMS_50cm  # SMS_50cm
    #                                                     + '/qualityControlLevelCode=' + qcLevelCode, startDate_str,
    #                                                     startDate_str)
    #         value_SMS50cm = float(variable_SMS_50cm.timeSeries[0].values[0].value[0].value)
    #         data_array_iUtah[i][18] = value_SMS50cm / 100  # sms_50cm, convert pct to decimal
    #
    #         variable_SMS_100cm = service.GetValuesObject(networkCode + ':' + siteCode_loganriverShort,
    #                                                      networkCode + ':' + SMS_100cm  # SMS_100cm
    #                                                      + '/qualityControlLevelCode=' + qcLevelCode, startDate_str,
    #                                                      startDate_str)
    #         value_SMS100cm = float(variable_SMS_100cm.timeSeries[0].values[0].value[0].value)
    #         data_array_iUtah[i][19] = defaultValue if value_SMS100cm == -9999.0 \
    #             else value_SMS100cm / 100  # sms_100cm, convert pct to decimal
    #
    #         # retrieving STO of five depths at the retrieving day *#
    #         variable_ST_5cm = service.GetValuesObject(networkCode + ':' + siteCode_loganriverShort,
    #                                                   networkCode + ':' + ST_5cm  # ST_5cm
    #                                                   + '/qualityControlLevelCode=' + qcLevelCode, startDate_str,
    #                                                   startDate_str)
    #         value_ST5cm = float(variable_ST_5cm.timeSeries[0].values[0].value[0].value)
    #         data_array_iUtah[i][20] = defaultValue if value_ST5cm == -9999.0 \
    #             else round(value_ST5cm, 2)  # ST_5cm
    #
    #         variable_ST_10cm = service.GetValuesObject(networkCode + ':' + siteCode_loganriverShort,
    #                                                    networkCode + ':' + ST_10cm  # ST_10cm
    #                                                    + '/qualityControlLevelCode=' + qcLevelCode, startDate_str,
    #                                                    startDate_str)
    #         value_ST10cm = float(variable_ST_10cm.timeSeries[0].values[0].value[0].value)
    #         data_array_iUtah[i][21] = defaultValue if value_ST10cm == -9999.0 \
    #             else round(value_ST10cm, 2)  # ST_10cm
    #
    #         variable_ST_20cm = service.GetValuesObject(networkCode + ':' + siteCode_loganriverShort,
    #                                                    networkCode + ':' + ST_20cm  # ST_20cm
    #                                                    + '/qualityControlLevelCode=' + qcLevelCode, startDate_str,
    #                                                    startDate_str)
    #         value_ST20cm = float(variable_ST_20cm.timeSeries[0].values[0].value[0].value)
    #         data_array_iUtah[i][22] = defaultValue if value_ST20cm == -9999.0 \
    #             else round(value_ST20cm, 2)  # ST_20cm
    #
    #         variable_ST_50cm = service.GetValuesObject(networkCode + ':' + siteCode_loganriverShort,
    #                                                    networkCode + ':' + ST_50cm  # ST_50cm
    #                                                    + '/qualityControlLevelCode=' + qcLevelCode, startDate_str,
    #                                                    startDate_str)
    #         value_ST50cm = float(variable_ST_50cm.timeSeries[0].values[0].value[0].value)
    #         data_array_iUtah[i][23] = defaultValue if value_ST50cm == -9999.0 \
    #             else round(value_ST50cm, 2)  # ST_50cm
    #
    #         variable_ST_100cm = service.GetValuesObject(networkCode + ':' + siteCode_loganriverShort,
    #                                                     networkCode + ':' + ST_100cm  # ST_100cm
    #                                                     + '/qualityControlLevelCode=' + qcLevelCode, startDate_str,
    #                                                     startDate_str)
    #         value_ST100cm = float(variable_ST_100cm.timeSeries[0].values[0].value[0].value)
    #         data_array_iUtah[i][24] = defaultValue if value_ST100cm == -9999.0 \
    #             else round(value_ST100cm, 2)  # ST_100cm
    #         print (
    #             data_array_iUtah[i][0], data_array_iUtah[i][1], data_array_iUtah[i][2], data_array_iUtah[i][3],
    #             data_array_iUtah[i][4],
    #             data_array_iUtah[i][5], data_array_iUtah[i][6], data_array_iUtah[i][7],
    #             data_array_iUtah[i][8], data_array_iUtah[i][9]
    #             , data_array_iUtah[i][10], data_array_iUtah[i][11], data_array_iUtah[i][12],
    #             data_array_iUtah[i][13], data_array_iUtah[i][14], data_array_iUtah[i][15], data_array_iUtah[i][16],
    #             data_array_iUtah[i][17], data_array_iUtah[i][18], data_array_iUtah[i][19], data_array_iUtah[i][20],
    #             data_array_iUtah[i][21],
    #             data_array_iUtah[i][22], data_array_iUtah[i][23], data_array_iUtah[i][24])
    #         i += 1
    #         # data_array.append(data_array_iUtah)
    #     return data_array_iUtah


    def GetSnortelData(count):
        fileName = basename(sys.argv[0])
        pathName = abspath(getsourcefile(lambda: 0))

        # * AWDB (Air-Water Database) web service *#
        wsdl = r"https://wcc.sc.egov.usda.gov/awdbWebService/services?WSDL"
        svc_url = r"https://wcc.sc.egov.usda.gov/awdbWebService/services"
        awdb = Client(wsdl)
        network = r"SNTL"  # SNTL network
        sensor_WSPDV = r"WSPDV"  # WSPDV, WIND SPEED AVERAGE (Hourly), mph
        sensor_TOBS = r"TOBS"  # TOBS, AIR TEMPERATURE OBSERVED, Fahrenheit
        sensor_PREC = r"PREC"  # PREC, PRECIPITATION ACCUMULATION, Inches
        sensor_SMS = r"SMS"  # SMS, SOIL MOISTURE PERCENT (Hourly), pct
        sensor_STO = r"STO"  # STO, SOIL TEMPERATURE OBSERVED, Fahrenheit
        duration = r"DAIY"
        state = r"UT"
        normType = r'NORMAL'
        # height variables at different depth*#
        heights_2 = awdb.service.getHeightDepths()  # soil moisture
        heights_2[:] = [z for z in heights_2 if z.unitCd == "in" and z.value == -2]  # soil moisture -2 inch
        heights_4 = awdb.service.getHeightDepths()  # soil moisture
        heights_4[:] = [z for z in heights_4 if z.unitCd == "in" and z.value == -4]  # soil moisture -4 inch
        heights_8 = awdb.service.getHeightDepths()  # soil moisture
        heights_8[:] = [z for z in heights_8 if z.unitCd == "in" and z.value == -8]  # soil moisture -8 inch
        heights_20 = awdb.service.getHeightDepths()  # soil moisture
        heights_20[:] = [z for z in heights_20 if z.unitCd == "in" and z.value == -20]  # soil moisture -20 inch
        heights_40 = awdb.service.getHeightDepths()  # soil moisture
        heights_40[:] = [z for z in heights_40 if z.unitCd == "in" and z.value == -40]  # soil moisture -40 inch
        # * get stations' info from SCAN *#
        stations = awdb.service.getStations("*", state, network, "*", "*",
                                            -1000, 1000, -1000, 1000, 0, 29000, "*",
                                            1, None, True)  # get stations' info from SCAN
        num_stations = len(stations)  # number of stations

        # * get metadata of the stations from SCAN *#
        meta = awdb.service.getStationMetadataMultiple(stations)  # get metadata of the stations from SCAN

        meta[:] = [x for x in meta if isActive(x)]  # eliminate the inactive stations
        num_stations_validate = len(meta)  # number of active stations
        validTrip = [x.stationTriplet for x in meta]  # get stationTriplet for the active stations

        # * a data array for storing retrieved data *#
        data_array_snortel = [[defaultValue] * 25 for row in
                              range(1, num_stations_validate + 2)]  # a data array for storing retrieved data
        if meta:
         i = 0  # iteration for valid stations
         # validSite1 = filter(lambda x: x.stationTriplet == "1113:UT:SNTL",
         #                     meta)  # code to check for particular station
         # print(validSite1[0],"validSite")
         # validSite = validSite1[0]
         for validSite in meta:
            data_array_snortel[i][0] = count + i  # serial number
            geo = awdb.service.getStationMetadata(
                validSite.stationTriplet)  # geo-information: elevation (ft), longtitude, altitude
            data_array_snortel[i][1] = geo.name  # station name
            data_array_snortel[i][3] = "SNORTEL"
            data_array_snortel[i][4] = 0.3048 * geo.elevation  # elevation, convert ft to meter
            data_array_snortel[i][5] = geo.latitude  # latitude
            data_array_snortel[i][6] = geo.longitude  # longitude
            wind_speed = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_WSPDV, 1, None,
                                                           starDate_Snortel_str, starDate_Snortel_str, 'ALL',
                                                           'ENGLISH')  # wind speed (mph) of the retrieved date
            data_array_snortel[i][7] = defaultValue if 'beginDate' not in wind_speed[0] or 'endDate' not in \
                                                                                           wind_speed[0] or \
                                                       wind_speed[0].values[0] == "" or 'value' not in \
                                                                                        wind_speed[0].values[0] \
                else 0.44704 * wind_speed[0].values[0].value  # wind_speed, convert mph to m/s
            air_temp = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_TOBS, 1, None,
                                                         starDate_Snortel_str, starDate_Snortel_str, 'ALL',
                                                         'ENGLISH')  # air temperature (Fahrenheit) of the retrieved date
            data_array_snortel[i][8] = defaultValue if 'beginDate' not in air_temp[0] or 'endDate' not in air_temp[
                0] or \
                                                       air_temp[0].values[0] == "" or 'value' not in \
                                                                                      air_temp[0].values[0] \
                else (air_temp[0].values[0].value - 32) * 5 / 9  # air temp, convert Fahrenheit to Celsius
            # data_array[i][36] = startDate_str;
            sm_2 = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_SMS, 1, heights_2,
                                                     starDate_Snortel_str,
                                                     starDate_Snortel_str, 'ALL',
                                                     'ENGLISH')  # soil moisture (pct) -2 inch
            data_array_snortel[i][15] = defaultValue if 'beginDate' not in sm_2[0] or 'endDate' not in sm_2[0] or \
                                                        sm_2[0].values[0] == "" or 'value' not in sm_2[0].values[0] \
                else sm_2[0].values[0].value / 100  # sms_-2, convert pct to decimal
            sm_4 = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_SMS, 1, heights_4,
                                                     starDate_Snortel_str,
                                                     starDate_Snortel_str, 'ALL',
                                                     'ENGLISH')  # soil moisture (pct) -4 inch
            data_array_snortel[i][16] = defaultValue if 'beginDate' not in sm_4[0] or 'endDate' not in sm_4[0] or \
                                                        sm_4[0].values[0] == "" or 'value' not in sm_4[0].values[0] \
                else sm_4[0].values[0].value / 100  # sms_-4, convert pct to decimal
            sm_8 = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_SMS, 1, heights_8,
                                                     starDate_Snortel_str, starDate_Snortel_str, 'ALL',
                                                     'ENGLISH')  # soil moisture (pct) -8 inch
            data_array_snortel[i][17] = defaultValue if 'beginDate' not in sm_8[0] or 'endDate' not in sm_8[0] or \
                                                        sm_8[0].values[0] == "" or 'value' not in sm_8[0].values[0] \
                else sm_8[0].values[0].value / 100  # sms_-8, convert pct to decimal
            sm_20 = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_SMS, 1, heights_20,
                                                      starDate_Snortel_str, starDate_Snortel_str, 'ALL',
                                                      'ENGLISH')  # soil moisture (pct) -20 inch
            data_array_snortel[i][18] = defaultValue if 'beginDate' not in sm_20[0] or 'endDate' not in sm_20[0] or \
                                                        sm_20[0].values[0] == "" or 'value' not in sm_20[0].values[
                0] \
                else sm_20[0].values[0].value / 100  # sms_-20, convert pct to decimal
            sm_40 = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_SMS, 1, heights_40,
                                                      starDate_Snortel_str, starDate_Snortel_str, 'ALL',
                                                      'ENGLISH')  # soil moisture (pct) -40 inch
            data_array_snortel[i][19] = defaultValue if 'beginDate' not in sm_40[0] or 'endDate' not in sm_40[0] or \
                                                        sm_40[0].values[0] == "" or 'value' not in sm_40[0].values[
                0] \
                else sm_40[0].values[0].value / 100  # sms_-40, convert pct to decimal
            st_2 = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_STO, 1, heights_2,
                                                     starDate_Snortel_str, starDate_Snortel_str, 'ALL',
                                                     'ENGLISH')  # soil temperature (Fahrenheit) -2 inch
            data_array_snortel[i][20] = defaultValue if 'beginDate' not in st_2[0] or 'endDate' not in st_2[0] or \
                                                        st_2[0].values[0] == "" or 'value' not in st_2[0].values[0] \
                else round((st_2[0].values[0].value - 32) * 5 / 9, 2)
            st_4 = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_STO, 1, heights_4,
                                                     starDate_Snortel_str, starDate_Snortel_str, 'ALL',
                                                     'ENGLISH')  # soil temperature (Fahrenheit) -4 inch
            data_array_snortel[i][21] = defaultValue if 'beginDate' not in st_4[0] or 'endDate' not in st_4[0] or \
                                                        st_4[0].values[0] == "" or 'value' not in st_4[0].values[0] \
                else round((st_4[0].values[0].value - 32) * 5 / 9, 2)  # sto_-4, convert Fahrenheit to Celsius
            st_8 = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_STO, 1, heights_8,
                                                     starDate_Snortel_str, starDate_Snortel_str, 'ALL',
                                                     'ENGLISH')  # soil temperature (Fahrenheit) -8 inch
            data_array_snortel[i][22] = defaultValue if 'beginDate' not in st_8[0] or 'endDate' not in st_8[0] or \
                                                        st_8[0].values[0] == "" or 'value' not in st_8[0].values[0] \
                else round((st_8[0].values[0].value - 32) * 5 / 9, 2)  # sto_-8, convert Fahrenheit to Celsius
            st_20 = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_STO, 1, heights_20,
                                                      starDate_Snortel_str, starDate_Snortel_str, 'ALL',
                                                      'ENGLISH')  # soil temperature (Fahrenheit) -20 inch
            data_array_snortel[i][23] = defaultValue if 'beginDate' not in st_20[0] or 'endDate' not in st_20[0] or \
                                                        st_20[0].values[0] == "" or 'value' not in st_20[0].values[
                0] \
                else round((st_20[0].values[0].value - 32) * 5 / 9, 2)  # sto_-20, convert Fahrenheit to Celsius
            st_40 = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_STO, 1, heights_40,
                                                      starDate_Snortel_str, starDate_Snortel_str, 'ALL',
                                                      'ENGLISH')  # soil temperature (Fahrenheit) -40 inch
            data_array_snortel[i][24] = defaultValue if 'beginDate' not in st_40[0] or 'endDate' not in st_40[0] or \
                                                        st_40[0].values[0] == "" or 'value' not in st_40[0].values[
                0] \
                else round((st_40[0].values[0].value - 32) * 5 / 9, 2)  # sto_-40, convert Fahrenheit to Celsius
            prec = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_PREC, 1, None,
                                                     starDate_Snortel_str, starDate_Snortel_str, 'ALL',
                                                     'ENGLISH')  # prec (inches) of the retrieved date
            data_array_snortel[i][2] = prec[0].stationId  # station ID
            data_array_snortel[i][9] = startDate
            for j in range(0, NumberOfDays):
                dayPrecipitate = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_PREC, 1, None,
                                                                   PrecipitationPeriod[j + 1].beginDate_snortel,
                                                                   PrecipitationPeriod[j + 1].beginDate_snortel,
                                                                   'ALL',
                                                                   'ENGLISH')  # prec (inches)

                data_array_snortel[i][10 + j] = defaultValue if 'beginDate' not in dayPrecipitate[0] or \
                                                                'endDate' not in dayPrecipitate[0] or \
                                                                'values' not in dayPrecipitate[0] or \
                                                                'values' not in prec[0] or \
                                                                prec[0].values[0].value < dayPrecipitate[0].values[
                                                                    0].value \
                    else 25.4 * (prec[0].values[0].value - dayPrecipitate[0].values[0].value)

            print (
                data_array_snortel[i][0], data_array_snortel[i][1], data_array_snortel[i][2],
                data_array_snortel[i][3], data_array_snortel[i][4], data_array_snortel[i][5],
                data_array_snortel[i][6], data_array_snortel[i][7], data_array_snortel[i][8],
                data_array_snortel[i][9], data_array_snortel[i][10], data_array_snortel[i][11],
                data_array_snortel[i][12],
                data_array_snortel[i][13], data_array_snortel[i][14], data_array_snortel[i][15],
                data_array_snortel[i][16], data_array_snortel[i][17],
                data_array_snortel[i][18],
                data_array_snortel[i][19], data_array_snortel[i][20], data_array_snortel[i][21],
                data_array_snortel[i][22], data_array_snortel[i][23], data_array_snortel[i][24])
            i += 1
        return data_array_snortel


    def GetScanData(count):
        wsdl = r"https://wcc.sc.egov.usda.gov/awdbWebService/services?WSDL"
        svc_url = r"https://wcc.sc.egov.usda.gov/awdbWebService/services"
        awdb = Client(wsdl)
        heights_2 = awdb.service.getHeightDepths()  # soil moisture
        # * retrieving info & parameters *#
        network = r"SCAN"  # SCAN network
        sensor_WSPDV = r"WSPDV"  # WSPDV, WIND SPEED AVERAGE (Hourly), mph
        sensor_TOBS = r"TOBS"  # TOBS, AIR TEMPERATURE OBSERVED, Fahrenheit
        sensor_PREC = r"PREC"  # PREC, PRECIPITATION ACCUMULATION, Inches
        sensor_SMS = r"SMS"  # SMS, SOIL MOISTURE PERCENT (Hourly), pct
        sensor_STO = r"STO"  # STO, SOIL TEMPERATURE OBSERVED, Fahrenheit
        duration = r"DAILY"
        state = r"UT"
        normType = r'NORMAL'
        heights_2[:] = [z for z in heights_2 if z.unitCd == "in" and z.value == -2]  # soil moisture -2 inch
        heights_4 = awdb.service.getHeightDepths()  # soil moisture
        heights_4[:] = [z for z in heights_4 if z.unitCd == "in" and z.value == -4]  # soil moisture -4 inch
        heights_8 = awdb.service.getHeightDepths()  # soil moisture
        heights_8[:] = [z for z in heights_8 if z.unitCd == "in" and z.value == -8]  # soil moisture -8 inch
        heights_20 = awdb.service.getHeightDepths()  # soil moisture
        heights_20[:] = [z for z in heights_20 if z.unitCd == "in" and z.value == -20]  # soil moisture -20 inch
        heights_40 = awdb.service.getHeightDepths()  # soil moisture
        heights_40[:] = [z for z in heights_40 if z.unitCd == "in" and z.value == -40]  # soil moisture -40 inch

        # * get stations' info from SCAN *#
        stations = awdb.service.getStations("*", state, network, "*", "*",
                                            -1000, 1000, -1000, 1000, 0, 29000, "*",
                                            1, None, True)  # get stations' info from SCAN
        num_stations = len(stations)  # number of stations

        # * get metadata of the stations from SCAN *#
        meta = awdb.service.getStationMetadataMultiple(stations)  # get metadata of the stations from SCAN

        meta[:] = [x for x in meta if isActive(x)]  # eliminate the inactive stations
        num_stations_validate = len(meta)  # number of active stations
        validTrip = [x.stationTriplet for x in meta]  # get stationTriplet for the active stations

        # * a data array for storing retrieved data *#
        data_array_scan = [[-999999] * 25 for row in
                           range(1, num_stations_validate + 1)]  # a data array for storing retrieved data
        if meta:
            i = 0  # iteration for valid stations
        for validSite in meta:
            # validSite1 = filter(lambda x: x.stationTriplet == "2151:UT:SCAN",
            #                     meta)  # code to check for particular station
            # print(validSite1[0],"validSite")
            # validSite = validSite1[0]
            data_array_scan[i][0] = count + i  # serial number
            geo = awdb.service.getStationMetadata(
                validSite.stationTriplet)  # geo-information: elevation (ft), longtitude, altitude
            data_array_scan[i][1] = geo.name  # station name
            data_array_scan[i][3] = "SCAN"
            data_array_scan[i][4] = 0.3048 * geo.elevation  # elevation, convert ft to meter
            data_array_scan[i][5] = geo.latitude  # latitude
            data_array_scan[i][6] = geo.longitude  # longitude
            wind_speed = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_WSPDV, 1, None,
                                                           starDate_Snortel_str, starDate_Snortel_str, 'ALL',
                                                           'ENGLISH')  # wind speed (mph) of the retrieved date
            data_array_scan[i][7] = -999999 if 'beginDate' not in wind_speed[0] or 'endDate' not in wind_speed[0] or \
                                               wind_speed[0].values[0] == "" or 'value' not in wind_speed[0].values[
                0] \
                else 0.44704 * wind_speed[0].values[0].value  # wind_speed, convert mph to m/s
            air_temp = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_TOBS, 1, None,
                                                         starDate_Snortel_str, starDate_Snortel_str, 'ALL',
                                                         'ENGLISH')  # air temperature (Fahrenheit) of the retrieved date
            data_array_scan[i][8] = -999999 if 'beginDate' not in air_temp[0] or 'endDate' not in air_temp[0] or \
                                               air_temp[0].values[0] == "" or 'value' not in air_temp[0].values[0] \
                else (air_temp[0].values[0].value - 32) * 5 / 9  # air temp, convert Fahrenheit to Celsius
            # data_array[i][36] = startDate_str;
            sm_2 = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_SMS, 1, heights_2,
                                                     starDate_Snortel_str,
                                                     starDate_Snortel_str, 'ALL',
                                                     'ENGLISH')  # soil moisture (pct) -2 inch
            data_array_scan[i][15] = -999999 if 'beginDate' not in sm_2[0] or 'endDate' not in sm_2[0] or \
                                                sm_2[0].values[
                                                    0] == "" or 'value' not in \
                                                                sm_2[
                                                                    0].values[
                                                                    0] \
                else sm_2[0].values[0].value / 100  # sms_-2, convert pct to decimal
            sm_4 = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_SMS, 1, heights_4,
                                                     starDate_Snortel_str,
                                                     starDate_Snortel_str, 'ALL',
                                                     'ENGLISH')  # soil moisture (pct) -4 inch
            data_array_scan[i][16] = -999999 if 'beginDate' not in sm_4[0] or 'endDate' not in sm_4[0] or \
                                                sm_4[0].values[
                                                    0] == "" or 'value' not in \
                                                                sm_4[
                                                                    0].values[
                                                                    0] \
                else sm_4[0].values[0].value / 100  # sms_-4, convert pct to decimal
            sm_8 = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_SMS, 1, heights_8,
                                                     starDate_Snortel_str, starDate_Snortel_str, 'ALL',
                                                     'ENGLISH')  # soil moisture (pct) -8 inch
            data_array_scan[i][17] = -999999 if 'beginDate' not in sm_8[0] or 'endDate' not in sm_8[0] or \
                                                sm_8[0].values[
                                                    0] == "" or 'value' not in \
                                                                sm_8[
                                                                    0].values[
                                                                    0] \
                else sm_8[0].values[0].value / 100  # sms_-8, convert pct to decimal
            sm_20 = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_SMS, 1, heights_20,
                                                      starDate_Snortel_str, starDate_Snortel_str, 'ALL',
                                                      'ENGLISH')  # soil moisture (pct) -20 inch
            data_array_scan[i][18] = -999999 if 'beginDate' not in sm_20[0] or 'endDate' not in sm_20[0] or \
                                                sm_20[0].values[
                                                    0] == "" or 'value' not in \
                                                                sm_20[
                                                                    0].values[
                                                                    0] \
                else sm_20[0].values[0].value / 100  # sms_-20, convert pct to decimal
            sm_40 = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_SMS, 1, heights_40,
                                                      starDate_Snortel_str, starDate_Snortel_str, 'ALL',
                                                      'ENGLISH')  # soil moisture (pct) -40 inch
            data_array_scan[i][19] = -999999 if 'beginDate' not in sm_40[0] or 'endDate' not in sm_40[0] or \
                                                sm_40[0].values[
                                                    0] == "" or 'value' not in \
                                                                sm_40[
                                                                    0].values[
                                                                    0] \
                else sm_40[0].values[0].value / 100  # sms_-40, convert pct to decimal
            st_2 = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_STO, 1, heights_2,
                                                     starDate_Snortel_str, starDate_Snortel_str, 'ALL',
                                                     'ENGLISH')  # soil temperature (Fahrenheit) -2 inch
            data_array_scan[i][20] = -999999 if 'beginDate' not in st_2[0] or 'endDate' not in st_2[0] or \
                                                st_2[0].values[
                                                    0] == "" or 'value' not in \
                                                                st_2[
                                                                    0].values[
                                                                    0] \
                else round((st_2[0].values[0].value - 32) * 5 / 9, 2)
            st_4 = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_STO, 1, heights_4,
                                                     starDate_Snortel_str, starDate_Snortel_str, 'ALL',
                                                     'ENGLISH')  # soil temperature (Fahrenheit) -4 inch
            data_array_scan[i][21] = -999999 if 'beginDate' not in st_4[0] or 'endDate' not in st_4[0] or \
                                                st_4[0].values[
                                                    0] == "" or 'value' not in \
                                                                st_4[
                                                                    0].values[
                                                                    0] \
                else round((st_4[0].values[0].value - 32) * 5 / 9, 2)  # sto_-4, convert Fahrenheit to Celsius
            st_8 = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_STO, 1, heights_8,
                                                     starDate_Snortel_str, starDate_Snortel_str, 'ALL',
                                                     'ENGLISH')  # soil temperature (Fahrenheit) -8 inch
            data_array_scan[i][22] = -999999 if 'beginDate' not in st_8[0] or 'endDate' not in st_8[0] or \
                                                st_8[0].values[
                                                    0] == "" or 'value' not in \
                                                                st_8[
                                                                    0].values[
                                                                    0] \
                else round((st_8[0].values[0].value - 32) * 5 / 9, 2)  # sto_-8, convert Fahrenheit to Celsius
            st_20 = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_STO, 1, heights_20,
                                                      starDate_Snortel_str, starDate_Snortel_str, 'ALL',
                                                      'ENGLISH')  # soil temperature (Fahrenheit) -20 inch
            data_array_scan[i][23] = -999999 if 'beginDate' not in st_20[0] or 'endDate' not in st_20[0] or \
                                                st_20[0].values[
                                                    0] == "" or 'value' not in \
                                                                st_20[
                                                                    0].values[
                                                                    0] \
                else round((st_20[0].values[0].value - 32) * 5 / 9, 2)  # sto_-20, convert Fahrenheit to Celsius
            st_40 = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_STO, 1, heights_40,
                                                      starDate_Snortel_str, starDate_Snortel_str, 'ALL',
                                                      'ENGLISH')  # soil temperature (Fahrenheit) -40 inch
            data_array_scan[i][24] = -999999 if 'beginDate' not in st_40[0] or 'endDate' not in st_40[0] or \
                                                st_40[0].values[
                                                    0] == "" or 'value' not in \
                                                                st_40[
                                                                    0].values[
                                                                    0] \
                else  round((st_40[0].values[0].value - 32) * 5 / 9, 2)  # sto_-40, convert Fahrenheit to Celsius
            prec = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_PREC, 1, None,
                                                     starDate_Snortel_str, starDate_Snortel_str, 'ALL',
                                                     'ENGLISH')  # prec (inches) of the retrieved date
            data_array_scan[i][2] = prec[0].stationId  # station ID
            data_array_scan[i][9] = startDate
            for j in range(0, NumberOfDays):
                dayPrecipitate = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_PREC, 1, None,
                                                                   PrecipitationPeriod[j + 1].beginDate_snortel,
                                                                   PrecipitationPeriod[j + 1].beginDate_snortel,
                                                                   'ALL',
                                                                   'ENGLISH')  # prec (inches) of 5 day before
                data_array_scan[i][10 + j] = None if 'beginDate' not in dayPrecipitate[0] or 'endDate' not in \
                                                                                             dayPrecipitate[
                                                                                                 0] or 'beginDate' not in \
                                                                                                       prec[
                                                                                                           0] or 'endDate' not in \
                                                                                                                 prec[
                                                                                                                     0] \
                                                     or prec[0].values[0].value < dayPrecipitate[0].values[0].value \
                    else 25.4 * (prec[0].values[0].value - dayPrecipitate[0].values[0].value)

            print (
                data_array_scan[i][0], data_array_scan[i][1], data_array_scan[i][2], data_array_scan[i][3],
                data_array_scan[i][4], data_array_scan[i][5],
                data_array_scan[i][6],
                data_array_scan[i][7], data_array_scan[i][8], data_array_scan[i][9], data_array_scan[i][10],
                data_array_scan[i][11],
                data_array_scan[i][12],
                data_array_scan[i][13], data_array_scan[i][14], data_array_scan[i][15], data_array_scan[i][16],
                data_array_scan[i][17],
                data_array_scan[i][18],
                data_array_scan[i][19], data_array_scan[i][20], data_array_scan[i][21], data_array_scan[i][22],
                data_array_scan[i][23])
            i += 1
        return data_array_scan


    def GetMesoWestData(count):
        your_api_token = '2805d3f32c3446bbb7aef75f2d95dcae'  # my Mesowest API token#
        r = Meso(your_api_token)
        # * retrieve stations that have SMS measurement in UT in Mesowest *#
        data_sms = r.timeseries(state='UT', start=starDate_Meso_str, end=starDate_Meso_str,
                                vars='soil_moisture')  # sms data (%) in UT (stid ='CLSPT': 3-depth sms;
        stations_sms = data_sms['STATION']  # list of stations which have sms data
        validSite_sms = [x for x in stations_sms if int(x['MNET_ID']) != 29 and int(x['MNET_ID']) != 198
                         and x['STID'] != 'CLSPT' and x['STID'] != 'EDGUT' and x['STID'] != 'FG007'
                         and x['STID'] != 'FG014' and x[
                             'STID'] != 'NLGUT']  # exclude Mesowest stations that overlapped SCAN& UTAH by net ID;
        # NLGUT: 1 depth, no metadata; CLSPT: 3 depth; EDGUT: 2 depth
        num_validSite = len(validSite_sms)  # number of stations

        # * a data array for storing retrieved data *#
        data_array_Meso = [[defaultValue] * 25 for row in range(1, num_validSite + 1)]

        i = 0  # iteration for valid stations
        for validSite in validSite_sms:

            # * geo, wind_speed, and air temp info *#
            station_serialNo = i + count  # serial number
            station_name = validSite['NAME']  # station name
            station_id = validSite['STID']  # station ID
            # station_netid = validSite['MNET_ID'] #net ID
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
                data_array_Meso[i][10 + j] = defaultValue if 'OBSERVATIONS' not in dayPrecipitate \
                                                             or not dayPrecipitate['OBSERVATIONS'] \
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
            # print(validSite['SENSOR_VARIABLES']['soil_moisture'],"validSite['SENSOR_VARIABLES']['soil_moisture']")
            num_sms_positions = len(validSite['SENSOR_VARIABLES']['soil_moisture'])
            sms_observe_time = validSite['OBSERVATIONS']['date_time'][0]
            # sms_positions = validSite['SENSOR_VARIABLES']['soil_moisture'] #no data
            sms_observations = validSite['OBSERVATIONS']
            # data_array[i][19] = sms_observe_time
            # data_array[i][20] = num_sms_positions

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
                    # data_array[i][21] = sms_position_1  #1st sms depth
                    data_array_Meso[i][16] = sms_observation_1 / 100  # sms at the 1st depth, convert pct to decimal

                    # * SMS at the retrieving date *#
            data_sto = r.timeseries(state='UT', stid=station_id, start=starDate_Meso_str, end=starDate_Meso_str,
                                    vars='soil_temp')  # soil_temp at the retrieving date
            data_sto = data_sto['STATION'][0]
            num_sto_positions = len(data_sto['SENSOR_VARIABLES']['soil_temp'])
            # sto_positions = data_sto['SENSOR_VARIABLES']['soil_temp'] #wrong data or lack of data
            sto_observations = data_sto['OBSERVATIONS']
            # data_array[i][23] = num_sto_positions

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
                if sto_observations['soil_temp_set_1'] == "" or not sto_observations[
                    'soil_temp_set_1']:  # all sto are empty
                    sto_observation_1 = None
                else:
                    sto_observation_1 = sto_observations['soil_temp_set_1'][0]
                    # data_array[i][24] = sto_position_1 #1st sms depth
                    data_array_Meso[i][21] = sto_observation_1  # sms at the 1st depth

            if num_sms_positions == 1 and num_sto_positions == 1:
                print (data_array_Meso[i][0], data_array_Meso[i][1], data_array_Meso[i][2], data_array_Meso[i][3],
                       data_array_Meso[i][4],
                       data_array_Meso[i][5], data_array_Meso[i][6], data_array_Meso[i][7], data_array_Meso[i][8],
                       data_array_Meso[i][9],
                       data_array_Meso[i][10],
                       data_array_Meso[i][11], data_array_Meso[i][12],
                       data_array_Meso[i][13], data_array_Meso[i][14], data_array_Meso[i][15], data_array_Meso[i][16],
                       data_array_Meso[i][17], data_array_Meso[i][18], data_array_Meso[i][19], data_array_Meso[i][20],
                       data_array_Meso[i][21],
                       data_array_Meso[i][22], data_array_Meso[i][23], data_array_Meso[i][23], '/n')
            i += 1
        return data_array_Meso


    start_time = dt.now()  # for counting program running time
    with open(directory+'/SoilMoistureData'+startDate_str+'.csv','wb') as f:  # write in csv file
        writer = csv.writer(f)
        writer.writerows(data_array)  # data summary
    end_time = dt.now()  # for counting program running time
    print ("--- %s time ---" % (end_time - start_time))  # output run time
    startDate = startDate + datetime.timedelta(hours=-24)











