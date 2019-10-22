# -*- coding: utf-8 -*
#Import suds for connecting to the web service.
from suds.client import Client
import csv
import datetime
import numpy as np
from multiprocessing.pool import ThreadPool as Pool
import multiprocessing as mp
import traceback
import urllib3
from bs4 import BeautifulSoup #helps to beautify the returned code (optional)
import json


dt=datetime.datetime
NumberOfDays = 5 # Number of days precipitation accumulation is needed
defaultValue=-999999 # says value is missing or abnormal
nrcs_thread_num = 8
proc_num = 8
header=[["Serial Number", "Station Name", "Station Id", "Network", "Elevation(meter)", "Latitude", "Longitude", "Wind Speed(m/s)", "Air Temperature(C)",
         "Start Date", "Precipitation for 1 day", "Precipitation for 2 days", "Precipitation for 3 days", "Precipitation for 4 days", "Precipitation for 5 days",
         "sm_2", "sm_4", "sm_8", "sm_20", "sm_40", "st_2", "st_4", "st_8", "st_20", "st_40"]]


def isActive(x): #check if a station is still active or not
    # * time setting *#
    dt = datetime.datetime
    today = dt.today()
    if dt.strptime(x.endDate, "%Y-%m-%d %H:%M:%S").date() > today.date():
        return True


def getStationDataFromNRCS(awdb, validSite, heights, data_array_nrcs, i, count, startDate, ntwk, PrecipitationPeriod, nrcs_fail):
    try:
        #print('Start Snortel Station: {}'.format(i+1))
        sensor_WSPDV = r"WSPDV"  # WSPDV, WIND SPEED AVERAGE (Hourly), mph
        sensor_TOBS = r"TOBS"  # TOBS, AIR TEMPERATURE OBSERVED, Fahrenheit
        sensor_PREC = r"PREC"  # PREC, PRECIPITATION ACCUMULATION, Inches
        sensor_SMS = r"SMS"  # SMS, SOIL MOISTURE PERCENT (Hourly), pct
        sensor_STO = r"STO"  # STO, SOIL TEMPERATURE OBSERVED, Fahrenheit
        startDate_str = str(startDate)

        data_array_nrcs[i][0] = count + i  # serial number
        geo = awdb.service.getStationMetadata(validSite.stationTriplet)  # geo-information: elevation (ft), longtitude, altitude
        data_array_nrcs[i][1] = geo.name  # station name
        data_array_nrcs[i][3] = ntwk
        data_array_nrcs[i][4] = 0.3048 * geo.elevation  # elevation, convert ft to meter
        data_array_nrcs[i][5] = geo.latitude  # latitude
        data_array_nrcs[i][6] = geo.longitude  # longitude

        # wind speed (mph) of the retrieved date
        wind_speed = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_WSPDV, 1, None,
                                                       startDate_str, startDate_str, 'ALL',
                                                       'ENGLISH')
        data_array_nrcs[i][7] = defaultValue if 'beginDate' not in wind_speed[0] or 'endDate' not in wind_speed[0] or \
                                                   wind_speed[0].values[0] == "" or 'value' not in wind_speed[0].values[0] \
                                             else 0.44704 * wind_speed[0].values[0].value  # wind_speed, convert mph to m/s

        # air temperature (Fahrenheit) of the retrieved date
        air_temp = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_TOBS, 1, None,
                                                     startDate_str, startDate_str, 'ALL',
                                                     'ENGLISH')
        data_array_nrcs[i][8] = defaultValue if 'beginDate' not in air_temp[0] or 'endDate' not in air_temp[0] or \
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
                                                   startDate_str,
                                                   startDate_str, 'ALL', 'ENGLISH')
            # soil temperature (Fahrenheit)
            st = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_STO, 1, height,
                                                   startDate_str, startDate_str, 'ALL',
                                                   'ENGLISH')
            data_array_nrcs[i][col_sm] = defaultValue if 'beginDate' not in sm[0] or 'endDate' not in sm[0] or \
                                                            sm[0].values[0] == "" or 'value' not in sm[0].values[0] \
                                                         else sm[0].values[0].value / 100  # sms, convert pct to decimal
            data_array_nrcs[i][col_st] = defaultValue if 'beginDate' not in st[0] or 'endDate' not in st[0] or \
                                                            st[0].values[0] == "" or 'value' not in st[0].values[0] \
                                                         else round((st[0].values[0].value - 32) * 5 / 9, 2)  # sto, convert Fahrenheit to Celsius
            col_sm = col_sm + 1
            col_st = col_st + 1

        # precipitation (inches) of the retrieved date
        prec = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_PREC, 1, None,
                                                 startDate_str, startDate_str, 'ALL',
                                                 'ENGLISH')
        #print(prec[0].values[0].value)
        data_array_nrcs[i][2] = prec[0].stationId  # station ID
        data_array_nrcs[i][9] = startDate
        for j in range(0, NumberOfDays):
            dayPrecipitate = awdb.service.getInstantaneousData(validSite.stationTriplet, sensor_PREC, 1, None,
                                                               PrecipitationPeriod[j + 1],
                                                               PrecipitationPeriod[j + 1], 'ALL',
                                                               'ENGLISH')  # prec (inches)
            #print(dayPrecipitate[0].values[0].value)
            data_array_nrcs[i][10 + j] = defaultValue if 'beginDate' not in dayPrecipitate[0] or \
                                                            'endDate' not in dayPrecipitate[0] or \
                                                            'values' not in dayPrecipitate[0] or \
                                                            'values' not in prec[0] or \
                                                            prec[0].values[0].value < dayPrecipitate[0].values[0].value \
                                                      else 25.4 * (prec[0].values[0].value - dayPrecipitate[0].values[0].value)
        #print('---Finish Snorel Station: {}'.format(i+1))
        '''print (data_array_nrcs[i][0], data_array_nrcs[i][1], data_array_nrcs[i][2], data_array_nrcs[i][3], data_array_nrcs[i][4], data_array_nrcs[i][5],
               data_array_nrcs[i][6], data_array_nrcs[i][7], data_array_nrcs[i][8], data_array_nrcs[i][9], data_array_nrcs[i][10], data_array_nrcs[i][11],
               data_array_nrcs[i][12], data_array_nrcs[i][13], data_array_nrcs[i][14], data_array_nrcs[i][15], data_array_nrcs[i][16], data_array_nrcs[i][17],
               data_array_nrcs[i][18], data_array_nrcs[i][19], data_array_nrcs[i][20], data_array_nrcs[i][21], data_array_nrcs[i][22], data_array_nrcs[i][23],data_array_nrcs[i][24])'''
    except Exception as e:
        print('{}: Network {} Station {} Fail.'.format(startDate, ntwk, i+1))
        nrcs_fail.append((i, validSite))
        #print(traceback.format_exc())

def getNRCSData(count, startDate, ntwk, PrecipitationPeriod):
    # * AWDB (Air-Water Database) web service *#
    wsdl = r"https://wcc.sc.egov.usda.gov/awdbWebService/services?WSDL"
    awdb = Client(wsdl)
    if ntwk == "SNTL":
        network = r"SNTL"        # SNTL network
    elif ntwk == "SCAN":
        network = r"SCAN"
    else:
        print('There is no such network.')
        return 0
    state = r"UT"            # STATE

    # get stations' info from SCAN
    stations = awdb.service.getStations("*", state, network, "*", "*", -1000, 1000, -1000, 1000, 0, 29000, "*", 1, None, True)
    #print('station: ',stations)
    # get height format
    heights = awdb.service.getHeightDepths()
    # get metadata of the stations from SCAN
    meta = awdb.service.getStationMetadataMultiple(stations)
    meta[:] = [x for x in meta if isActive(x)]  # eliminate the inactive stations
    num_stations_validate = len(meta)  # number of active stations

    # * a data array for storing retrieved data *#
    data_array_nrcs = [[defaultValue]*25 for row in range(num_stations_validate)]  # a data array for storing retrieved data
    nrcs_fail=[]
    # Parallel by stations
    pool = Pool(processes=nrcs_thread_num)
    for i, validSite in enumerate(meta):
        pool.apply_async(getStationDataFromNRCS, (awdb, validSite, heights, data_array_nrcs, i, count, startDate, ntwk, PrecipitationPeriod, nrcs_fail))
    pool.close()
    pool.join()

    #retry fail station
    error_try=len(nrcs_fail)
    while len(nrcs_fail) != 0:
        elem = nrcs_fail.pop(0)
        error_try = error_try - 1
        print('{}: Network {} Station {} Retry.'.format(startDate, ntwk, i + 1))
        getStationDataFromNRCS(awdb, elem[1], heights, data_array_nrcs, elem[0], count, startDate, ntwk)
        if error_try < 0:
            print('{}: Network {} Retry.'.format(startDate, ntwk))
            break

    return data_array_nrcs

def getStationDataFromUCC(ntwk, id, startDate, i, data_array_ucc, PrecipitationPeriod):
    endDate = startDate + datetime.timedelta(days=1)
    url = 'https://climate.usu.edu/API/api.php/v2/key=600bt7gSrX85in1ptsrDhcZpi7kiKF/station_search/network={}/station_id={}/get_daily/start_date={}/end_date={}/units=english'.format(ntwk,id,startDate,endDate)
    http = urllib3.PoolManager()
    response = http.request('GET', url)
    soup = BeautifulSoup(response.data.decode('utf-8'), "html.parser")
    station = json.loads(str(soup))
    #print(station)
    if station['payload'] != False:
        station = station['payload'][0]
    else:
        return 0


    if ntwk == 'UCRN':
        data_array_ucc[i][7] = 0.44704 * float(station['winds'])  # wind_speed, convert mph to m/s
        data_array_ucc[i][8] = (float(station['airt']) - 32) * 5 / 9  # air temp, convert Fahrenheit to Celsius
    else:
        data_array_ucc[i][7] = 0.44704 * float(station['winds_avg'])  # wind_speed, convert mph to m/s
        data_array_ucc[i][8] = (float(station['airt_avg']) - 32) * 5 / 9  # air temp, convert Fahrenheit to Celsius

    #mositure
    if ntwk == 'AGWX':
        data_array_ucc[i][17] = defaultValue if station['soilvwc8_avg'] is None \
                                             else float(station['soilvwc8_avg']) / 100 #sm_8
    elif ntwk == 'FGNET':
        data_array_ucc[i][18] = defaultValue if station['tdr1_avg'] is None \
                                             else float(station['tdr1_avg']) / 100 #sm_20

    #temperature
    if ntwk == 'AGWX':
        data_array_ucc[i][21] = defaultValue if station['soilt4_avg'] is None \
                                             else round((float(station['soilt4_avg']) - 32) * 5/9, 2) #convert Fahrenheit to Celsius
        data_array_ucc[i][22] = defaultValue if station['soilt8_avg'] is None \
                                             else round((float(station['soilt8_avg']) - 32) * 5 / 9, 2)
    elif ntwk == 'FGNET':
        data_array_ucc[i][23] = defaultValue if station['soilt1_avg'] is None \
                                             else round((float(station['soilt1_avg']) - 32) * 5 / 9, 2)

    # prec current day
    if ntwk == 'AGWX':
        data_array_ucc[i][10] = defaultValue if station['precip_tb'] is None \
                                             else float(station['precip_tb'])*25.4 #inch to mm
    elif ntwk == 'FGNET':
        data_array_ucc[i][10] = defaultValue if station['tbprecp_tot_d'] is None \
                                             else float(station['tbprecp_tot_d']) * 25.4 #inch to mm
    else:
        data_array_ucc[i][10] = defaultValue if station['precip'] is None \
                                             else float(station['precip'])*25.4 #inch to mm

    if data_array_ucc[i][10] == defaultValue:
        prec_sum = 0
    else:
        prec_sum = data_array_ucc[i][10]

    #prec
    for j in range(0, NumberOfDays-1):
        url = 'https://climate.usu.edu/API/api.php/v2/key=600bt7gSrX85in1ptsrDhcZpi7kiKF/station_search/network={}/station_id={}/get_daily/start_date={}/end_date={}/units=english'.format(
            ntwk, id, PrecipitationPeriod[j+1], PrecipitationPeriod[j])
        http = urllib3.PoolManager()
        response = http.request('GET', url)
        soup = BeautifulSoup(response.data.decode('utf-8'), "html.parser")
        station = json.loads(str(soup))
        if station['payload'] != False:
            station = station['payload'][0]
        else:
            return 0
        #print(station)
        if ntwk == 'AGWX':
            data_array_ucc[i][11+j] = defaultValue if station['precip_tb'] is None \
                                                   else float(station['precip_tb'])*25.4+prec_sum #inch to mm
        elif ntwk == 'FGNET':
            data_array_ucc[i][11+j] = defaultValue if station['tbprecp_tot_d'] is None \
                                                   else float(station['tbprecp_tot_d']) * 25.4+prec_sum #inch to mm
        else:
            data_array_ucc[i][11+j] = defaultValue if station['precip'] is None \
                                                   else float(station['precip'])*25.4+prec_sum #inch to mm

        if data_array_ucc[i][11+j] == defaultValue:
            prec_sum = 0
        else:
            prec_sum = data_array_ucc[i][11+j]


def getUCCData(count, startDate, ntwk, PrecipitationPeriod):
    stations=[]
    url = "https://climate.usu.edu/API/api.php/v2/key=600bt7gSrX85in1ptsrDhcZpi7kiKF/station_search/source=UCC/network={}".format(ntwk)
    http = urllib3.PoolManager()
    response = http.request('GET', url)
    soup = BeautifulSoup(response.data.decode('utf-8'), "html.parser")
    stationsInfo = json.loads(str(soup))

    #Extract station
    for elem in stationsInfo['payload']:
        if elem['state'] != 'UT' or elem['country'] != 'US':
            continue
        stationID = elem['station_id']
        stationName = elem['name']
        elevation = elem['elevation']
        latitude = elem['latitude']
        longitude = elem['longitude']
        stations.append((stationID, stationName, elevation, latitude, longitude))

    data_array_ucc = [[defaultValue] * 25 for row in range(len(stations))]

    i=0
    for id, name, elev, lat, long in stations:
        #print(id, name, elev)
        data_array_ucc[i][0] = count + i  # serial number
        data_array_ucc[i][1] = name  # station name
        data_array_ucc[i][2] = id
        data_array_ucc[i][3] = ntwk
        data_array_ucc[i][4] = 0.3048 * float(elev) # elevation, convert ft to meter
        data_array_ucc[i][5] = lat  # latitude
        data_array_ucc[i][6] = long  # longitude
        data_array_ucc[i][9] = startDate
        getStationDataFromUCC(ntwk, id, startDate, i, data_array_ucc, PrecipitationPeriod)
        i+=1

    return data_array_ucc

def run(start_date_time):
    SNTLArray = [[defaultValue]*25]
    SCANArray = [[defaultValue]*25]
    UAGRIMETArray = [[defaultValue]*25]
    UCRNArray = [[defaultValue]*25]
    AGWXArray = [[defaultValue]*25]
    USUwxArray = [[defaultValue]*25]
    FGNETArray = [[defaultValue]*25]
    SNTLArray_len = 0
    SCANArray_len = 0
    UAGRIMET_len = 0
    UCRN_len = 0
    AGWX_len = 0
    USUwx_len = 0
    FGNET_len = 0

    #remove time for file name
    start_date_str = start_date_time.strftime("%Y-%m-%d")

    #precipitation in five days
    PrecipitationPeriod = []
    for i in range(0, NumberOfDays + 1):
            tdelta = datetime.timedelta(days=i)
            precipitation_date=str(start_date_time - tdelta)
            PrecipitationPeriod.append(precipitation_date) # adding all the dates needed for

    # Data from Snotel Network
    #startTime_SNTL = dt.now()
    print('Retrieve data from SNTL on {}'.format(start_date_str))
    try:
        SNTLArray = getNRCSData(1, start_date_time, 'SNTL', PrecipitationPeriod)
        SNTLArray_len = len(SNTLArray)
    except:
        print('The SNTL Network has been crashed down: {}.'.format(start_date_str))
        print(traceback.format_exc())
    #endTime_SNTL = dt.now()
    #print ("SNTL Time on %s is: %s" %(start_date_str, (endTime_SNTL - startTime_SNTL)))

    #Data from Scan Network
    #startTime_SCAN = dt.now()
    print('Retrieve data from SCAN on {}'.format(start_date_str))
    try:
        SCANArray=getNRCSData(1+SNTLArray_len, start_date_time, 'SCAN', PrecipitationPeriod)
        SCANArray_len=len(SCANArray)
    except:
        print('The SCAN Network has been crashed down: {}.'.format(start_date_str))
        print(traceback.format_exc())
    #endTime_SCAN = dt.now()
    #print ("SCAN Time on %s is: %s" % (start_date_str, (endTime_SCAN - startTime_SCAN)))

    #Data from UAGRIMET
    print('Retrieve data from UAGRIMET on {}'.format(start_date_str))
    try:
        UAGRIMETArray = getUCCData(1+SNTLArray_len+SCANArray_len, start_date_time, 'UAGRIMET', PrecipitationPeriod)
        UAGRIMET_len = len(UAGRIMETArray)
    except:
        print('The UAGRIMET Network has been crashed down: {}.'.format(start_date_str))
        print(traceback.format_exc())

    # Data from UCRN
    print('Retrieve data from UCRN on {}'.format(start_date_str))
    try:
        UCRNArray = getUCCData(1 + SNTLArray_len + SCANArray_len + UAGRIMET_len, start_date_time, 'UCRN', PrecipitationPeriod)
        UCRN_len = len(UCRNArray)
    except:
        print('The UCRN Network has been crashed down: {}.'.format(start_date_str))
        print(traceback.format_exc())

    # Data from AGWX
    print('Retrieve data from AGWX on {}'.format(start_date_str))
    try:
        AGWXArray = getUCCData(1 + SNTLArray_len + SCANArray_len + UAGRIMET_len + UCRN_len, start_date_time, 'AGWX', PrecipitationPeriod)
        AGWX_len = len(AGWXArray)
    except:
        print('The AGWX Network has been crashed down: {}.'.format(start_date_str))
        print(traceback.format_exc())

    # Data from USUwx
    print('Retrieve data from USUwx on {}'.format(start_date_str))
    try:
        USUwxArray = getUCCData(1 + SNTLArray_len + SCANArray_len + UAGRIMET_len + UCRN_len+AGWX_len, start_date_time, 'USUwx', PrecipitationPeriod)
        USUwx_len = len(USUwxArray)
    except:
        print('The USUwx Network has been crashed down: {}.'.format(start_date_str))
        print(traceback.format_exc())

    # Data from fgnet
    print('Retrieve data from FGNET on {}'.format(start_date_str))
    try:
        FGNETArray = getUCCData(1 + SNTLArray_len + SCANArray_len + UAGRIMET_len + UCRN_len + AGWX_len + USUwx_len, start_date_time, 'FGNET', PrecipitationPeriod)
        FGNET_len = len(FGNETArray)
    except:
        print('The FGNET Network has been crashed down: {}.'.format(start_date_str))
        print(traceback.format_exc())

    #Combining data from all networks
    data_array = np.vstack((header, SNTLArray, SCANArray, UAGRIMETArray, UCRNArray, AGWXArray, USUwxArray, FGNETArray))

    #start_date_str=start_date_str.replace(":","-")
    with open('SoilMoisture_'+start_date_str+'.csv', 'w', newline='') as f: #should be wb if python2.7
        writer = csv.writer(f)
        writer.writerows(data_array)

if __name__ == '__main__':
    run_start = dt.now()
    #Specify date
    start_date_time = datetime.datetime(2017, 9, 12)
    end_date_time = datetime.datetime(2017, 9, 25)
    delta_date = datetime.timedelta(days = 1)

    date_time_list = []
    while start_date_time <= end_date_time:
        date_time_list.append(start_date_time)
        start_date_time+=delta_date

    pool = mp.Pool(processes=proc_num)
    pool.map(run, date_time_list)
    pool.close()
    pool.join()

    run_end=dt.now()
    print('Overall Time: %s' % (run_end-run_start))









