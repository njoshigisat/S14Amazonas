###############################################################################################################
###  CREATING OF 12-DAY MOSAICS of S1 DATA FOR S14AMAZONAS PROJECT ### ### ### ### ### ### ###
### Script written by Neha Joshi, dated 10.02.2021
### Script execute with command - python S14Amazonas_MOSAICS.py 21MUS,20MRC AB ### (where 21MUS,20MRC are tiles and "AB" is the site name)
###############################################################################################################

import os
import sys
from sys import platform

import gdal
from gdalconst import *
import csv
from datetime import datetime, timedelta
from datetime import date, timedelta
import shutil
import xml.etree.ElementTree as ET
import numpy as np
from numpy import genfromtxt
import math
import ogr
from osgeo import ogr, osr
import glob
from csv import reader
import requests
import shutil

def find_imagery_eodata_v2(satellite, geometry_wkt, startDate, endDate):
    base_url = 'https://finder.creodias.eu/resto/api/collections/'
    if satellite not in ['Landsat5', 'Landsat7', 'Landsat8', 'Sentinel2', 'Sentinel1']:
        raise ValueError('Satellite must be Landsat5, Landsat7, Landsat8 or Sentinel2')
    start_format = datetime.strftime(startDate, '%Y-%m-%d')
    end_format = datetime.strftime(endDate, '%Y-%m-%d')
    # url = '%s%s/search.json?sortOrder=ascending&geometry=%s&startDate=%s&completionDate=%s&productType=SLC&index=1&sortParam=startDate&status=0|34|37&maxRecords=2000' % (base_url, satellite, geometry_wkt, start_format, end_format)
    url = '%s%s/search.json?sortOrder=ascending&geometry=%s&startDate=%s&completionDate=%s&productType=GRD&sensorMode=IW&&index=1&sortParam=startDate&status=0|34|37&maxRecords=2000' % (base_url, satellite, geometry_wkt, start_format, end_format)
    # url = '%s%s/search.json?sortOrder=ascending&geometry=%s&startDate=%s&completionDate=%s&productType=GRD&sensorMode=IW&status=0|34|37&index=1&sortParam=startDate&maxRecords=2000' % (base_url, satellite, geometry_wkt, start_format, end_format)
    try:
        resp = requests.get(url)
        resp2 = resp.json()
        return resp2
    except:
        raise requests.HTTPError('error searching eocloud url: %s' % url)
    return resp2

def get_orbit_list(resp):
    beginposit = None
    orbitdirc = None
    relativeor = None
    src_geometry = None
    src_filename = None
    orbList = list()
    for resp_features in resp['features']:
        title = resp_features['properties']['title'].replace('.SAFE', '')
        id = title.split('_')[0]
        beginposit = str(resp_features['properties']['startDate']).split('T')[0]
        orbitdirc = resp_features['properties']['orbitDirection']
        orbitNumber = resp_features['properties']['orbitNumber']
        if id == 'S1A':
            #    Sentinel-1A Relative Orbit Number = mod (Absolute Orbit Number orbit - 73, 175) + 1
            relativeor = ((orbitNumber - 73) % 175) + 1
            orbList.append(relativeor)
        elif id == 'S1B':
            #    Sentinel-1B Relative Orbit Number = mod (Absolute Orbit Number orbit - 27, 175) + 1
            relativeor = ((orbitNumber - 27) % 175) + 1
            orbList.append(relativeor)
        orbList = list(set(orbList))
    # return (beginposit, orbitdirc, relativeor, src_geometry, src_filename)
    return (orbList)

### GDAL and SNAP settings ###
memsize = 2048
co = ['COMPRESS=DEFLATE', 'TILED=YES']

### DECLARE STARTING FOLDERS and FILES ###
SRC_folder = '/mnt/s14amazonas-data-05/data/SLC/'
pilot_site = sys.argv[2]
Tile_list = [str(i) for i in str(sys.argv[1]).split(',')]
MOS_folder = '/mnt/s14amazonas-data-06/data/S1_Processed/'+ pilot_site +'/Mosaics/'
if not os.path.exists(MOS_folder):
    os.mkdir(MOS_folder)

### CREATE A GRID-LIST OF DATES EVERY 6/12 DAYS (for 12-day acquisition frequency) ###
acq_frequency = 12
start_date = datetime(2015,05,01)
end_date = datetime(2021,12,31) # datetime.today().date()
days_interval = np.ndarray.tolist(np.arange(start_date,end_date,timedelta(days=acq_frequency),dtype = 'datetime64[D]'))
year = start_date.year

### DECLARE IF MOSAICS OF BACKSCATTER, COHERENCE, OR BOTH, ARE TO BE PREPARED, and IF INDIVIDUAL ORBITS ARE NEEDED ###
# TYPE = ['_COH_','_BAC_']
TYPE = ['_BAC_']
Individual_orbits_COH_ = 'FALSE'
Individual_orbits_BAC_ = 'FALSE'
ORBIT_DIRECTIONS = ['descending']

#################################### THE START ##################################################
#################################### THE START ##################################################
#################################### THE START ##################################################

for Tile in Tile_list:
    PATHS_LIST = '/home/njoshi/projects/s14amazonas/scripts/S2_grid_AmazonBasin_paths.csv'
    with open(PATHS_LIST, 'r') as read_paths:
        txt_reader = reader(read_paths)
        print txt_reader
        for each_Ama_tile in txt_reader:
            Ama_tile = str(each_Ama_tile[0])
            Ama_path = str(each_Ama_tile[1])
            if Ama_tile == str(Tile):
                print Tile + '... has paths...' + Ama_path
                if Ama_path == 'AD':
                    ORBIT_DIRECTIONS = ['descending', 'ascending']
    WKT_Tiles_Amazon = '/home/njoshi/projects/s14amazonas/scripts/S2_grid_AmazonBasic.csv'
    with open(WKT_Tiles_Amazon, 'r') as read_obj:
        csv_reader = reader(read_obj)
        for each_Ama_tile in csv_reader:
            Ama_tile = str(each_Ama_tile).split(")))',")[1].split("'")[1]
            if Ama_tile == Tile:
                print "Found WKT for Tile ... " + Tile
                print ','.join(each_Ama_tile)
                f = open('/home/njoshi/projects/s14amazonas/scripts/S2_grid_MG.wkt/S2_tiles_' + Tile + '.csv', 'w')
                f.write(','.join(each_Ama_tile))
                f.close()
    WKT_Tiles_studyarea = '/home/njoshi/projects/s14amazonas/scripts/S2_grid_MG.wkt/S2_tiles_' + Tile + '.csv'
    temp_folder = '/home/njoshi/projects/s14amazonas/data/Temp/'

    ### FIND WHERE PRE-PROCESSED BACKSCATTER AND COHERENCE IS STORED FOR THE TILE OF INTEREST ##########
    # The variable "SRC_folder_B_start" can be hardcoded if all backscatter outputs are stored in one folder!!!
    # The variable "SRC_folder_B_start" can be hardcoded if all backscatter outputs are stored in one folder!!!
    # The variable "SRC_folder_B_start" can be hardcoded if all backscatter outputs are stored in one folder!!!
    if '_BAC_' in TYPE:
        for root, dirs, files in os.walk(SRC_folder):
            if not root == MOS_folder:
                if str(root).__contains__(Tile) and str(root).__contains__('backscatter'):
                    SRC_folder_B_start = str(root).split(Tile)[0]
                    print "_BAC_ found in ... " + SRC_folder_B_start
                    break
    if '_COH_' in TYPE:
        for root, dirs, files in os.walk(SRC_folder):
            if not root == MOS_folder:
                if str(root).__contains__(Tile) and str(root).__contains__('coherence'):
                    SRC_folder_C_start = str(root).split(Tile)[0]
                    print "_COH_ found in ... " + SRC_folder_C_start
                    break

    for ORBIT_DIRECTION in ORBIT_DIRECTIONS:
        Image_list = []
        with open(WKT_Tiles_studyarea) as WKT_Tiles:
            WKT_lines = reader(WKT_Tiles)
            for row in WKT_lines:
                geometry_wkt = str(row).split(")))',")[0].replace("'",'').replace('[','') +')))'
                ulx = round(float(geometry_wkt.split('(')[3].split(' ')[0]),7)
                uly = round(float(geometry_wkt.split('(')[3].split(',')[0].split(' ')[1]),7)
                lrx = round(float(geometry_wkt.split('(')[3].split(',')[1].split(' ')[1]),7)
                lry = round(float(geometry_wkt.split('(')[3].split(',')[2].split(' ')[2]),7)
                tile_name = str(row).split(")))',")[1].split("'")[1]
                print "CURRENTLY PROCESSING ..." + ORBIT_DIRECTION + "..for TILE..." + tile_name
                if '_BAC_' in TYPE:
                    SRC_folder_B = SRC_folder_B_start + tile_name + '/'
                if '_COH_' in TYPE:
                    SRC_folder_C = SRC_folder_C_start + tile_name + '/'
                if not os.path.exists(MOS_folder + tile_name + '/'):
                    os.mkdir(MOS_folder + tile_name + '/')
                ### CHECK IF ANY FILES EXIST ALREADY IN THE MOS_FOLDER AND RECORD THE BIGGEST FILE SIZE DIMENSIONS
                for rts, ds, ffff in os.walk(MOS_folder + tile_name + '/' + ORBIT_DIRECTIONS[0] + '/'):
                    if ffff:
                        so_far = 0
                        Base_file = ""
                        for existing_file in ffff:
                            size = os.path.getsize(os.path.join(rts,existing_file))
                            if size > so_far:
                                so_far = size
                                Base_file = existing_file
                        raster = gdal.Open(os.path.join(rts,Base_file))
                        width = raster.RasterXSize
                        height = raster.RasterYSize
                        print "Width and height found ..." + str(width) + '...and...' + str(height)
                Text_file = open(MOS_folder + tile_name + '/' + tile_name + '.txt', 'w+')
                resp = find_imagery_eodata_v2('Sentinel1', geometry_wkt, datetime.date(start_date), datetime.date(end_date))
                orbList = get_orbit_list(resp)
                Text_file.write('FILE_NAME,'+ ','.join(('Orbit' + str(m)) for m in orbList))
                Text_file.write('\n')
                Image_list = []
                for resp_features in resp['features']:
                    title_p = str((resp_features['properties']['title'].replace('.SAFE', '')))
                    orbitdirc = str(resp_features['properties']['orbitDirection'])
                    if orbitdirc == ORBIT_DIRECTION:
                        Image_list.append(title_p)
                        if not os.path.exists(MOS_folder + tile_name + '/' + ORBIT_DIRECTION + '/'):
                            os.mkdir(MOS_folder + tile_name + '/' + ORBIT_DIRECTION + '/')
                    # print "THis is the image list for ..." + ORBIT_DIRECTION + "...." + tile_name
                size_count = -1
                for date in days_interval:
                    temp_folder = '/home/njoshi/projects/s14amazonas/data/Temp_' + tile_name + '_' + str(date) + '/'
                    if not os.path.exists(temp_folder):
                        os.mkdir(temp_folder)
                    week_date = datetime.strptime(str(date), '%Y-%m-%d')
                    if '_BAC_' in TYPE:
                        OUT_BAC_VH = MOS_folder + tile_name + '/' + ORBIT_DIRECTION + '/' + tile_name + '_BAC_VH_' + str(week_date).split(' ')[0].replace('-', '') + '.tif'
                        OUT_BAC_VV = MOS_folder + tile_name + '/' + ORBIT_DIRECTION + '/' + tile_name + '_BAC_VV_' + str(week_date).split(' ')[0].replace('-', '') + '.tif'
                    if '_COH_' in TYPE:
                        OUT_COH_VH = MOS_folder + tile_name + '/' + ORBIT_DIRECTION + '/' + tile_name + '_COH_VH_' + str(week_date).split(' ')[0].replace('-', '') + '.tif'
                        OUT_COH_VV = MOS_folder + tile_name + '/' + ORBIT_DIRECTION + '/' + tile_name + '_COH_VV_' + str(week_date).split(' ')[0].replace('-', '') + '.tif'

                    for pols in ['VV', 'VH']:
                        for type in TYPE:
                            globals()[tile_name + '_List' + type + pols + '_' + str(date).replace('-', '')] = list()
                    print "CURRENTLY PROCESSING " + tile_name + ', for date '+ str(date)
                    if '_BAC_' in TYPE:
                        for root, dirs, files in os.walk(SRC_folder_B):
                            for file in files:
                                if '_BAC_' in TYPE:
                                    if not os.path.exists(OUT_BAC_VH) or not os.path.exists(OUT_BAC_VV):
                                        if file.__contains__('backscatter') and file.endswith('.tif'):
                                            UNID = file.split('_')[1]
                                            if any(UNID in chars for chars in Image_list):
                                                acq_date = datetime.strptime(((file.split('PC_')[1]).split('T')[0]), '%Y%m%d')
                                                pol = file.split('_')[2]
                                                if (acq_date >= week_date) and (acq_date < week_date + timedelta(days=acq_frequency)):
                                                    temp_file = temp_folder + tile_name + '_' + file.split('.tif')[0] +'.tif'
                                                    if not os.path.exists(temp_file) and not os.path.exists(MOS_folder + tile_name + '/' + ORBIT_DIRECTION + '/' + tile_name + '_BAC_' + pol + '_' + str(week_date).split(' ')[0].replace('-', '') + '.tif'):
                                                        cmd_gdal = 'gdal_translate -eco -projwin %s %s %s %s %s %s' %(ulx,uly,lrx,lry,(os.path.join(root, file)),temp_file)
                                                        os.system(cmd_gdal)
                                                    (globals()[tile_name + '_List_BAC_' + pol + '_' + str(date).replace('-', '')]).append(temp_file)
                                                    print globals()[tile_name + '_List_BAC_' + pol + '_' + str(date).replace('-', '')]
                    if '_COH_' in TYPE:
                        for root, dirs, files in os.walk(SRC_folder_C):
                            for file in files:
                                if '_COH_' in TYPE:
                                    if not os.path.exists(OUT_COH_VH) or not os.path.exists(OUT_COH_VV):
                                        if file.__contains__('coherence') and file.endswith('.tif'):
                                            UNID = file.split ('_')[7]
                                            if any(UNID in chars for chars in Image_list):
                                                acq_date = datetime.strptime(((file.split('PC_')[1]).split('_')[0]), '%Y%m%d')
                                                pol = file.split('_')[4]
                                                if (acq_date >= week_date) and (acq_date < week_date + timedelta(days=acq_frequency)):
                                                    temp_file = temp_folder + tile_name + '_' + file.split('.tif')[0] +'.tif'
                                                    if not os.path.exists(temp_file) and not os.path.exists(MOS_folder + tile_name + '/' + ORBIT_DIRECTION + '/' + tile_name + '_COH_' + pol + '_' + str(week_date).split(' ')[0].replace('-', '') + '.tif'):
                                                        cmd_gdal = 'gdal_translate -eco -projwin %s %s %s %s %s %s' %(ulx,uly,lrx,lry,(os.path.join(root, file)),temp_file)
                                                        os.system(cmd_gdal)
                                                    (globals()[tile_name + '_List_COH_' + pol + '_' + str(date).replace('-', '')]).append(temp_file)
                    for pols in ['VV', 'VH']:
                        for type in TYPE:
                            if not os.path.exists(globals()[('OUT' + type + pols)]):
                                if len(globals()[tile_name + '_List' + type + pols + '_' + str(date).replace('-', '')]) > 0:
                                    SAGA_list_orbs = list()
                                    TARGET_OUT_GRID = list()
                                    count = -1
                                    if globals()['Individual_orbits' + type] == 'TRUE':
                                        print "INDIVIDUAL ORBITS ARE TO BE MOSAICKED for " + type + tile_name + ', for date ' + str(date)
                                        for orbit in orbList:
                                            SAGA_list_orbs.append(';'.join([s for s in globals()[tile_name + '_List' + type + pols + '_' + str(date).replace('-', '')] if ('_' + str(orbit) + '_') in s]))
                                            TARGET_OUT_GRID.append(MOS_folder + tile_name + '/' + ORBIT_DIRECTION + '/' + tile_name + type + pols + '_' + str(week_date).split(' ')[0].replace('-', '') + '_' + str(orbit))
                                    else:
                                        print "ALL ORBITS ARE TO BE MERGED MOSAICKED for " + type + tile_name + ', for date ' + str(date)
                                        SAGA_list_orbs = [';'.join(globals()[tile_name + '_List' + type + pols + '_' + str(date).replace('-', '')])]
                                        TARGET_OUT_GRID = [MOS_folder + tile_name + '/' + ORBIT_DIRECTION + '/' + tile_name + type + pols + '_' + str(week_date).split(' ')[0].replace('-', '')]
                                    for SAGA_list in SAGA_list_orbs:
                                        count += 1
                                        size_count +=1
                                        if (len(SAGA_list.split(';')) > 0) and not os.path.exists(TARGET_OUT_GRID[count] + '.tif'):
                                            cmd_saga = 'saga_cmd grid_tools 3 -GRIDS="' + SAGA_list + '" -NAME=Mosaic -TYPE=9 -RESAMPLING=0 -OVERLAP=5 -MATCH=0 -TARGET_DEFINITION=0 -TARGET_USER_SIZE=0.00017966 -TARGET_USER_FITS=0 -TARGET_OUT_GRID=' + TARGET_OUT_GRID[count]
                                            os.system(cmd_saga)
                                            if size_count == 0 and pols == 'VV' and not 'width' in globals() and not 'height' in globals():
                                                cmd_gdal = 'gdal_translate -co %s -co BIGTIFF=IF_NEEDED %s %s' % (' -co '.join(co), TARGET_OUT_GRID[count] + '.sdat', TARGET_OUT_GRID[count] + '.tif')
                                                os.system(cmd_gdal)
                                                raster = gdal.Open(TARGET_OUT_GRID[count] + '.tif')
                                                width = raster.RasterXSize
                                                height = raster.RasterYSize
                                                print "COUNT IS 0, width and height found ..." + str(width) + '...and...' + str(height)
                                            else:
                                                if not 'width' in globals() and not 'height' in globals():
                                                    cmd_gdal = 'gdal_translate -co %s -co BIGTIFF=IF_NEEDED %s %s' % (' -co '.join(co), TARGET_OUT_GRID[count] + '.sdat',TARGET_OUT_GRID[count] + '.tif')
                                                    os.system(cmd_gdal)
                                                    raster = gdal.Open(TARGET_OUT_GRID[count] + '.tif')
                                                    width = raster.RasterXSize
                                                    height = raster.RasterYSize
                                                    print "COUNT IS  " + str(size_count) + ", width and height has now been found ..."
                                                else:
                                                    cmd_gdal = 'gdal_translate -co %s -co BIGTIFF=IF_NEEDED -outsize %s %s %s %s' % (' -co '.join(co), str(width), str(height), TARGET_OUT_GRID[count] + '.sdat', TARGET_OUT_GRID[count] + '.tif')
                                                    os.system(cmd_gdal)
                                            cmd_delete = 'find ' + MOS_folder + tile_name + '/' + ORBIT_DIRECTION + '/ -type f ! -iname "*.tif"' + " -delete"
                                            os.system(cmd_delete)
                                            Text_string = [os.path.basename(TARGET_OUT_GRID[count])]
                                            for orbit in orbList:
                                                N_used_orbits = len([s for s in SAGA_list.split(';') if ('_' + str(orbit) + '_') in s])
                                                Text_string.append(N_used_orbits)
                                            Text_file.write(','.join(str(x) for x in Text_string))
                                            Text_file.write('\n')
                    shutil.rmtree(temp_folder)
                Text_file.close()

#################################### THE END ##################################################
#################################### THE END ##################################################
#################################### THE END ##################################################