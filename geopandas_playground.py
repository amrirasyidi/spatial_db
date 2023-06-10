import logging
import multiprocessing
import os
import glob
import time
from datetime import datetime, timedelta
import pandas as pd
import polars as pl
import geopandas as gpd
import fiona

logging.captureWarnings(True)
num_cores = multiprocessing.cpu_count()
os.environ["POLARS_NUM_THREADS"] = str(num_cores//2)

print(datetime.now())
program_start_time = time.process_time()
BASE_PATH = os.getcwd()

DATA_BASE_PATH = os.path.join(BASE_PATH, 'data')
DATA_TABLE_PATH = os.path.join(DATA_BASE_PATH, 'table')
DATA_SHP_PATH = os.path.join(DATA_BASE_PATH, 'shapefile')

RESULT_BASE_PATH = os.path.join(BASE_PATH, 'result')
RESULT_TABLE_PATH = os.path.join(RESULT_BASE_PATH, 'table')
RESULT_SHP_PATH = os.path.join(RESULT_BASE_PATH, 'shapefile')

# PREDEFINED VARIABLES
PROCESSING_GRANULARITY = 7
ORIGINAL_CRS = 4326
PROJECTED_CRS = 3857  # WGS84 Web Mercator (Auxiliary Sphere)
BUFFER_RADIUS = 10  # in meter

# People movement
START_DATE = datetime(2023, 1, 24, 7, 0, 0)  # year, month, date, GMT+7
END_DATE = datetime(2023, 1, 30, 7, 0, 0)
PM_ID_COL_NAME = "Hashed_Device_ID"
PM_LON_COL_NAME = "Lon_of_Observation_Point"
PM_LAT_COL_NAME = "Lat_of_Observation_Point"
PM_DATETIME_COL_NAME = "Unix_Timestamp_of_Observation_Point"
PM_COLS_SELECT = [
    PM_ID_COL_NAME,
    PM_LON_COL_NAME,
    PM_LAT_COL_NAME,
    PM_DATETIME_COL_NAME
    ]

# POI
POI_ID_COL_NAME = "PlaceID"
POI_COLS_SELECT = [
    POI_ID_COL_NAME,
    'Official_Name',
    'latitude',
    'longitude',
    'geometry'
    ]

# Preparing functions


def long_lat_rename(
        data: pd.DataFrame
) -> pd.DataFrame:
    """
    Rename the longitude and latitude columns flexibly
    """
    data = data.rename(
        columns=lambda x: 'latitude' if 'lat' in x.lower()
        else 'longitude' if 'lon' in x.lower() else x
        )
    return data


def projected_buffer(
        gdf: gpd.GeoDataFrame,
        buffer_radius: float | int
) -> gpd.GeoDataFrame:
    """
    Buffer the geometries using unit in meters
    """
    if gdf.crs.is_projected:
        gdf['geometry'] = gdf.buffer(buffer_radius)
    else:
        # need to convert to projected crs first
        # to make the buffer unit in meter
        gdf_proj = gdf.to_crs(PROJECTED_CRS)
        gdf_proj['geometry'] = gdf_proj.buffer(buffer_radius)
        gdf = gdf_proj.to_crs(ORIGINAL_CRS)
    return gdf


# Creating the necesary folders
data_relative_path_list = [DATA_BASE_PATH, DATA_TABLE_PATH, DATA_SHP_PATH]
result_relative_path_list = [RESULT_BASE_PATH, RESULT_TABLE_PATH,
                             RESULT_SHP_PATH]

print("Creating the necessary folders...")
start_time = time.process_time()
for _ in [*data_relative_path_list, *result_relative_path_list]:
    if not os.path.exists(_):
        os.mkdir(_)
    else:
        print("Directory already exist, skipping the process!")
print(
    "Folders created! Time taken:{:.2f} second"
    .format(time.process_time()-start_time)
    )

# Importing data
print("Importing shapefile...")
start_time = time.process_time()
KC_PATH = os.path.join(DATA_SHP_PATH, "KantorCabang.shp")
kc_gdf = gpd.read_file(KC_PATH, engine='pyogrio')
print("Time taken:{:.2f} second".format(time.process_time() - start_time))

# buffer 5km for kantor cabang
kc_gdf_buffer = projected_buffer(kc_gdf, 5000)
# find the total_bounds as input for create_dummy
minx, miny, maxx, maxy = kc_gdf_buffer.total_bounds

# Preparing poi and pm data path
poi_par = glob.glob(os.path.join(DATA_TABLE_PATH, '*_poi_raw.parquet'))
pm_par = glob.glob(os.path.join(DATA_TABLE_PATH, '*_pm_raw.parquet'))

# create a dictionary with kantor cabang name as its key,
# and its parquet file as the value
pm_poi_match = {
    poi.split('\\')[-1].split('_')[0]: [poi, pm]
    for poi, pm in zip(poi_par, pm_par)
    if (poi.split('\\')[-1].split('_')[0] in poi)
    & (poi.split('\\')[-1].split('_')[0] in pm)
    }

# count footfall from POI in each kantor cabang
for kc in pm_poi_match:
    current_date = START_DATE
    # POI
    print("Processing POI\n")
    if POI_DATA := glob.glob(os.path.join(DATA_TABLE_PATH, '*_poi_raw.parquet')):
        print("Reading parquet files...")
        start_time = time.process_time()
        poi_df = pl.read_parquet(pm_poi_match[kc][0]).to_pandas()
        poi_df = long_lat_rename(poi_df)[POI_COLS_SELECT]
        print("Converting into geodataframe...")
        convert_start_time = time.process_time()
        poi_gdf = gpd.GeoDataFrame(
            poi_df,
            geometry=gpd.points_from_xy(
                poi_df['longitude'],
                poi_df['latitude']
            ),
            crs=ORIGINAL_CRS
        )
    else:
        print("Reading from geodataframe")
        start_time = time.process_time()
        DATA_GDB = os.path.join(DATA_SHP_PATH, "3_market_prioritize_data.gdb")
        POI_DATA = fiona.listlayers(DATA_GDB)
        poi_gdf = gpd.read_file(
            DATA_GDB,
            driver='FileGDB',
            layer=f"{kc}_poi_raw"
            )[POI_COLS_SELECT]
        print("Converting into parquet..")
        convert_start_time = time.process_time()
        poi_gdf.to_parquet(os.path.join(
            DATA_TABLE_PATH, f"{kc}_poi_raw.parquet"))
    print(
        "Convert complete! Time taken: {:.2f} second\n"
        .format(time.process_time()-convert_start_time)
        )

    # 10m buffer
    poi_gdf_buffered = projected_buffer(poi_gdf, 10)

    # processing daily
    while current_date <= END_DATE:
        parquet_footfall_result_path = os.path.join(
            RESULT_TABLE_PATH,
            f'{kc}_{str(current_date.date()).replace("-","")}_{PROCESSING_GRANULARITY}_footfall.parquet'
            )
        shp_footfall_result_path = os.path.join(
            RESULT_SHP_PATH,
            f'{kc}_{str(current_date.date()).replace("-","")}_footfall.shp'
            )
        if not (os.path.exists(parquet_footfall_result_path)
                & os.path.exists(shp_footfall_result_path)):
            # PEOPLE MOVEMENT
            print(f"Processing People Movement, date: {current_date.date()}\n")
            print("Reading from geodataframe...")
            start_time = time.process_time()
            pm_df = (
                    pl.scan_parquet(pm_poi_match[kc][1])
                    .select(PM_COLS_SELECT)
                    .filter(
                        (pl.col(PM_DATETIME_COL_NAME) >= int(current_date.timestamp()))
                        & (pl.col(PM_DATETIME_COL_NAME) <= int((current_date + timedelta(days=PROCESSING_GRANULARITY)).timestamp()))
                    )
                ).collect()
            # # convert unix to datetime
            pm_df = pm_df.with_columns(
                    pl.from_epoch(PM_DATETIME_COL_NAME, time_unit="s")
                    .alias('datetime')
                ).with_columns(
                    pl.col("datetime").cast(pl.Date).alias('date')
                )
            
            # # convert to geopandas
            pm_df = long_lat_rename(pm_df.to_pandas())
            pm_gdf = gpd.GeoDataFrame(
                    pm_df,
                    geometry=gpd.points_from_xy(
                        pm_df.longitude,
                        pm_df.latitude
                    )
                )
            print(
                "Read complete! Time taken: {:.2f} second\n"
                .format(time.process_time()-start_time)
                )

            # SPATIAL JOIN
            print("Spatial join...")
            start_time = time.process_time()

            spatial_joined_poi = gpd.sjoin(
                poi_gdf_buffered,
                pm_gdf,
                how='left', predicate='contains',
                lsuffix="poi",
                rsuffix="pm"
                ).drop(columns=["index_pm", PM_DATETIME_COL_NAME])

            print(
                "Spatial join complete! Time taken: {:.2f} second\n"
                .format(time.process_time()-start_time)
                )

            print("Counting footfall...")
            start_time = time.process_time()

            spatial_joined_poi['num_daily_visit'] = (
                spatial_joined_poi
                .groupby([POI_ID_COL_NAME])[PM_ID_COL_NAME]
                .transform('nunique')
                )

            print(
                "Counting footfall complete! Time taken: {:.2f} second\n"
                .format(time.process_time()-start_time)
                )

            print("Removing duplciates...")
            start_time = time.process_time()
            col_to_drop = [PM_ID_COL_NAME, 'longitude_pm', 'latitude_pm',
                           'datetime']
            spatial_joined_poi = spatial_joined_poi.drop(columns=col_to_drop)

            spatial_joined_poi = (
                pl.from_pandas(spatial_joined_poi.drop(columns='geometry'))
                .unique()
                )

            print(
                "Removing duplciates complete! Time taken: {:.2f} second\n"
                .format(time.process_time()-start_time)
                )

            # EXPORT
            print("Converting into parquet..")
            start_time = time.process_time()
            spatial_joined_poi.write_parquet(
                parquet_footfall_result_path
            )
            print(
                "Convert complete! Time taken: {:.2f} second\n"
                .format(time.process_time()-start_time)
                )
            # print("Converting into shapefile..")
            # start_time = time.process_time()
            # (
            #     gpd.GeoDataFrame(
            #         spatial_joined_poi.to_pandas(),
            #         geometry=gpd.points_from_xy(
            #             spatial_joined_poi.to_pandas().longitude_poi,
            #             spatial_joined_poi.to_pandas().latitude_poi
            #         )
            #     )
            # ).to_file(
            #     shp_footfall_result_path,
            #     engine='pyogrio'
            # )
            print(
                "Convert complete! Time taken: {:.2f} second\n"
                .format(time.process_time()-start_time)
                )
        else:
            print(f"{parquet_footfall_result_path} already exist\nSkipping process...\n")
        current_date += timedelta(days=PROCESSING_GRANULARITY)
print(
    "Process Complete! Total time taken:{:.2f} second"
    .format(time.process_time() - program_start_time)
    )

print(datetime.now())
