# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     custom_cell_magics: kql
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: ds-raster-explore
#     language: python
#     name: ds-raster-explore
# ---

# %%
import tqdm
import xarray as xr 
import pandas as pd
import os
from dotenv import load_dotenv
from azure.storage.blob import ContainerClient
import rioxarray as rxr
import io
import zipfile
import matplotlib.pyplot as plt
import geopandas as gpd
from datetime import datetime
from dask.distributed import Client, LocalCluster

# %%
client = Client(LocalCluster(n_workers=4, threads_per_worker=2, memory_limit='2GB'))


# %%
load_dotenv()
# DEV_BLOB_SAS = os.getenv("DEV_BLOB_SAS")
DEV_BLOB_SAS = os.getenv("DSCI_AZ_SAS_DEV")
DEV_BLOB_NAME = "imb0chd0dev"
# IMERG_ZARR_ROOT = "az://global/imerg.zarr"
DEV_BLOB_URL = f"https://{DEV_BLOB_NAME}.blob.core.windows.net/"
DEV_BLOB_PROJ_URL = DEV_BLOB_URL + "projects" + "?" + DEV_BLOB_SAS
GLOBAL_CONTAINER_NAME = "global"
DEV_BLOB_GLB_URL = (DEV_BLOB_URL + GLOBAL_CONTAINER_NAME + "?" + DEV_BLOB_SAS)

# %%
dev_container_client = ContainerClient.from_container_url(DEV_BLOB_PROJ_URL)
dev_glb_container_client = ContainerClient.from_container_url(DEV_BLOB_GLB_URL)

# %%
def _load_blob_data(blob_name):
    container_client = dev_container_client
    blob_client = container_client.get_blob_client(blob_name)
    data = blob_client.download_blob().readall()
    return data
    
def _load_gdf_from_blob(blob_name, shapefile: str = None):
    blob_data = _load_blob_data(blob_name)
    with zipfile.ZipFile(io.BytesIO(blob_data), "r") as zip_ref:
        zip_ref.extractall("temp")
        if shapefile is None:
            shapefile = [f for f in zip_ref.namelist() if f.endswith(".shp")][
                0
            ]
        gdf = gpd.read_file(f"temp/{shapefile}")
    return gdf

# %%
blob_names = existing_files = [
    x.name
    for x in dev_glb_container_client.list_blobs(
        name_starts_with="raster/cogs/aer"
    )
]

# %%
import re

def extract_date(filename):
    regex = r"aer_area_300s_(\d{8})_v05r01\.tif"
    match = re.search(regex, filename)
    if match:
        date_str = match.group(1)
        return datetime.strptime(date_str, "%Y%m%d")
    else:
        return None

blob_names_sorted = sorted(blob_names, key=extract_date)


# %%

# Define the coordinates for the bounding box of Somalia
minx = 40.9869
miny = -1.6741
maxx = 51.6177
maxy = 12.0246

# 5-8mins
das = []
for blob_name in tqdm.tqdm(blob_names_sorted):
    cog_url = (
        f"https://{DEV_BLOB_NAME}.blob.core.windows.net/global/"
        f"{blob_name}?{DEV_BLOB_SAS}"
    )
    da_in = rxr.open_rasterio(
        cog_url, masked=True, chunks={"band": 1, "x": 225, "y":
         900}
    )
    da_in = da_in.sel(x=slice(minx, maxx), y=slice(miny, maxy))
    #date_in = pd.to_datetime(blob_name.split(".")[0][-10:])
    date_in = extract_date(blob_name)
    da_in["date"] = date_in

    # Persisting to reduce the number of downstream Dask layers
    da_in = da_in.persist()
    das.append(da_in)

ds = xr.concat(das, dim="date", join='override', combine_attrs='drop')

# %%



# import matplotlib.pyplot as plt



# Create a geopandas dataframe with the bounding box
# bounding_box_gdf = gpd.GeoDataFrame(geometry=[gpd.box(minx, miny, maxx, maxy)], crs='EPSG:4326')

# # Plot the bounding box on a basemap
# world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
# ax = world.plot(figsize=(10, 10), color='white', edgecolor='black')
# bounding_box_gdf.plot(ax=ax, facecolor='none', edgecolor='red')

# plt.show()
# bounding_box = somalia_shapefile.total_bounds

# # Create a geopandas dataframe with the bounding box
# bounding_box_gdf = gpd.GeoDataFrame(geometry=[gpd.box(*bounding_box)], crs=somalia_shapefile.crs)

# # Plot the bounding box
# bounding_box_gdf.plot()

# gdf = _load_gdf_from_blob(
#     "ds-aa-hti-hurricanes/raw/codab/hti.shp.zip",
#     shapefile="hti_adm0.shp"
# )

# minx, miny, maxx, maxy = gdf.total_bounds


# %%

# %%
