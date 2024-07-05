
<!-- README.md is generated from README.Rmd. Please edit that file -->

# ds-floodscan-ingest

<!-- badges: start -->
<!-- badges: end -->

The primary purpose of this repo is to host a pipeline to copy AER
FloodScan data to our Azure Blob Storage.

Raw data provided in 2 formats (for both SFED & MFED):

1.  90d rotating zip file containing GeoTifs for 2024 onwards
2.  a `.nc` file containing historical FloodScan data from 1998-2023

## GitHub Action (continuous)

The automated pipeline was set up in 2 phases using GitHub Actions
(GHA). Scripts triggered by GHA sit in the `src/` directory.

- **Phase 1**: was developed prior to our blob storage being set up and
  simply downloaded the zip file each day to our gdrive folder:
  `Shared drives/Data Science - Data Storage/private/raw/glb/FloodScan`
  - `.github/workflows/floodscan-ingest.yaml` which triggers
    `src/DL_zips.R`
  - this process could be archived as it was replaced by phase 2 below
- **Phase 2**: was developed once our blob storage account was created.
  It sends the data to our “dev” blob storage “global” container with
  the prefix of `raster/cogs/aer_area_300s_{yyyymmdd}_v05r01.tif`. Each
  COG represents 1 day of FloodScan data with both SFED & MFED bands.
  - `.github/workflows/floodcan-cog-blob.yml` which triggers
    `src/upload_latest_cog.R`
  - This process downloads the SFED & MFED 90 day zip -\> extracts the
    latest tifs from each -\> merges them into 1 spatRaster -\> and then
    writes to the blob as a Cloud Optimized Geotif (COG).

## Upload of historical (one-time)

There are 2 one-time static upload processes that upload the historical
data to our blob storage. They are both contained in the `data-raw`
folder:

- `data-raw/process_historical_nc_to_cogs.R` -\> processes the both SFED
  & MFED historical `.nc` files to single date cogs with 2 bands (SFED &
  MFED) and uploads to blob.
- `data-raw/floodscan_cogs_recent.R` -\> As the `.nc` files do not
  contain data for 2024, this script processes the geotifs obtained in
  teh 90d rotating zip files (pipeline phase 1) to create & upload COGS
  in the same format.