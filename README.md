# Analyzing PDSI and Precipitation by county 1895-2016

## Analyzing the Impact of Precipitation on the Palmer Drought Severity Index (PDSI) in the Contiguous US

Capstone Project
By Scotty Hall

Creates visualizations:

- Multivariate bubble charts
- k-NN regression line chart
- County maps with continuous colors

Creates Tables:

- counties
  - id
  - fips
  - name
- drought
  - id
  - year
  - month
  - state_fips
  - county_fips
  - pdsi
- rain
  - id
  - state_id
  - county_id
  - year
  - precip by month with each month having a column ('jan', 'feb', etc.)
- pdsi_precip
  - id
  - year
  - month
  - county_fips
  - pdsi
  - precip
  - state_fips
- states
  - id
  - name
  - postal_code
  - fips
  - noaa_code

Requires:

- PostgreSQL database (Optional)
- Python3
  - Various Dependencies

Data Sources:

climdiv-pcpncy-v1.0.0-20220108.csv,
drought.csv
ftp://ftp.ncdc.noaa.gov/pub/data/cirs/climdiv/
ftp://ftp.ncdc.noaa.gov/pub/data/normals/1981-2010/

Geo.json (downloaded 2/22/22 for file freeze)
https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json

County Fips Codes
https://www.nrcs.usda.gov/wps/portal/nrcs/detail/national/home/?cid=nrcs143_013697

State Fips Codes
https://www.nrcs.usda.gov/wps/portal/nrcs/detail/?cid=nrcs143_013696
