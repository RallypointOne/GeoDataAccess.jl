#--------------------------------------------------------------------------------# GFS Zarr (dynamical.org)

module GFSZarr

import ..GeoDataAccess
using ..GeoDataAccess: AbstractZarrSource, MetaData, _register_source!,
    WEATHER, RASTER, TemporalType

"""
    GFSZarr.Source()

NOAA GFS forecast data via [dynamical.org](https://dynamical.org/catalog/noaa-gfs-forecast/).

- **Coverage**: Global, 0.25° (~25 km) resolution
- **Temporal**: Forecast init every 6h, lead times 0–384h
- **Format**: Zarr v3 with sharding
- **Auth**: None (public HTTPS)

### Examples

```julia
using Zarr

url = store_url(GFSZarr.Source())
store = zopen(url)
t2m = store["temperature_2m"]
```
"""
struct Source <: AbstractZarrSource end

GeoDataAccess.name(::Type{Source}) = "gfszarr"

GeoDataAccess.store_url(::Source) = "https://data.dynamical.org/noaa/gfs/forecast/latest.zarr"

const variables = (;
    temperature_2m                           = "Temperature at 2m (K)",
    maximum_temperature_2m                   = "Maximum temperature at 2m (K)",
    minimum_temperature_2m                   = "Minimum temperature at 2m (K)",
    relative_humidity_2m                     = "Relative humidity at 2m (%)",
    wind_u_10m                               = "U-component of wind at 10m (m/s)",
    wind_v_10m                               = "V-component of wind at 10m (m/s)",
    wind_u_100m                              = "U-component of wind at 100m (m/s)",
    wind_v_100m                              = "V-component of wind at 100m (m/s)",
    pressure_surface                         = "Surface pressure (Pa)",
    pressure_reduced_to_mean_sea_level       = "Mean sea level pressure (Pa)",
    precipitation_surface                    = "Precipitation (kg/m²)",
    precipitable_water_atmosphere            = "Precipitable water (kg/m²)",
    total_cloud_cover_atmosphere             = "Total cloud cover (%)",
    downward_short_wave_radiation_flux_surface = "Downward shortwave radiation (W/m²)",
    downward_long_wave_radiation_flux_surface  = "Downward longwave radiation (W/m²)",
    geopotential_height_cloud_ceiling        = "Cloud ceiling geopotential height (gpm)",
    categorical_rain_surface                 = "Categorical rain (0/1)",
    categorical_snow_surface                 = "Categorical snow (0/1)",
    categorical_freezing_rain_surface        = "Categorical freezing rain (0/1)",
    categorical_ice_pellets_surface          = "Categorical ice pellets (0/1)",
    percent_frozen_precipitation_surface     = "Frozen precipitation fraction (%)",
)

const metadata = MetaData(
    "", "None (public HTTPS)",
    WEATHER, variables,
    RASTER, "0.25° (~25 km)", "Global",
    TemporalType.forecast, nothing, "Forecast (6h init, 0–384h lead), 2021–present",
    "Public Domain",
    "https://dynamical.org/catalog/noaa-gfs-forecast/",
)

GeoDataAccess.MetaData(::Source) = metadata

_register_source!(Source())

end # module GFSZarr
