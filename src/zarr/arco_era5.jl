module ARCOERA5

import ..GeoDataAccess
using ..GeoDataAccess: AbstractZarrSource, MetaData, _register_source!,
    WEATHER, RASTER, TemporalType
using Dates

"""
    ARCOERA5.Source()

Analysis-Ready, Cloud-Optimized ERA5 reanalysis data on
[Google Cloud](https://cloud.google.com/storage/docs/public-datasets/era5).

- **Coverage**: Global, 0.25° (~25 km) resolution
- **Temporal**: Hourly, 1940–present
- **Format**: Zarr v2, consolidated metadata
- **Auth**: None (public GCS bucket)
- **Chunks**: `[1, 721, 1440]` per timestep

### Examples

```julia
using Zarr

url = store_url(ARCOERA5.Source())
store = zopen(url, consolidated=true)
t2m = store["2m_temperature"]
```
"""
struct Source <: AbstractZarrSource end

GeoDataAccess.name(::Type{Source}) = "arcoera5"

GeoDataAccess.store_url(::Source) = "gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3"

const variables = (;
    var"2m_temperature"                 = "2m temperature (K)",
    var"2m_dewpoint_temperature"        = "2m dewpoint temperature (K)",
    var"10m_u_component_of_wind"        = "10m u-component of wind (m/s)",
    var"10m_v_component_of_wind"        = "10m v-component of wind (m/s)",
    var"total_precipitation"            = "Total precipitation (m)",
    var"surface_pressure"               = "Surface pressure (Pa)",
    var"mean_sea_level_pressure"        = "Mean sea level pressure (Pa)",
    var"skin_temperature"               = "Skin temperature (K)",
    var"sea_surface_temperature"        = "Sea surface temperature (K)",
    var"total_cloud_cover"              = "Total cloud cover (0–1)",
    var"surface_solar_radiation_downwards" = "Surface solar radiation downwards (J/m²)",
    var"surface_thermal_radiation_downwards" = "Surface thermal radiation downwards (J/m²)",
    var"snowfall"                       = "Snowfall (m of water equivalent)",
    var"snow_depth"                     = "Snow depth (m of water equivalent)",
    var"boundary_layer_height"          = "Boundary layer height (m)",
    var"total_evaporation"              = "Total evaporation (m of water equivalent)",
    var"convective_available_potential_energy" = "CAPE (J/kg)",
    var"temperature"                    = "Temperature at model levels (K)",
    var"geopotential"                   = "Geopotential at model levels (m²/s²)",
    var"specific_humidity"              = "Specific humidity at model levels (kg/kg)",
)

const metadata = MetaData(
    "", "None (public GCS)",
    WEATHER, variables,
    RASTER, "0.25° (~25 km)", "Global",
    TemporalType.timeseries, Hour(1), "1940–present",
    "CC BY 4.0",
    "https://cloud.google.com/storage/docs/public-datasets/era5",
)

GeoDataAccess.MetaData(::Source) = metadata

_register_source!(Source())

end # module ARCOERA5
