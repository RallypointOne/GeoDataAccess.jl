#--------------------------------------------------------------------------------# HRRR Zarr (MesoWest / University of Utah)

module HRRRZarr

import ..GeoDataAccess
using ..GeoDataAccess: AbstractZarrSource, MetaData, _register_source!,
    WEATHER, RASTER, TemporalType, HRRRLevel, HRRRRunType
using Dates

"""
    HRRRZarr.Source(; level=HRRRLevel.sfc)

High-Resolution Rapid Refresh (HRRR) model data in Zarr format on
[AWS S3](https://registry.opendata.aws/noaa-hrrr-pds/), maintained by the
University of Utah MesoWest group.

- **Coverage**: CONUS, 3 km resolution (1059 × 1799 grid)
- **Temporal**: Hourly, 2014–present
- **Format**: Zarr v2, consolidated metadata
- **Auth**: None (public S3 bucket, `us-west-1`)
- **Chunks**: 150 × 150 gridpoints per chunk (96 chunks per field)

Two levels: `HRRRLevel.sfc` (surface & near-surface) or `HRRRLevel.prs` (pressure levels).

### Examples

```julia
using Zarr

# The store URL encodes date, cycle, run type, level, and variable:
src = HRRRZarr.Source()
url = store_url(src; date="20230601", cycle="00", run=HRRRRunType.anl,
                level="2m_above_ground", variable="TMP")
store = zopen(url, consolidated=true)
```
"""
struct Source <: AbstractZarrSource
    level::HRRRLevel.T
    function Source(; level::HRRRLevel.T=HRRRLevel.sfc)
        new(level)
    end
end

GeoDataAccess.name(::Type{Source}) = "hrrrzarr"

"""
    store_url(src::Source; date, cycle, run, level, variable) -> String

Build the S3 URL for a specific HRRR Zarr variable slice.

- `date`: `"YYYYMMDD"` string
- `cycle`: `"00"` through `"23"` (2-digit hour)
- `run`: `HRRRRunType.anl` (analysis) or `HRRRRunType.fcst` (forecast)
- `level`: e.g. `"surface"`, `"2m_above_ground"`, `"10m_above_ground"`, `"500mb"`
- `variable`: e.g. `"TMP"`, `"UGRD"`, `"REFC"`
"""
function GeoDataAccess.store_url(src::Source;
        date::AbstractString="",
        cycle::AbstractString="00",
        run::HRRRRunType.T=HRRRRunType.anl,
        level::AbstractString="surface",
        variable::AbstractString="TMP")
    isempty(date) && error("date keyword is required, e.g. date=\"20230601\"")
    lv = lowercase(string(src.level))
    "s3://hrrrzarr/$lv/$date/$(date)_$(cycle)z_$(lowercase(string(run))).zarr/$(level)/$(variable)/$(level)/"
end

const variables = (;
    TMP                  = "Temperature (K)",
    DPT                  = "Dewpoint temperature (K)",
    RH                   = "Relative humidity (%)",
    SPFH                 = "Specific humidity (kg/kg)",
    POT                  = "Potential temperature (K)",
    UGRD                 = "U-component of wind (m/s)",
    VGRD                 = "V-component of wind (m/s)",
    WIND                 = "Wind speed (m/s)",
    GUST                 = "Wind gust speed (m/s)",
    PRES                 = "Pressure (Pa)",
    HGT                  = "Geopotential height (gpm)",
    VIS                  = "Visibility (m)",
    REFC                 = "Composite reflectivity (dBZ)",
    APCP                 = "Accumulated precipitation (kg/m²)",
    PRATE                = "Precipitation rate (kg/m²/s)",
    FRZR                 = "Freezing rain (kg/m²)",
    ASNOW                = "Accumulated snow (m)",
    WEASD                = "Water equivalent of snow depth (kg/m²)",
    SNOD                 = "Snow depth (m)",
    SNOWC                = "Snow cover (%)",
    TCDC                 = "Total cloud cover (%)",
    DSWRF                = "Downward shortwave radiation flux (W/m²)",
    DLWRF                = "Downward longwave radiation flux (W/m²)",
    USWRF                = "Upward shortwave radiation flux (W/m²)",
    ULWRF                = "Upward longwave radiation flux (W/m²)",
    CAPE                 = "Convective available potential energy (J/kg)",
    CIN                  = "Convective inhibition (J/kg)",
    PWAT                 = "Precipitable water (kg/m²)",
    VIL                  = "Vertically integrated liquid (kg/m²)",
    HLCY                 = "Storm relative helicity (m²/s²)",
    MXUPHL               = "Maximum updraft helicity (m²/s²)",
    LTNG                 = "Lightning (flashes/km²/5min)",
    GFLUX                = "Ground heat flux (W/m²)",
    SHTFL                = "Sensible heat flux (W/m²)",
    LHTFL                = "Latent heat flux (W/m²)",
    HAIL                 = "Maximum hail diameter (m)",
    MASSDEN              = "Smoke mass density at 8m (kg/m³)",
    COLMD                = "Column-integrated smoke mass density (kg/m²)",
)

const metadata = MetaData(
    "", "None (public S3)",
    WEATHER, variables,
    RASTER, "3 km", "CONUS",
    TemporalType.timeseries, Hour(1), "2014–present",
    "Public Domain",
    "https://mesowest.utah.edu/html/hrrr/zarr_documentation/",
)

GeoDataAccess.MetaData(::Source) = metadata

_register_source!(Source())

end # module HRRRZarr
