#--------------------------------------------------------------------------------# NASA POWER Zarr

module NASAPowerZarr

import ..GeoDataAccess
using ..GeoDataAccess: AbstractZarrSource, MetaData, _register_source!,
    WEATHER, RASTER, TemporalType
using Dates
using EnumX

@enumx Product merra2 syn1deg geosit imerg srb flashflux
@enumx Frequency daily hourly monthly annual climatology
@enumx Orientation temporal spatial

"""
    NASAPowerZarr.Source(; product=Product.merra2, frequency=Frequency.daily, orientation=Orientation.temporal)

NASA POWER Analysis-Ready Data on [AWS S3](https://registry.opendata.aws/nasa-power/).

- **Coverage**: Global, 0.5° × 0.625° (MERRA-2) or 1° (SYN1Deg)
- **Temporal**: Daily/Hourly/Monthly, 1981–present
- **Format**: Zarr v2, consolidated metadata
- **Auth**: None (anonymous S3 access, `us-west-2`)

### Examples

```julia
using Zarr, AWS

AWS.global_aws_config(AWSConfig(creds=nothing, region="us-west-2"))
url = store_url(NASAPowerZarr.Source())
store = zopen(url, consolidated=true)
t2m = store["T2M"]
```
"""
struct Source <: AbstractZarrSource
    product::Product.T
    frequency::Frequency.T
    orientation::Orientation.T
end

function Source(; product::Product.T=Product.merra2,
                 frequency::Frequency.T=Frequency.daily,
                 orientation::Orientation.T=Orientation.temporal)
    Source(product, frequency, orientation)
end

GeoDataAccess.name(::Type{Source}) = "nasapowerzarr"

function GeoDataAccess.store_url(s::Source)
    p = lowercase(string(s.product))
    f = lowercase(string(s.frequency))
    o = lowercase(string(s.orientation))
    "s3://nasa-power/$p/$o/power_$(p)_$(f)_$(o)_utc.zarr"
end

const variables = (;
    T2M              = "Temperature at 2m (°C)",
    T2M_MAX          = "Maximum temperature at 2m (°C)",
    T2M_MIN          = "Minimum temperature at 2m (°C)",
    T2M_RANGE        = "Temperature range at 2m (°C)",
    T2MDEW           = "Dewpoint temperature at 2m (°C)",
    T2MWET           = "Wet bulb temperature at 2m (°C)",
    RH2M             = "Relative humidity at 2m (%)",
    QV2M             = "Specific humidity at 2m (kg/kg)",
    PRECTOTCORR      = "Corrected total precipitation (mm/day)",
    PS               = "Surface pressure (kPa)",
    WS2M             = "Wind speed at 2m (m/s)",
    WS10M            = "Wind speed at 10m (m/s)",
    WS50M            = "Wind speed at 50m (m/s)",
    WD2M             = "Wind direction at 2m (°)",
    WD10M            = "Wind direction at 10m (°)",
    U2M              = "Eastward wind at 2m (m/s)",
    V2M              = "Northward wind at 2m (m/s)",
    U10M             = "Eastward wind at 10m (m/s)",
    V10M             = "Northward wind at 10m (m/s)",
    TS               = "Earth skin temperature (°C)",
    GWETTOP          = "Surface soil wetness (0–1)",
    GWETROOT         = "Root zone soil wetness (0–1)",
    GWETPROF         = "Profile soil moisture (0–1)",
    EVLAND           = "Evaporation land (kg/m²/s)",
    SNODP            = "Snow depth (m)",
    FROST_DAYS       = "Frost days (count)",
    SLP              = "Sea level pressure (kPa)",
    RHOA             = "Air density at surface (kg/m³)",
    CDD18_3          = "Cooling degree days (base 18.3°C)",
    HDD18_3          = "Heating degree days (base 18.3°C)",
)

const metadata = MetaData(
    "", "None (public S3)",
    WEATHER, variables,
    RASTER, "0.5° × 0.625°", "Global",
    TemporalType.timeseries, Day(1), "1981–present",
    "NASA Open Data",
    "https://power.larc.nasa.gov/docs/services/aws/",
)

GeoDataAccess.MetaData(::Source) = metadata

_register_source!(Source())

end # module NASAPowerZarr
