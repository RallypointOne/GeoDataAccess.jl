#--------------------------------------------------------------------------------# NOAA OISST

module NOAAOISST

import ..GeoDataAccess
using ..GeoDataAccess: AbstractDataSource, MetaData, DataAccessPlan, RequestInfo,
    _register_source!, _describe_extent,
    WEATHER, RASTER, TemporalType, HTTPMethod
using Dates

"""
    NOAAOISST.Source()

NOAA Optimum Interpolation Sea Surface Temperature (OISST) daily data from
[NCEI](https://www.ncei.noaa.gov/products/optimum-interpolation-sst).  Provides global
daily SST on a 0.25° grid, blending satellite, ship, buoy, and Argo float observations.

- **Coverage**: Global, 0.25° (~25 km) resolution
- **Temporal**: Daily, 1981–present
- **API Key**: None required
- **Rate Limit**: None (be respectful)
- **Response Format**: NetCDF

### Examples

```julia
using GeoInterface.Extents: Extent
using Dates

plan = DataAccessPlan(NOAAOISST.Source(),
    Extent(X=(-80.0, -60.0), Y=(30.0, 45.0)),
    Date(2024, 1, 1), Date(2024, 1, 7))
files = fetch(plan)
```
"""
struct Source <: AbstractDataSource end

GeoDataAccess.name(::Type{Source}) = "noaaoisst"

const URL = "https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/v2.1/access/avhrr"

const variables = (;
    sst  = "Sea surface temperature (°C)",
    anom = "SST anomaly from climatology (°C)",
    ice  = "Sea ice concentration (%)",
    err  = "Estimated SST error (°C)",
)

const metadata = MetaData(
    "", "None (be respectful)",
    WEATHER, variables,
    RASTER, "0.25° (~25 km)", "Global",
    TemporalType.timeseries, Day(1), "1981–present",
    "Public Domain",
    "https://www.ncei.noaa.gov/products/optimum-interpolation-sst";
    load_packages = Dict("Rasters" => "a3a2b9e3-a471-40c9-b274-f788e487c689"),
)

GeoDataAccess.MetaData(::Source) = metadata

function GeoDataAccess.DataAccessPlan(source::Source, extent, start_date::Date, stop_date::Date;
                                       variables::Vector{Symbol} = [:sst],
                                       retention::Union{Nothing, Dates.Period} = metadata.default_retention)
    start_date <= stop_date || error("start_date must be <= stop_date")
    requests = RequestInfo[]
    for d in start_date:Day(1):stop_date
        ym = Dates.format(d, dateformat"yyyymm")
        ymd = Dates.format(d, dateformat"yyyymmdd")
        url = "$URL/$ym/oisst-avhrr-v02r01.$ymd.nc"
        push!(requests, RequestInfo(source, url, HTTPMethod.GET, "OISST $ymd"; ext=".nc"))
    end
    extent_desc = _describe_extent(extent)
    n_days = Dates.value(stop_date - start_date) + 1
    DataAccessPlan(source, requests, extent_desc,
        (start_date, stop_date), variables,
        Dict{Symbol, Any}(), n_days * 1_600_000, retention)
end

_register_source!(Source())

end # module NOAAOISST
