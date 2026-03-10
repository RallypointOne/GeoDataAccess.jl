#--------------------------------------------------------------------------------# NASA FIRMS

module NASAFIRMS

import ..GeoDataAccess
using ..GeoDataAccess: AbstractDataSource, MetaData, DataAccessPlan, RequestInfo,
    _register_source!, _get_api_key, _describe_extent, _estimate_bytes,
    NATURAL_HAZARDS, POINT, TemporalType, HTTPMethod
using Dates
import GeoInterface as GI

"""
    NASAFIRMS.Source()

Active fire hotspot data from [NASA FIRMS](https://firms.modaps.eosdis.nasa.gov/).

- **Coverage**: Global, near real-time + archive
- **Resolution**: 375 m (VIIRS) / 1 km (MODIS)
- **API Key**: Required (`FIRMS_MAP_KEY` environment variable)
- **Rate Limit**: 5,000 requests per 10 minutes

### Examples

```julia
ENV["FIRMS_MAP_KEY"] = "your-map-key"
using GeoInterface.Extents: Extent

plan = DataAccessPlan(NASAFIRMS.Source(),
    Extent(X=(-125.0, -114.0), Y=(32.0, 42.0)),
    Date(2024, 7, 1), Date(2024, 7, 5))
files = fetch(plan)
```
"""
struct Source <: AbstractDataSource end

GeoDataAccess.name(::Type{Source}) = "nasafirms"

const AREA_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
const COUNTRY_URL = "https://firms.modaps.eosdis.nasa.gov/api/country/csv"

const variables = (;
    latitude    = "Center latitude of fire pixel (°)",
    longitude   = "Center longitude of fire pixel (°)",
    bright_ti4  = "VIIRS I-4 brightness temperature (K)",
    bright_ti5  = "VIIRS I-5 brightness temperature (K)",
    frp         = "Fire Radiative Power (MW)",
    confidence  = "Detection confidence",
    acq_date    = "Acquisition date (YYYY-MM-DD)",
    acq_time    = "Acquisition time UTC (HHMM)",
    satellite   = "Satellite platform",
    daynight    = "Day (D) or Night (N) observation",
    scan        = "Along-scan pixel size (km)",
    track       = "Along-track pixel size (km)",
)

const metadata = MetaData(
    "FIRMS_MAP_KEY", "5,000 req/10 min",
    NATURAL_HAZARDS, variables,
    POINT, "375 m (VIIRS) / 1 km (MODIS)", "Global",
    TemporalType.timeseries, nothing, "Near real-time + archive",
    "NASA EOSDIS",
    "https://firms.modaps.eosdis.nasa.gov/api/";
    load_packages = Dict("DataFrames" => "a93c6f00-e57d-5684-b7b6-d8193f3e46c0",
                         "CSV" => "336ed68f-0bac-5ca0-87d4-7b16caf5d00b"),
    default_retention = Day(1),
)

GeoDataAccess.MetaData(::Source) = metadata

function GeoDataAccess.DataAccessPlan(source::Source, extent, start_date::Date, stop_date::Date;
                                       satellite::String = "VIIRS_SNPP_NRT",
                                       country::String = "",
                                       retention::Union{Nothing, Dates.Period} = metadata.default_retention)
    api_key = _get_api_key(source)
    n_days = Dates.value(stop_date - start_date) + 1
    n_days < 1 && error("stop_date must be on or after start_date")

    if !isempty(country)
        base_url = COUNTRY_URL
        spatial_param = country
        extent_desc = "Country: $country"
    else
        base_url = AREA_URL
        west, south, east, north = _bbox(extent)
        spatial_param = "$west,$south,$east,$north"
        extent_desc = _describe_extent(extent)
    end

    requests = RequestInfo[]
    current = start_date
    while current <= stop_date
        remaining = Dates.value(stop_date - current) + 1
        chunk = min(remaining, 10)
        date_str = Dates.format(current, dateformat"yyyy-mm-dd")
        url = "$base_url/$api_key/$satellite/$spatial_param/$chunk/$date_str"
        push!(requests, RequestInfo(source, url, HTTPMethod.GET,
            "$satellite, $chunk days from $date_str"; ext=".csv"))
        current += Day(chunk)
    end

    kwargs = Dict{Symbol, Any}(:satellite => satellite)
    !isempty(country) && (kwargs[:country] = country)

    DataAccessPlan(source, requests, extent_desc,
        (start_date, stop_date), collect(keys(variables)), kwargs,
        _estimate_bytes(n_days * 100, length(variables)), retention)
end

#--------------------------------------------------------------------------------# Helpers

function _bbox(extent)
    trait = GI.geomtrait(extent)
    _bbox(trait, extent)
end

function _bbox(::GI.PointTrait, geom)
    lon, lat = GI.x(geom), GI.y(geom)
    (lon - 0.5, lat - 0.5, lon + 0.5, lat + 0.5)
end

function _bbox(::GI.AbstractPolygonTrait, geom)
    _bbox(nothing, GI.extent(geom))
end

function _bbox(::Nothing, geom)
    if hasproperty(geom, :X) && hasproperty(geom, :Y)
        xmin, xmax = geom.X
        ymin, ymax = geom.Y
        return (xmin, ymin, xmax, ymax)
    end
    error("Cannot extract bounding box from $(typeof(geom)) for NASA FIRMS.")
end

function _bbox(::Union{GI.MultiPointTrait, GI.AbstractCurveTrait}, geom)
    points = collect(GI.getpoint(geom))
    lons = [GI.x(p) for p in points]
    lats = [GI.y(p) for p in points]
    (minimum(lons), minimum(lats), maximum(lons), maximum(lats))
end

_register_source!(Source())

end # module NASAFIRMS
