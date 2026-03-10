#--------------------------------------------------------------------------------# USGS Earthquake

module USGSEarthquake

import ..GeoDataAccess
using ..GeoDataAccess: AbstractDataSource, MetaData, DataAccessPlan, RequestInfo,
    _register_source!, _build_url, _describe_extent, _estimate_bytes,
    NATURAL_HAZARDS, POINT, TemporalType, HTTPMethod
using Dates
import GeoInterface as GI

"""
    USGSEarthquake.Source()

Earthquake event data from the [USGS Earthquake Hazards Program](https://earthquake.usgs.gov/).

- **Coverage**: Global, comprehensive catalog
- **Resolution**: Event-based (point locations)
- **API Key**: Not required
- **Rate Limit**: Max 20,000 events per query

### Examples

```julia
using GeoInterface.Extents: Extent

plan = DataAccessPlan(USGSEarthquake.Source(),
    Extent(X=(-125.0, -114.0), Y=(32.0, 42.0)),
    Date(2024, 1, 1), Date(2024, 1, 31);
    minmagnitude = 4.0)
files = fetch(plan)
```
"""
struct Source <: AbstractDataSource end

GeoDataAccess.name(::Type{Source}) = "usgsearthquake"

const URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

const variables = (;
    mag        = "Magnitude",
    place      = "Description of location",
    time       = "Event time (ms since epoch)",
    depth      = "Depth (km)",
    felt       = "Number of felt reports",
    cdi        = "Community Decimal Intensity",
    mmi        = "Modified Mercalli Intensity",
    alert      = "PAGER alert level",
    sig        = "Significance (0-1000)",
    tsunami    = "Tsunami association flag",
    magType    = "Magnitude type (ml, mb, mw, etc.)",
    type       = "Event type (earthquake, quarry blast, etc.)",
)

const metadata = MetaData(
    "", "20,000 events/query",
    NATURAL_HAZARDS, variables,
    POINT, "Event-based", "Global",
    TemporalType.timeseries, nothing, "Comprehensive catalog",
    "Public Domain",
    "https://earthquake.usgs.gov/fdsnws/event/1/";
    load_packages = Dict("DataFrames" => "a93c6f00-e57d-5684-b7b6-d8193f3e46c0"),
)

GeoDataAccess.MetaData(::Source) = metadata

#--------------------------------------------------------------------------------# DataAccessPlan

function GeoDataAccess.DataAccessPlan(source::Source, extent, start_date::Date, stop_date::Date;
                                       minmagnitude::Union{Nothing, Float64} = nothing,
                                       maxmagnitude::Union{Nothing, Float64} = nothing,
                                       mindepth::Union{Nothing, Float64} = nothing,
                                       maxdepth::Union{Nothing, Float64} = nothing,
                                       limit::Int = 20000,
                                       orderby::String = "time",
                                       retention::Union{Nothing, Dates.Period} = metadata.default_retention)
    trait = GI.geomtrait(extent)
    _plan(source, trait, extent, start_date, stop_date, retention;
          minmagnitude, maxmagnitude, mindepth, maxdepth, limit, orderby)
end

function _plan(source::Source, ::GI.PointTrait, geom,
               start_date, stop_date, retention; kw...)
    lat, lon = GI.y(geom), GI.x(geom)
    params = _base_params(start_date, stop_date; kw...)
    params["latitude"] = string(lat)
    params["longitude"] = string(lon)
    params["maxradiuskm"] = "100"
    _build_plan(source, params, start_date, stop_date,
        "Point($lat, $lon), radius 100 km", retention; kw...)
end

function _plan(source::Source, ::Nothing, geom,
               start_date, stop_date, retention; kw...)
    if !(hasproperty(geom, :X) && hasproperty(geom, :Y))
        error("Cannot extract coordinates from $(typeof(geom)) for USGS Earthquake.")
    end
    xmin, xmax = geom.X
    ymin, ymax = geom.Y
    params = _base_params(start_date, stop_date; kw...)
    params["minlongitude"] = string(xmin)
    params["maxlongitude"] = string(xmax)
    params["minlatitude"] = string(ymin)
    params["maxlatitude"] = string(ymax)
    _build_plan(source, params, start_date, stop_date,
        _describe_extent(nothing, geom), retention; kw...)
end

function _plan(source::Source, ::GI.AbstractPolygonTrait, geom,
               start_date, stop_date, retention; kw...)
    _plan(source, nothing, GI.extent(geom), start_date, stop_date, retention; kw...)
end

function _base_params(start_date, stop_date;
                      minmagnitude=nothing, maxmagnitude=nothing,
                      mindepth=nothing, maxdepth=nothing,
                      limit=20000, orderby="time")
    params = Dict{String, String}(
        "format"    => "geojson",
        "starttime" => Dates.format(start_date, dateformat"yyyy-mm-dd"),
        "endtime"   => Dates.format(stop_date, dateformat"yyyy-mm-dd"),
        "limit"     => string(limit),
        "orderby"   => orderby,
    )
    !isnothing(minmagnitude) && (params["minmagnitude"] = string(minmagnitude))
    !isnothing(maxmagnitude) && (params["maxmagnitude"] = string(maxmagnitude))
    !isnothing(mindepth) && (params["mindepth"] = string(mindepth))
    !isnothing(maxdepth) && (params["maxdepth"] = string(maxdepth))
    params
end

function _build_plan(source, params, start_date, stop_date, extent_desc, retention; kw...)
    url = _build_url(URL, params)
    n_days = Dates.value(stop_date - start_date) + 1
    request = RequestInfo(source, url, HTTPMethod.GET, "$extent_desc, $n_days days")
    kwargs = Dict{Symbol, Any}()
    for (k, v) in pairs(kw)
        !isnothing(v) && (kwargs[k] = v)
    end
    DataAccessPlan(source, [request], extent_desc,
        (start_date, stop_date), collect(keys(variables)), kwargs,
        _estimate_bytes(n_days * 10, length(variables)), retention)
end

_register_source!(Source())

end # module USGSEarthquake
