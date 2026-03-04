#--------------------------------------------------------------------------------# USGS Earthquake

"""
    USGSEarthquake()

Earthquake event data from the [USGS Earthquake Hazards Program](https://earthquake.usgs.gov/).

- **Coverage**: Global, comprehensive catalog
- **Resolution**: Event-based (point locations)
- **API Key**: Not required
- **Rate Limit**: Max 20,000 events per query

Supports bounding box queries via `Extent` or `Polygon` geometries, and point+radius queries
via `Point` / `Tuple`.  Returns GeoJSON FeatureCollections.

### Examples

```julia
using GeoInterface.Extents: Extent

# M4+ earthquakes in California, January 2024
plan = DataAccessPlan(USGSEarthquake(),
    Extent(X=(-125.0, -114.0), Y=(32.0, 42.0)),
    Date(2024, 1, 1), Date(2024, 1, 31);
    minmagnitude = 4.0)
files = fetch(plan)
```
"""
struct USGSEarthquake <: AbstractDataSource end

_register_source!(USGSEarthquake())

const USGS_EARTHQUAKE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

const USGS_EARTHQUAKE_VARIABLES = Dict{Symbol, String}(
    :mag        => "Magnitude",
    :place      => "Description of location",
    :time       => "Event time (ms since epoch)",
    :depth      => "Depth (km)",
    :felt       => "Number of felt reports",
    :cdi        => "Community Decimal Intensity",
    :mmi        => "Modified Mercalli Intensity",
    :alert      => "PAGER alert level",
    :sig        => "Significance (0-1000)",
    :tsunami    => "Tsunami association flag",
    :magType    => "Magnitude type (ml, mb, mw, etc.)",
    :type       => "Event type (earthquake, quarry blast, etc.)",
)

#--------------------------------------------------------------------------------# MetaData

MetaData(::USGSEarthquake) = MetaData(
    "", "20,000 events/query",
    :natural_hazards, USGS_EARTHQUAKE_VARIABLES,
    :point, "Event-based", "Global",
    :timeseries, nothing, "Comprehensive catalog",
    "Public Domain",
    "https://earthquake.usgs.gov/fdsnws/event/1/";
    load_packages = Dict("DataFrames" => "a93c6f00-e57d-5684-b7b6-d8193f3e46c0"),
)

#--------------------------------------------------------------------------------# DataAccessPlan

function DataAccessPlan(source::USGSEarthquake, extent, start_date::Date, stop_date::Date;
                        minmagnitude::Union{Nothing, Float64} = nothing,
                        maxmagnitude::Union{Nothing, Float64} = nothing,
                        mindepth::Union{Nothing, Float64} = nothing,
                        maxdepth::Union{Nothing, Float64} = nothing,
                        limit::Int = 20000,
                        orderby::String = "time")
    trait = GI.geomtrait(extent)
    _usgs_eq_plan(source, trait, extent, start_date, stop_date;
                  minmagnitude, maxmagnitude, mindepth, maxdepth, limit, orderby)
end

# Point query → radial search (default 100km radius)
function _usgs_eq_plan(source::USGSEarthquake, ::GI.PointTrait, geom,
                       start_date, stop_date; kw...)
    lat, lon = GI.y(geom), GI.x(geom)
    params = _usgs_eq_base_params(start_date, stop_date; kw...)
    params["latitude"] = string(lat)
    params["longitude"] = string(lon)
    params["maxradiuskm"] = "100"
    _usgs_eq_build_plan(source, params, start_date, stop_date,
        "Point($lat, $lon), radius 100 km"; kw...)
end

# Extent / bounding box
function _usgs_eq_plan(source::USGSEarthquake, ::Nothing, geom,
                       start_date, stop_date; kw...)
    if !(hasproperty(geom, :X) && hasproperty(geom, :Y))
        error("Cannot extract coordinates from $(typeof(geom)) for USGS Earthquake.")
    end
    xmin, xmax = geom.X
    ymin, ymax = geom.Y
    params = _usgs_eq_base_params(start_date, stop_date; kw...)
    params["minlongitude"] = string(xmin)
    params["maxlongitude"] = string(xmax)
    params["minlatitude"] = string(ymin)
    params["maxlatitude"] = string(ymax)
    _usgs_eq_build_plan(source, params, start_date, stop_date,
        _describe_extent(nothing, geom); kw...)
end

# Polygon → extract extent
function _usgs_eq_plan(source::USGSEarthquake, ::GI.AbstractPolygonTrait, geom,
                       start_date, stop_date; kw...)
    _usgs_eq_plan(source, nothing, GI.extent(geom), start_date, stop_date; kw...)
end

function _usgs_eq_base_params(start_date, stop_date;
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

function _usgs_eq_build_plan(source, params, start_date, stop_date, extent_desc; kw...)
    url = _build_url(USGS_EARTHQUAKE_URL, params)
    n_days = Dates.value(stop_date - start_date) + 1
    request = RequestInfo(source, url, :GET, "$extent_desc, $n_days days")
    kwargs = Dict{Symbol, Any}()
    for (k, v) in pairs(kw)
        !isnothing(v) && (kwargs[k] = v)
    end
    DataAccessPlan(source, [request], extent_desc,
        (start_date, stop_date), collect(keys(USGS_EARTHQUAKE_VARIABLES)), kwargs,
        _estimate_bytes(n_days * 10, length(USGS_EARTHQUAKE_VARIABLES)))
end
