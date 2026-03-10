#--------------------------------------------------------------------------------# OpenStreetMap

module OpenStreetMap

import ..GeoDataAccess
using ..GeoDataAccess: AbstractDataSource, MetaData, DataAccessPlan, RequestInfo,
    _register_source!, _build_url, _describe_extent,
    INFRASTRUCTURE, VECTOR_FEATURE, TemporalType, HTTPMethod
using Dates
import GeoInterface as GI

"""
    OpenStreetMap.Source()

Vector feature data from [OpenStreetMap](https://www.openstreetmap.org/) via the
[Overpass API](https://overpass-api.de/).

- **Coverage**: Global, continuously updated
- **Resolution**: Varies (individual features)
- **API Key**: Not required
- **Response Format**: JSON (Overpass JSON)

### Examples

```julia
using GeoInterface.Extents: Extent

plan = DataAccessPlan(OpenStreetMap.Source(),
    Extent(X=(-74.01, -73.99), Y=(40.70, 40.72));
    variables = [:building])
files = fetch(plan)
```
"""
struct Source <: AbstractDataSource end

GeoDataAccess.name(::Type{Source}) = "openstreetmap"

const URL = "https://overpass-api.de/api/interpreter"

const variables = (;
    building  = "Buildings (residential, commercial, etc.)",
    highway   = "Roads, paths, and streets",
    natural   = "Natural features (water, wood, etc.)",
    amenity   = "Amenities (restaurants, schools, hospitals, etc.)",
    shop      = "Shops and retail",
    leisure   = "Leisure areas (parks, playgrounds, etc.)",
    landuse   = "Land use zones (residential, industrial, etc.)",
    waterway  = "Waterways (rivers, streams, canals)",
    railway   = "Railway infrastructure",
    boundary  = "Administrative boundaries",
    tourism   = "Tourism features (hotels, attractions, etc.)",
    power     = "Power infrastructure (lines, substations, etc.)",
)

const metadata = MetaData(
    "", "Informal (be respectful)",
    INFRASTRUCTURE, variables,
    VECTOR_FEATURE, "Varies (individual features)", "Global",
    TemporalType.snapshot, nothing, "Continuously updated",
    "ODbL 1.0",
    "https://wiki.openstreetmap.org/wiki/Overpass_API";
    load_packages = Dict("DataFrames" => "a93c6f00-e57d-5684-b7b6-d8193f3e46c0"),
    default_retention = Day(7),
)

GeoDataAccess.MetaData(::Source) = metadata

#--------------------------------------------------------------------------------# DataAccessPlan

function GeoDataAccess.DataAccessPlan(source::Source, extent;
                                       variables::Vector{Symbol} = [:building],
                                       timeout::Int = 90,
                                       retention::Union{Nothing, Dates.Period} = metadata.default_retention)
    south, west, north, east = _bbox(extent)

    requests = RequestInfo[]
    for var in variables
        query = _overpass_query(var, south, west, north, east; timeout)
        url = _build_url(URL, Dict("data" => query))
        push!(requests, RequestInfo(source, url, HTTPMethod.GET, "OSM $var"))
    end

    extent_desc = _describe_extent(extent)
    area_deg2 = max((north - south) * (east - west), 0.001)
    est_bytes = round(Int, area_deg2 * 500_000 * length(variables))

    DataAccessPlan(source, requests, extent_desc,
        nothing, variables, Dict{Symbol, Any}(:timeout => timeout),
        est_bytes, retention)
end

#--------------------------------------------------------------------------------# Helpers

function _overpass_query(tag::Symbol, south, west, north, east; timeout=90)
    "[out:json][timeout:$timeout];(nwr[\"$tag\"]($south,$west,$north,$east););out center body qt;"
end

function _bbox(extent)
    trait = GI.geomtrait(extent)
    _bbox(trait, extent)
end

function _bbox(::GI.PointTrait, geom)
    lon, lat = GI.x(geom), GI.y(geom)
    (lat - 0.005, lon - 0.005, lat + 0.005, lon + 0.005)  # S, W, N, E
end

function _bbox(::GI.AbstractPolygonTrait, geom)
    _bbox(nothing, GI.extent(geom))
end

function _bbox(::Nothing, geom)
    if hasproperty(geom, :X) && hasproperty(geom, :Y)
        xmin, xmax = geom.X
        ymin, ymax = geom.Y
        return (ymin, xmin, ymax, xmax)  # S, W, N, E
    end
    error("Cannot extract bounding box from $(typeof(geom)) for OpenStreetMap.")
end

function _bbox(::Union{GI.MultiPointTrait, GI.AbstractCurveTrait}, geom)
    points = collect(GI.getpoint(geom))
    lons = [GI.x(p) for p in points]
    lats = [GI.y(p) for p in points]
    (minimum(lats), minimum(lons), maximum(lats), maximum(lons))  # S, W, N, E
end

_register_source!(Source())

end # module OpenStreetMap
