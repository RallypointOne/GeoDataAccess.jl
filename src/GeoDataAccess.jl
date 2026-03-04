module GeoDataAccess

using Dates
using Downloads
using URIs

import GeoInterface as GI

#--------------------------------------------------------------------------------# Cache
module Cache
using Scratch
const ENABLED = Ref(true)
dir(x...) = joinpath(@get_scratch!("geodataaccess_cache"), x...)
enable!(x = true) = (ENABLED[] = x)
clear!(x...) = rm(dir(x...); force=true, recursive=true)
list() = filter(isfile, [joinpath(root, f) for (root, _, files) in walkdir(dir()) for f in files])
end  # Cache module

#--------------------------------------------------------------------------------# AbstractDataSource
abstract type AbstractDataSource end

"""
    name(::Type{<:AbstractDataSource}) -> String

Lowercase name of the data source type, used as cache directory key and display name.
"""
name(::Type{T}) where {T <: AbstractDataSource} = lowercase(string(T.name.name))

#--------------------------------------------------------------------------------# MetaData

"""
    MetaData

Describes a data source's capabilities, access requirements, and data characteristics.
"""
struct MetaData
    api_key_env_var::String                     # "" if no key needed
    rate_limit::String                          # human-readable, e.g., "10,000/day"
    domain::Symbol                              # :weather, :terrain, :landcover, :ocean, :soil, :air_quality, :hydrology, :satellite, :socioeconomic
    variables::Dict{Symbol, String}             # variable_name => description
    spatial_type::Symbol                        # :raster, :point, :vector
    spatial_resolution::String                  # "25 km", "30 m", "station-based"
    coverage::String                            # "Global", "US", "Europe"
    temporal_type::Symbol                       # :timeseries, :snapshot, :climatology, :forecast
    temporal_resolution::Union{Dates.Period, Nothing}  # nothing for static data
    temporal_extent::String                     # "1940-present", "2020", "N/A"
    license::String                             # "CC BY 4.0", "Public Domain"
    docs_url::String                            # URL to API documentation
end

has_api_key(source::AbstractDataSource) = !isempty(MetaData(source).api_key_env_var)

function is_available(source::AbstractDataSource)
    meta = MetaData(source)
    isempty(meta.api_key_env_var) || haskey(ENV, meta.api_key_env_var)
end

function _get_api_key(source::AbstractDataSource)
    meta = MetaData(source)
    env_var = meta.api_key_env_var
    isempty(env_var) && return ""
    key = get(ENV, env_var, "")
    isempty(key) && error("$env_var environment variable not set (required for $(name(typeof(source))))")
    key
end

#--------------------------------------------------------------------------------# RequestInfo

"""
    RequestInfo

Describes a single HTTP request that will be made as part of a `DataAccessPlan`.
"""
struct RequestInfo
    url::String
    method::Symbol
    description::String
    cache_path::String
end

RequestInfo(source::AbstractDataSource, url::String, method::Symbol, description::String) =
    RequestInfo(url, method, description, _cache_path(name(typeof(source)), url))

function Base.show(io::IO, r::RequestInfo)
    short_path = replace(r.cache_path, r"^.*?(/geodataaccess_cache/)" => "")
    cached = isfile(r.cache_path) ? " (cached)" : ""
    print(io, "$(r.method) $(r.description) → $(short_path)$(cached)")
end

#--------------------------------------------------------------------------------# DataAccessPlan

"""
    DataAccessPlan

A plan for retrieving data from a source.  Created by calling `DataAccessPlan(source, extent, ...)`
without making network calls.  Inspect the plan, then call `fetch!(plan)` to execute.

### Examples

```julia
plan = DataAccessPlan(OpenMeteoArchive(), (-74.0, 40.7), Date(2023,1,1), Date(2023,1,3))
plan          # inspect
fetch!(plan)  # execute
```
"""
struct DataAccessPlan
    source::AbstractDataSource
    requests::Vector{RequestInfo}
    extent_description::String
    time_range::Union{Nothing, Tuple{Date, Date}}
    variables::Vector{Symbol}
    kwargs::Dict{Symbol, Any}
    estimated_bytes::Int
end

function Base.show(io::IO, ::MIME"text/plain", plan::DataAccessPlan)
    println(io, "DataAccessPlan for ", name(typeof(plan.source)))
    println(io, "  Extent:    ", plan.extent_description)
    if !isnothing(plan.time_range)
        s, e = plan.time_range
        days = Dates.value(e - s) + 1
        println(io, "  Time:      $s to $e ($days days)")
    end
    println(io, "  Variables: ", join(plan.variables, ", "))
    if !isempty(plan.kwargs)
        for (k, v) in plan.kwargs
            println(io, "  $(k): $v")
        end
    end
    println(io, "  API calls: ", length(plan.requests))
    println(io, "  Est. size: ", Base.format_bytes(plan.estimated_bytes))
    println(io)
    for (i, req) in enumerate(plan.requests)
        print(io, "  Request $i: ", req)
        i < length(plan.requests) && println(io)
    end
end

"""
    fetch!(plan::DataAccessPlan) -> Vector{String}

Execute a `DataAccessPlan`, downloading the planned requests and returning file paths.
"""
function fetch!(plan::DataAccessPlan)
    src_name = name(typeof(plan.source))
    [_cached_get(src_name, req.url) for req in plan.requests]
end

"""
    fetch_data(source, extent, [start_date, stop_date]; kw...) -> Vector{String}

Convenience function that creates a `DataAccessPlan` and immediately executes it.
Returns file paths to the downloaded data.
"""
function fetch_data(source::AbstractDataSource, extent, start_date::Date, stop_date::Date; kw...)
    plan = DataAccessPlan(source, extent, start_date, stop_date; kw...)
    fetch!(plan)
end

function fetch_data(source::AbstractDataSource, extent; kw...)
    plan = DataAccessPlan(source, extent; kw...)
    fetch!(plan)
end

#--------------------------------------------------------------------------------# Source Registry

const SOURCES = AbstractDataSource[]

function _register_source!(source::AbstractDataSource)
    push!(SOURCES, source)
end

"""
    all_sources(; domain=nothing) -> Vector{AbstractDataSource}

Return all registered data sources, optionally filtered by domain.
"""
function all_sources(; domain::Union{Nothing, Symbol} = nothing)
    isnothing(domain) && return copy(SOURCES)
    filter(s -> MetaData(s).domain == domain, SOURCES)
end

"""
    available_sources() -> Vector{AbstractDataSource}

Return data sources that are currently usable (no API key required, or key is set in ENV).
"""
available_sources() = filter(is_available, SOURCES)

#--------------------------------------------------------------------------------# Shared Utilities

function _build_url(base::String, params::AbstractDict)
    string(URI(base; query = escapeuri(params)))
end

function _cache_path(source_name::String, url::String)
    h = string(hash(url), base=16)
    Cache.dir(source_name, "$h.json")
end

function _cached_get(source_name::String, url::String)
    cache_path = _cache_path(source_name, url)
    if Cache.ENABLED[] && isfile(cache_path)
        return cache_path
    end
    if Cache.ENABLED[]
        mkpath(dirname(cache_path))
        Downloads.download(url, cache_path)
        return cache_path
    else
        return Downloads.download(url)
    end
end


function _describe_extent(extent)
    trait = GI.geomtrait(extent)
    _describe_extent(trait, extent)
end

function _describe_extent(::GI.PointTrait, geom)
    "Point($(GI.y(geom)), $(GI.x(geom)))"
end

function _describe_extent(::GI.MultiPointTrait, geom)
    n = length(collect(GI.getpoint(geom)))
    "$n points"
end

function _describe_extent(::GI.AbstractCurveTrait, geom)
    n = length(collect(GI.getpoint(geom)))
    "LineString ($n vertices)"
end

function _describe_extent(::GI.AbstractPolygonTrait, geom)
    ext = GI.extent(geom)
    _describe_extent(nothing, ext)
end

function _describe_extent(::Nothing, geom)
    if hasproperty(geom, :X) && hasproperty(geom, :Y)
        xmin, xmax = geom.X
        ymin, ymax = geom.Y
        return "Extent(X=($xmin, $xmax), Y=($ymin, $ymax))"
    end
    "$(typeof(geom))"
end

_estimate_bytes(rows, n_vars) = rows * max(n_vars, 1) * 8


#--------------------------------------------------------------------------------# GeoInterface Extent Utilities

function _extent_to_latlon(extent)
    trait = GI.geomtrait(extent)
    _extent_to_latlon(trait, extent)
end

function _extent_to_latlon(::GI.PointTrait, geom)
    string(GI.y(geom)), string(GI.x(geom))
end

function _extent_to_latlon(::GI.MultiPointTrait, geom)
    lats = join([string(GI.y(p)) for p in GI.getpoint(geom)], ",")
    lons = join([string(GI.x(p)) for p in GI.getpoint(geom)], ",")
    lats, lons
end

function _extent_to_latlon(::GI.AbstractCurveTrait, geom)
    lats = join([string(GI.y(p)) for p in GI.getpoint(geom)], ",")
    lons = join([string(GI.x(p)) for p in GI.getpoint(geom)], ",")
    lats, lons
end

function _extent_to_latlon(::GI.AbstractPolygonTrait, geom)
    _extent_to_latlon(GI.extent(geom))
end

function _extent_to_latlon(::Nothing, geom)
    if hasproperty(geom, :X) && hasproperty(geom, :Y)
        xmin, xmax = geom.X
        ymin, ymax = geom.Y
        lats = join(string.([ymin, ymin, ymax, ymax]), ",")
        lons = join(string.([xmin, xmax, xmin, xmax]), ",")
        return lats, lons
    end
    error("Cannot extract coordinates from $(typeof(geom)). " *
          "Pass a GeoInterface-compatible geometry (Point, MultiPoint, LineString, Polygon) " *
          "or an Extents.Extent.")
end

function _count_points(extent)
    trait = GI.geomtrait(extent)
    _count_points(trait, extent)
end

_count_points(::GI.PointTrait, _) = 1
_count_points(::GI.MultiPointTrait, geom) = length(collect(GI.getpoint(geom)))
_count_points(::GI.AbstractCurveTrait, geom) = length(collect(GI.getpoint(geom)))
_count_points(::GI.AbstractPolygonTrait, _) = 4

function _count_points(::Nothing, geom)
    if hasproperty(geom, :X) && hasproperty(geom, :Y)
        return 4
    end
    error("Cannot count points for $(typeof(geom))")
end

#--------------------------------------------------------------------------------# Sources
include("sources/open_meteo.jl")
include("sources/noaa_ncei.jl")
include("sources/nasa_power.jl")
include("sources/tomorrow_io.jl")
include("sources/visual_crossing.jl")

end # module
