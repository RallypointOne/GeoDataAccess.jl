module GeoDataAccess

using Dates
using Downloads
using EnumX
using JSON3
using URIs

import GeoInterface as GI

#--------------------------------------------------------------------------------# Cache
module Cache
using Scratch
using Dates
using TOML

const RETENTION_FILE = "__retention__.toml"

dir(x...) = joinpath(@get_scratch!("geodataaccess_cache"), x...)
clear!(x...) = rm(dir(x...); force=true, recursive=true)

function list()
    isdir(dir()) || return String[]
    all = [joinpath(root, f) for (root, _, files) in walkdir(dir()) for f in files]
    filter(f -> isfile(f) && basename(f) != RETENTION_FILE, all)
end

#--------------------------------------------------------------------------------# Retention TOML

function _retention_path()
    dir(RETENTION_FILE)
end

function _read_toml()
    p = _retention_path()
    isfile(p) ? TOML.parsefile(p) : Dict{String, Any}()
end

function _write_toml(data::Dict)
    p = _retention_path()
    mkpath(dirname(p))
    open(p, "w") do io
        TOML.print(io, data)
    end
end

function _relative_key(path::String)
    relpath(path, dir())
end

function _write_retention(path::String, retention::Period)
    data = _read_toml()
    key = _relative_key(path)
    data[key] = Dict{String, Any}(
        "retention" => Dates.value(retention),
        "retention_unit" => string(nameof(typeof(retention))),
    )
    _write_toml(data)
end

function _read_retention(path::String)
    data = _read_toml()
    key = _relative_key(path)
    haskey(data, key) || return nothing
    entry = data[key]
    try
        T = getfield(Dates, Symbol(entry["retention_unit"]))
        return T(entry["retention"])
    catch
        return nothing
    end
end

function _remove_retention(path::String)
    data = _read_toml()
    key = _relative_key(path)
    delete!(data, key)
    _write_toml(data)
end

#--------------------------------------------------------------------------------# table

"""
    table() -> Vector{NamedTuple}

Return a vector of `NamedTuple`s describing every cached file.  Each entry has:

- `path::String` — absolute file path
- `size::Int` — file size in bytes
- `retention::Union{Nothing, Int}` — retention value (e.g., `7`)
- `retention_unit::Union{Nothing, Type{<:Period}}` — period type (e.g., `Day`)
"""
function table()
    files = list()
    data = _read_toml()
    map(files) do f
        key = _relative_key(f)
        entry = get(data, key, nothing)
        r = if !isnothing(entry)
            try
                T = getfield(Dates, Symbol(entry["retention_unit"]))
                T(entry["retention"])
            catch
                nothing
            end
        else
            nothing
        end
        (
            path = f,
            size = filesize(f),
            retention = isnothing(r) ? nothing : Dates.value(r),
            retention_unit = isnothing(r) ? nothing : typeof(r),
        )
    end
end

#--------------------------------------------------------------------------------# cleanup!

"""
    cleanup!() -> NamedTuple{(:removed, :freed), Tuple{Int, Int}}

Remove cached files whose age exceeds their per-source retention period.
Returns the number of files removed and bytes freed.
"""
function cleanup!()
    files = list()
    removed = 0
    freed = 0
    now_dt = now()
    data = _read_toml()

    for f in files
        key = _relative_key(f)
        entry = get(data, key, nothing)
        isnothing(entry) && continue

        r = try
            T = getfield(Dates, Symbol(entry["retention_unit"]))
            T(entry["retention"])
        catch
            continue
        end

        file_dt = Dates.unix2datetime(mtime(f))
        if file_dt < now_dt - r
            sz = filesize(f)
            rm(f; force=true)
            delete!(data, key)
            removed += 1
            freed += sz
        end
    end

    _write_toml(data)
    _remove_empty_dirs!(dir())
    (removed=removed, freed=freed)
end

function _remove_empty_dirs!(path)
    isdir(path) || return
    for entry in readdir(path; join=true)
        isdir(entry) && _remove_empty_dirs!(entry)
    end
    isempty(readdir(path)) && rm(path; force=true)
end

end  # Cache module

#--------------------------------------------------------------------------------# AbstractDataSource
abstract type AbstractDataSource end

#--------------------------------------------------------------------------------# Enums

@enum Domain WEATHER TERRAIN AIR_QUALITY HYDROLOGY NATURAL_HAZARDS INFRASTRUCTURE

@enum SpatialType RASTER POINT VECTOR_FEATURE

@enumx TemporalType timeseries snapshot climatology forecast

@enumx HTTPMethod GET POST

@enumx Frequency hourly daily

@enumx QueryType point regional multi_point

@enumx EPAService sampleData dailyData annualData

@enumx HRRRLevel sfc prs

@enumx HRRRRunType anl fcst

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
    domain::Domain
    variables::NamedTuple
    spatial_type::SpatialType
    spatial_resolution::String                  # "25 km", "30 m", "station-based"
    coverage::String                            # "Global", "US", "Europe"
    temporal_type::TemporalType.T
    temporal_resolution::Union{Dates.Period, Nothing}  # nothing for static data
    temporal_extent::String                     # "1940-present", "2020", "N/A"
    license::String                             # "CC BY 4.0", "Public Domain", etc.
    docs_url::String                            # URL to API documentation
    load_packages::Dict{String, String}         # name => uuid of packages needed by `load`
    default_retention::Union{Nothing, Dates.Period}  # default cache retention for this source
end

function MetaData(api_key_env_var, rate_limit, domain, variables, spatial_type,
                  spatial_resolution, coverage, temporal_type, temporal_resolution,
                  temporal_extent, license, docs_url;
                  load_packages::Dict{String, String} = Dict{String, String}(),
                  default_retention::Union{Nothing, Dates.Period} = nothing)
    MetaData(api_key_env_var, rate_limit, domain, variables, spatial_type,
             spatial_resolution, coverage, temporal_type, temporal_resolution,
             temporal_extent, license, docs_url, load_packages, default_retention)
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
    method::HTTPMethod.T
    description::String
    cache_path::String
end

RequestInfo(source::AbstractDataSource, url::String, method::HTTPMethod.T, description::String; ext::String=".json") =
    RequestInfo(url, method, description, _cache_path(name(typeof(source)), url; ext))

function Base.show(io::IO, r::RequestInfo)
    short_path = replace(r.cache_path, r"^.*?(/geodataaccess_cache/)" => "")
    cached = isfile(r.cache_path) ? " (cached)" : ""
    print(io, "$(r.method) $(r.description) → $(short_path)$(cached)")
end

#--------------------------------------------------------------------------------# DataAccessPlan

"""
    DataAccessPlan

A plan for retrieving data from a source.  Created by calling `DataAccessPlan(source, extent, ...)`
without making network calls.  Inspect the plan, then call `fetch(plan)` to get file paths
or `load(plan)` to get parsed data.

### Examples

```julia
plan = DataAccessPlan(OpenMeteoArchive(), (-74.0, 40.7), Date(2023,1,1), Date(2023,1,3))
plan          # inspect
fetch(plan)   # download → file paths
load(plan)    # download → parsed data
```
"""
struct DataAccessPlan{S<:AbstractDataSource}
    source::S
    requests::Vector{RequestInfo}
    extent_description::String
    time_range::Union{Nothing, Tuple{Date, Date}}
    variables::Vector{Symbol}
    kwargs::Dict{Symbol, Any}
    estimated_bytes::Int
    retention::Union{Nothing, Dates.Period}
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
    if !isnothing(plan.retention)
        println(io, "  Retention: ", plan.retention)
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
    _request_headers(source::AbstractDataSource) -> Vector{Pair{String,String}}

Return extra HTTP headers needed for a source (e.g. API key headers).  Default is no headers.
"""
_request_headers(::AbstractDataSource) = Pair{String,String}[]

"""
    fetch(plan::DataAccessPlan) -> Vector{String}

Execute a `DataAccessPlan`, downloading the planned requests and returning local file paths.
"""
function fetch(plan::DataAccessPlan)
    src_name = name(typeof(plan.source))
    hdrs = _request_headers(plan.source)
    [_cached_get(src_name, req.url; headers=hdrs, ext=splitext(req.cache_path)[2], retention=plan.retention) for req in plan.requests]
end

"""
    load(plan::DataAccessPlan)

Execute a `DataAccessPlan`, downloading and parsing the data.  Requires a package extension —
install the packages listed in `MetaData(source).load_packages` to enable `load` for a source.
"""
function load(plan::DataAccessPlan)
    src = plan.source
    pkgs = MetaData(src).load_packages
    src_name = name(typeof(src))
    if isempty(pkgs)
        error("`load` is not implemented for $src_name. Use `fetch(plan)` to get file paths.")
    end
    pkg_list = join(["Pkg.add(\"$k\")" for k in keys(pkgs)], "; ")
    error("`load` for $src_name requires a package extension. Install the required packages:\n\n" *
          "  using Pkg; $pkg_list\n\n" *
          "Then `using` the package(s) before calling `load`.")
end

function fetch(source::AbstractDataSource, extent, start_date::Date, stop_date::Date; kw...)
    fetch(DataAccessPlan(source, extent, start_date, stop_date; kw...))
end

function fetch(source::AbstractDataSource, extent; kw...)
    fetch(DataAccessPlan(source, extent; kw...))
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
function all_sources(; domain::Union{Nothing, Domain} = nothing)
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

function _cache_path(source_name::String, url::String; ext::String=".json")
    h = string(hash(url), base=16)
    Cache.dir(source_name, "$h$ext")
end

function _cached_get(source_name::String, url::String;
                     headers=Pair{String,String}[], ext::String=".json", retries::Int=3,
                     retention::Union{Nothing, Dates.Period}=nothing)
    cache_path = _cache_path(source_name, url; ext)
    isfile(cache_path) && return cache_path
    local last_err
    for attempt in 1:retries
        try
            mkpath(dirname(cache_path))
            Downloads.download(url, cache_path; headers, timeout=120)
            if !isnothing(retention)
                Cache._write_retention(cache_path, retention)
            end
            return cache_path
        catch e
            last_err = e
            attempt < retries && @warn "Download attempt $attempt/$retries failed, retrying..." url exception=e
        end
    end
    throw(last_err)
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

#--------------------------------------------------------------------------------# Lazy Loading
include("lazy.jl")

#--------------------------------------------------------------------------------# Sources
include("sources/open_meteo.jl")
include("sources/noaa_ncei.jl")
include("sources/nasa_power.jl")
include("sources/tomorrow_io.jl")
include("sources/visual_crossing.jl")
include("sources/usgs_earthquake.jl")
include("sources/usgs_waterservices.jl")
include("sources/openaq.jl")
include("sources/nasa_firms.jl")
include("sources/epa_aqs.jl")
include("sources/noaa_gfs.jl")
include("sources/era5.jl")
include("sources/copernicus_dem.jl")
include("sources/openstreetmap.jl")
include("sources/noaa_oisst.jl")

#--------------------------------------------------------------------------------# Zarr Sources
include("zarr/common.jl")
include("zarr/arco_era5.jl")
include("zarr/nasa_power.jl")
include("zarr/gfs.jl")
include("zarr/hrrr.jl")

end # module
