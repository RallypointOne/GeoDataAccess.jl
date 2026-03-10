#--------------------------------------------------------------------------------# LazyData
#
# A lazy wrapper around a DataAccessPlan.  Each "chunk" is one request (typically one file).
# Chunks are fetched on first access and cached, so subsequent reads are free.
#
# This emulates Zarr-style lazy loading for any data source: instead of downloading all
# files upfront with `fetch(plan)`, a `LazyData` object defers downloads until the data
# is actually indexed.
#
#   plan = DataAccessPlan(NOAAOISST(), extent, d1, d2)
#   data = LazyData(plan)        # no downloads yet
#   data[3]                      # fetches & reads only request 3
#   data[1:5]                    # fetches requests 1–5 (skips already-cached ones)
#
# The `readfn` argument controls how a downloaded file is turned into data.  It receives
# the file path and should return whatever data representation you want (Array, DataFrame,
# Dict, raw bytes, etc.).  The default reads raw bytes.

"""
    LazyData(plan; readfn=read)

A lazy, chunk-oriented view over a `DataAccessPlan`.  Each request in the plan is one chunk.
Chunks are downloaded and cached on first access.

- `plan` — the `DataAccessPlan` describing what to download
- `readfn` — function `(filepath::String) -> data` applied after download (default: `read`)

### Indexing

- `data[i]` — fetch and read chunk `i`
- `data[1:5]` — fetch and read chunks 1 through 5 (returns a `Vector`)

Already-cached files are not re-downloaded.

### Examples

```julia
plan = DataAccessPlan(NOAAOISST(), (-74.0, 40.7),
    Date(2024, 1, 1), Date(2024, 1, 7))

# Raw bytes (default)
data = LazyData(plan)
bytes = data[1]  # downloads day 1, returns raw bytes

# Custom reader
data = LazyData(plan, readfn=JSON3.read ∘ read)
parsed = data[1]

# With Rasters.jl
using Rasters
data = LazyData(plan, readfn=Raster)
raster = data[3]  # downloads day 3, returns a Raster
```
"""
struct LazyData{S<:AbstractDataSource, F}
    plan::DataAccessPlan{S}
    readfn::F
    cache::Vector{Any}       # per-chunk result cache (nothing = not yet read)
end

function LazyData(plan::DataAccessPlan{S}; readfn::F=read) where {S, F}
    cache = Vector{Any}(nothing, length(plan.requests))
    LazyData{S, F}(plan, readfn, cache)
end

Base.length(d::LazyData) = length(d.plan.requests)
Base.size(d::LazyData) = (length(d),)
Base.keys(d::LazyData) = Base.OneTo(length(d))
Base.firstindex(d::LazyData) = 1
Base.lastindex(d::LazyData) = length(d)

function Base.show(io::IO, ::MIME"text/plain", d::LazyData)
    n = length(d)
    n_loaded = count(!isnothing, d.cache)
    n_cached = count(r -> isfile(r.cache_path), d.plan.requests)
    src = name(typeof(d.plan.source))
    println(io, "LazyData for $src ($n chunks, $n_loaded loaded, $n_cached on disk)")
    println(io, "  Extent:    ", d.plan.extent_description)
    if !isnothing(d.plan.time_range)
        s, e = d.plan.time_range
        println(io, "  Time:      $s to $e")
    end
    print(io, "  Variables: ", join(d.plan.variables, ", "))
end

#--------------------------------------------------------------------------------# Fetching a single chunk

function _fetch_chunk!(d::LazyData, i::Int)
    @boundscheck checkbounds(d.cache, i)
    # Return from in-memory cache if already loaded
    !isnothing(d.cache[i]) && return d.cache[i]
    # Download (or hit disk cache) then apply readfn
    req = d.plan.requests[i]
    src_name = name(typeof(d.plan.source))
    hdrs = _request_headers(d.plan.source)
    ext = splitext(req.cache_path)[2]
    path = _cached_get(src_name, req.url; headers=hdrs, ext, retention=d.plan.retention)
    result = d.readfn(path)
    d.cache[i] = result
    return result
end

#--------------------------------------------------------------------------------# Indexing

function Base.getindex(d::LazyData, i::Int)
    _fetch_chunk!(d, i)
end

function Base.getindex(d::LazyData, r::AbstractUnitRange{<:Integer})
    [_fetch_chunk!(d, i) for i in r]
end

function Base.getindex(d::LazyData, idxs::AbstractVector{<:Integer})
    [_fetch_chunk!(d, i) for i in idxs]
end

#--------------------------------------------------------------------------------# Iteration

Base.iterate(d::LazyData) = length(d) == 0 ? nothing : (d[1], 2)
Base.iterate(d::LazyData, i::Int) = i > length(d) ? nothing : (d[i], i + 1)
Base.eltype(::Type{<:LazyData}) = Any

#--------------------------------------------------------------------------------# Utilities

"""
    prefetch!(data::LazyData, idxs=keys(data))

Download (but don't read) the specified chunks in the background, populating the disk cache.
This is useful to trigger downloads ahead of time without paying the `readfn` cost.
"""
function prefetch!(d::LazyData, idxs=keys(d))
    src_name = name(typeof(d.plan.source))
    hdrs = _request_headers(d.plan.source)
    for i in idxs
        req = d.plan.requests[i]
        isfile(req.cache_path) && continue
        ext = splitext(req.cache_path)[2]
        _cached_get(src_name, req.url; headers=hdrs, ext, retention=d.plan.retention)
    end
    d
end

"""
    status(data::LazyData) -> NamedTuple

Return a summary of chunk states: how many are loaded in memory, cached on disk, or pending.
"""
function status(d::LazyData)
    n = length(d)
    loaded = count(!isnothing, d.cache)
    on_disk = count(r -> isfile(r.cache_path), d.plan.requests)
    (total=n, loaded=loaded, on_disk=on_disk, pending=n - on_disk)
end

"""
    reset!(data::LazyData)

Clear the in-memory cache.  Files on disk are not removed.  Useful to free memory after
processing chunks.
"""
function reset!(d::LazyData)
    fill!(d.cache, nothing)
    d
end
