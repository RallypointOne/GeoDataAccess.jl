#--------------------------------------------------------------------------------# AbstractZarrSource
#
# Base type for data sources backed by remote Zarr stores.  Unlike download-based sources,
# Zarr sources provide lazy, chunk-on-demand access — no files are downloaded upfront.
#
# Each Zarr source defines:
#   - `store_url(source)` — the URL/path to the Zarr store
#   - `MetaData(source)` — standard metadata
#
# Opening a store requires `Zarr.jl` (and possibly cloud storage packages).
# Use `store_url(source)` to get the URL, then open it with `Zarr.zopen`.

"""
    AbstractZarrSource <: AbstractDataSource

Base type for data sources backed by remote Zarr stores.  These sources provide lazy,
chunk-on-demand access to cloud-hosted datasets without downloading files.

Use `store_url(source)` to get the Zarr store URL, then open it with `Zarr.zopen`:

```julia
using Zarr
url = store_url(ARCOERA5.Source())
store = zopen(url, consolidated=true)
store["2m_temperature"]  # lazy ZArray
```
"""
abstract type AbstractZarrSource <: AbstractDataSource end

"""
    store_url(source::AbstractZarrSource) -> String

Return the URL or cloud path to the Zarr store for this source.
"""
function store_url end
