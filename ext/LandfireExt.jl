module LandfireExt

using GeoDataAccess
using Landfire

import GeoDataAccess: AbstractDataSource, DataAccessPlan, MetaData, RequestInfo,
    _register_source!, name, _describe_extent, _estimate_bytes

import Dates: Day
import GeoInterface as GI

#--------------------------------------------------------------------------------# LandfireSource

"""
    LandfireSource()

LANDFIRE (Landscape Fire and Resource Management Planning Tools) geospatial data via the
[LFPS API](https://lfps.usgs.gov/).  Provides fuel models, vegetation, topography, and
disturbance layers at 30 m resolution for CONUS, Alaska, and Hawaii.

- **Coverage**: CONUS, Alaska, Hawaii
- **Resolution**: 30 m
- **API Key**: Not required (email via `LANDFIRE_EMAIL` env var)
- **Workflow**: Async job (submit → poll → download ZIP → extract GeoTIFF)

Requires `Landfire.Product` objects via the `products` keyword argument.  Use
`Landfire.products()` to browse available products.

### Examples

```julia
using GeoDataAccess, Landfire
using GeoInterface.Extents: Extent

prods = Landfire.products(layer="FBFM40")
ext = Extent(X=(-107.7, -106.0), Y=(46.5, 47.3))

plan = DataAccessPlan(LandfireSource(), ext; products=prods)
files = fetch!(plan)  # submits job, polls, downloads, extracts → returns [tif_path]
```
"""
struct LandfireSource <: AbstractDataSource end

function __init__()
    _register_source!(LandfireSource())
end

const LANDFIRE_VARIABLES = Dict{Symbol, String}(
    :FBFM13  => "13 Anderson Fire Behavior Fuel Models",
    :FBFM40  => "40 Scott & Burgan Fire Behavior Fuel Models",
    :CC      => "Forest Canopy Cover (%)",
    :CH      => "Forest Canopy Height (m)",
    :CBD     => "Forest Canopy Bulk Density (kg/m³)",
    :CBH     => "Forest Canopy Base Height (m)",
    :EVT     => "Existing Vegetation Type",
    :EVC     => "Existing Vegetation Cover",
    :EVH     => "Existing Vegetation Height",
    :BPS     => "Biophysical Settings",
    :ELEV    => "Elevation (m)",
    :SLP     => "Slope (degrees)",
    :ASP     => "Aspect (degrees)",
    :FDIST   => "Fuel Disturbance",
)

#--------------------------------------------------------------------------------# MetaData

MetaData(::LandfireSource) = MetaData(
    "", "Async job queue",
    :terrain, LANDFIRE_VARIABLES,
    :raster, "30 m", "CONUS, AK, HI",
    :snapshot, nothing, "Multi-year (2001–2024)",
    "Public Domain",
    "https://landfire.gov/",
)

#--------------------------------------------------------------------------------# DataAccessPlan

function DataAccessPlan(source::LandfireSource, extent;
                        products::Vector{Landfire.Product} = Landfire.Product[],
                        email::String = get(ENV, "LANDFIRE_EMAIL", ""),
                        output_projection::Union{Nothing, String} = nothing,
                        resample_resolution::Union{Nothing, Int} = nothing)
    isempty(products) && error("LandfireSource requires `products` keyword. " *
                               "Use `Landfire.products()` to browse available products.")
    isempty(email) && error("Set LANDFIRE_EMAIL environment variable or pass `email` keyword.")

    aoi = Landfire.area_of_interest(extent)
    extent_desc = _describe_extent(extent)

    # Store Landfire.Job in kwargs for fetch! to use
    job = Landfire.Job(products, extent;
        email, output_projection, resample_resolution)

    kwargs = Dict{Symbol, Any}(
        :products => [p.layer for p in products],
        :job => job,
    )
    !isnothing(output_projection) && (kwargs[:output_projection] = output_projection)
    !isnothing(resample_resolution) && (kwargs[:resample_resolution] = resample_resolution)

    # No traditional requests — the async job replaces them
    DataAccessPlan(source, RequestInfo[], extent_desc,
        nothing, [Symbol(p.layer) for p in products], kwargs,
        0)
end

#--------------------------------------------------------------------------------# fetch!

function GeoDataAccess.fetch!(plan::DataAccessPlan{LandfireSource})
    job = plan.kwargs[:job]::Landfire.Job
    data = Landfire.Dataset(job.layers, job.area_of_interest;
        email=job.email,
        output_projection=job.output_projection,
        resample_resolution=job.resample_resolution)
    tif = Base.get(data)
    [tif]
end

end # module
