#--------------------------------------------------------------------------------# Copernicus DEM

module CopernicusDEM

import ..GeoDataAccess
using ..GeoDataAccess: AbstractDataSource, MetaData, DataAccessPlan, RequestInfo,
    _register_source!, _describe_extent,
    TERRAIN, RASTER, TemporalType, HTTPMethod
using Dates
import GeoInterface as GI

"""
    CopernicusDEM.Source(; resolution=30)

Copernicus DEM elevation data from [AWS Open Data](https://registry.opendata.aws/copernicus-dem/).
Two resolutions: `30` (~30 m) or `90` (~90 m).

- **Coverage**: Global
- **Format**: Cloud Optimized GeoTIFF (COG)
- **API Key**: None required
- **Temporal**: Static (snapshot)

### Examples

```julia
using GeoInterface.Extents: Extent

plan = DataAccessPlan(CopernicusDEM.Source(), (-105.0, 40.0))
files = fetch(plan)

plan = DataAccessPlan(CopernicusDEM.Source(resolution=90),
    Extent(X=(-106.0, -104.0), Y=(39.0, 41.0)))
files = fetch(plan)
```
"""
struct Source <: AbstractDataSource
    resolution::Int
    function Source(; resolution::Int=30)
        resolution in (30, 90) || error("resolution must be 30 or 90 (got $resolution)")
        new(resolution)
    end
end

GeoDataAccess.name(::Type{Source}) = "copernicusdem"

const variables = (;
    elevation = "Elevation above sea level (m)",
)

function GeoDataAccess.MetaData(source::Source)
    MetaData(
        "", "None (AWS S3)",
        TERRAIN, variables,
        RASTER, "$(source.resolution) m", "Global",
        TemporalType.snapshot, nothing, "Static",
        "Copernicus License",
        "https://copernicus-dem-$(source.resolution)m.s3.amazonaws.com/readme.html";
        load_packages = Dict("Rasters" => "a3a2b9e3-a471-40c9-b274-f788e487c689"),
    )
end

#--------------------------------------------------------------------------------# DataAccessPlan

function GeoDataAccess.DataAccessPlan(source::Source, extent;
                                       variables::Vector{Symbol} = [:elevation],
                                       retention::Union{Nothing, Dates.Period} = GeoDataAccess.MetaData(source).default_retention)
    lons, lats = _tile_range(extent)
    requests = RequestInfo[]
    for lat in lats, lon in lons
        url = _tile_url(source.resolution, lat, lon)
        desc = "DEM tile $(lat >= 0 ? "N" : "S")$(lpad(abs(lat), 2, '0'))_$(lon >= 0 ? "E" : "W")$(lpad(abs(lon), 3, '0'))"
        push!(requests, RequestInfo(source, url, HTTPMethod.GET, desc; ext=".tif"))
    end
    extent_desc = _describe_extent(extent)
    est_bytes = source.resolution == 30 ? 25_000_000 : 3_000_000
    DataAccessPlan(source, requests, extent_desc,
        nothing, variables, Dict{Symbol, Any}(:resolution => source.resolution),
        length(requests) * est_bytes, retention)
end

#--------------------------------------------------------------------------------# Helpers

function _tile_url(resolution::Int, lat::Int, lon::Int)
    ns = lat >= 0 ? "N" : "S"
    ew = lon >= 0 ? "E" : "W"
    lat_str = "$(ns)$(lpad(abs(lat), 2, '0'))_00"
    lon_str = "$(ew)$(lpad(abs(lon), 3, '0'))_00"
    arcsec = resolution == 30 ? "10" : "30"
    bucket = "copernicus-dem-$(resolution)m"
    tile = "Copernicus_DSM_COG_$(arcsec)_$(lat_str)_$(lon_str)_DEM"
    return "https://$(bucket).s3.eu-central-1.amazonaws.com/$tile/$tile.tif"
end

function _tile_range(extent)
    trait = GI.geomtrait(extent)
    _tile_range(trait, extent)
end

function _tile_range(::GI.PointTrait, geom)
    lon = floor(Int, GI.x(geom))
    lat = floor(Int, GI.y(geom))
    [lon], [lat]
end

function _tile_range(::GI.AbstractPolygonTrait, geom)
    _tile_range(nothing, GI.extent(geom))
end

function _tile_range(::Nothing, geom)
    if hasproperty(geom, :X) && hasproperty(geom, :Y)
        xmin, xmax = geom.X
        ymin, ymax = geom.Y
        lons = floor(Int, xmin):floor(Int, xmax)
        lats = floor(Int, ymin):floor(Int, ymax)
        return collect(lons), collect(lats)
    end
    error("Cannot extract bounding box from $(typeof(geom)) for Copernicus DEM.")
end

function _tile_range(::Union{GI.MultiPointTrait, GI.AbstractCurveTrait}, geom)
    points = collect(GI.getpoint(geom))
    lon_vals = [GI.x(p) for p in points]
    lat_vals = [GI.y(p) for p in points]
    lons = floor(Int, minimum(lon_vals)):floor(Int, maximum(lon_vals))
    lats = floor(Int, minimum(lat_vals)):floor(Int, maximum(lat_vals))
    collect(lons), collect(lats)
end

_register_source!(Source())

end # module CopernicusDEM
