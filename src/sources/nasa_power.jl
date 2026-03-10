#--------------------------------------------------------------------------------# NASA POWER

module NASAPower

import ..GeoDataAccess
using ..GeoDataAccess: AbstractDataSource, MetaData, DataAccessPlan, RequestInfo,
    _register_source!, _build_url, _describe_extent, _estimate_bytes,
    WEATHER, RASTER, TemporalType, HTTPMethod, QueryType
using Dates
import GeoInterface as GI

"""
    NASAPower.Source()

Global daily meteorological and solar energy data from [NASA POWER](https://power.larc.nasa.gov/).

- **Coverage**: Global, 1981–present
- **Resolution**: ~55 km (0.5° × 0.5°), daily
- **API Key**: Not required

### Examples

```julia
plan = DataAccessPlan(NASAPower.Source(), (-74.0, 40.7),
    Date(2024, 1, 1), Date(2024, 1, 7);
    variables = [:T2M, :PRECTOTCORR])
files = fetch(plan)
```
"""
struct Source <: AbstractDataSource end

GeoDataAccess.name(::Type{Source}) = "nasapower"

const POINT_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"
const REGIONAL_URL = "https://power.larc.nasa.gov/api/temporal/daily/regional"

const variables = (;
    T2M                = "Temperature at 2m (°C)",
    T2M_MAX            = "Maximum temperature at 2m (°C)",
    T2M_MIN            = "Minimum temperature at 2m (°C)",
    T2M_RANGE          = "Temperature range at 2m (°C)",
    T2MDEW             = "Dew/frost point at 2m (°C)",
    RH2M               = "Relative humidity at 2m (%)",
    PRECTOTCORR        = "Precipitation corrected (mm/day)",
    WS2M               = "Wind speed at 2m (m/s)",
    WS10M              = "Wind speed at 10m (m/s)",
    WD2M               = "Wind direction at 2m (°)",
    WD10M              = "Wind direction at 10m (°)",
    PS                 = "Surface pressure (kPa)",
    QV2M               = "Specific humidity at 2m (g/kg)",
    CLOUD_AMT          = "Cloud amount (%)",
    ALLSKY_SFC_SW_DWN  = "All-sky surface shortwave downward irradiance (MJ/m²/day)",
    CLRSKY_SFC_SW_DWN  = "Clear-sky surface shortwave downward irradiance (MJ/m²/day)",
    ALLSKY_SFC_LW_DWN  = "All-sky surface longwave downward irradiance (W/m²)",
)

const metadata = MetaData(
    "", "No published rate limit",
    WEATHER, variables,
    RASTER, "55 km", "Global",
    TemporalType.timeseries, Day(1), "1981-present",
    "Open Data (NASA)",
    "https://power.larc.nasa.gov/docs/services/api/";
    load_packages = Dict("DataFrames" => "a93c6f00-e57d-5684-b7b6-d8193f3e46c0"),
)

GeoDataAccess.MetaData(::Source) = metadata

#--------------------------------------------------------------------------------# DataAccessPlan

function GeoDataAccess.DataAccessPlan(source::Source, extent, start_date::Date, stop_date::Date;
                                       variables::Vector{Symbol} = [:T2M, :PRECTOTCORR],
                                       community::String = "AG",
                                       retention::Union{Nothing, Dates.Period} = metadata.default_retention)
    trait = GI.geomtrait(extent)
    _plan(source, trait, extent, start_date, stop_date, variables, community, retention)
end

function _plan(source::Source, ::GI.PointTrait, geom, start_date, stop_date, variables, community, retention)
    lat, lon = GI.y(geom), GI.x(geom)
    params = Dict{String, String}(
        "parameters" => join(string.(variables), ","),
        "community"  => community,
        "longitude"  => string(lon),
        "latitude"   => string(lat),
        "start"      => Dates.format(start_date, dateformat"yyyymmdd"),
        "end"        => Dates.format(stop_date, dateformat"yyyymmdd"),
        "format"     => "JSON",
    )
    url = _build_url(POINT_URL, params)
    n_days = Dates.value(stop_date - start_date) + 1
    request = RequestInfo(source, url, HTTPMethod.GET, "Point($lat, $lon), $n_days days")
    DataAccessPlan(source, [request], _describe_extent(GI.PointTrait(), geom),
        (start_date, stop_date), variables,
        Dict{Symbol, Any}(:community => community, :query_type => QueryType.point),
        _estimate_bytes(n_days, length(variables)), retention)
end

function _plan(source::Source, ::Nothing, geom, start_date, stop_date, variables, community, retention)
    if !(hasproperty(geom, :X) && hasproperty(geom, :Y))
        error("Cannot extract coordinates from $(typeof(geom)) for NASA POWER.")
    end
    xmin, xmax = geom.X
    ymin, ymax = geom.Y
    (xmax - xmin) < 2 && error("NASA POWER regional endpoint requires at least 2° longitude range (got $(xmax - xmin)°). Use a point query instead.")
    (ymax - ymin) < 2 && error("NASA POWER regional endpoint requires at least 2° latitude range (got $(ymax - ymin)°). Use a point query instead.")
    params = Dict{String, String}(
        "parameters"    => join(string.(variables), ","),
        "community"     => community,
        "longitude-min" => string(xmin),
        "longitude-max" => string(xmax),
        "latitude-min"  => string(ymin),
        "latitude-max"  => string(ymax),
        "start"         => Dates.format(start_date, dateformat"yyyymmdd"),
        "end"           => Dates.format(stop_date, dateformat"yyyymmdd"),
        "format"        => "JSON",
    )
    url = _build_url(REGIONAL_URL, params)
    n_days = Dates.value(stop_date - start_date) + 1
    request = RequestInfo(source, url, HTTPMethod.GET, "Regional bbox, $n_days days")
    DataAccessPlan(source, [request], _describe_extent(nothing, geom),
        (start_date, stop_date), variables,
        Dict{Symbol, Any}(:community => community, :query_type => QueryType.regional),
        _estimate_bytes(n_days, length(variables)), retention)
end

function _plan(source::Source, ::GI.AbstractPolygonTrait, geom, start_date, stop_date, variables, community, retention)
    _plan(source, nothing, GI.extent(geom), start_date, stop_date, variables, community, retention)
end

function _plan(source::Source, ::Union{GI.MultiPointTrait, GI.AbstractCurveTrait}, geom,
               start_date, stop_date, variables, community, retention)
    points = collect(GI.getpoint(geom))
    n_days = Dates.value(stop_date - start_date) + 1
    requests = RequestInfo[]
    for p in points
        lat, lon = GI.y(p), GI.x(p)
        params = Dict{String, String}(
            "parameters" => join(string.(variables), ","),
            "community"  => community,
            "longitude"  => string(lon),
            "latitude"   => string(lat),
            "start"      => Dates.format(start_date, dateformat"yyyymmdd"),
            "end"        => Dates.format(stop_date, dateformat"yyyymmdd"),
            "format"     => "JSON",
        )
        url = _build_url(POINT_URL, params)
        push!(requests, RequestInfo(source, url, HTTPMethod.GET, "Point($lat, $lon), $n_days days"))
    end
    total_rows = n_days * length(points)
    DataAccessPlan(source, requests, _describe_extent(GI.geomtrait(geom), geom),
        (start_date, stop_date), variables,
        Dict{Symbol, Any}(:community => community, :query_type => QueryType.multi_point),
        _estimate_bytes(total_rows, length(variables)), retention)
end

_register_source!(Source())

end # module NASAPower
