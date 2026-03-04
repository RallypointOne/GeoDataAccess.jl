#--------------------------------------------------------------------------------# NASA POWER

"""
    NASAPower()

Global daily meteorological and solar energy data from [NASA POWER](https://power.larc.nasa.gov/).

- **Coverage**: Global, 1981–present
- **Resolution**: ~55 km (0.5° × 0.5°), daily
- **API Key**: Not required
- **Rate Limit**: No published rate limit

Supports point queries, multi-point queries, and regional bounding box queries.  The regional
endpoint requires at least 2° in both latitude and longitude.

### Examples

```julia
# Point query
plan = DataAccessPlan(NASAPower(), (-74.0, 40.7),
    Date(2024, 1, 1), Date(2024, 1, 7);
    variables = [:T2M, :PRECTOTCORR])
files = fetch(plan)
```
"""
struct NASAPower <: AbstractDataSource end

_register_source!(NASAPower())

const NASA_POWER_POINT_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"
const NASA_POWER_REGIONAL_URL = "https://power.larc.nasa.gov/api/temporal/daily/regional"

const NASA_POWER_VARIABLES = Dict{Symbol, String}(
    :T2M                => "Temperature at 2m (°C)",
    :T2M_MAX            => "Maximum temperature at 2m (°C)",
    :T2M_MIN            => "Minimum temperature at 2m (°C)",
    :T2M_RANGE          => "Temperature range at 2m (°C)",
    :T2MDEW             => "Dew/frost point at 2m (°C)",
    :RH2M               => "Relative humidity at 2m (%)",
    :PRECTOTCORR        => "Precipitation corrected (mm/day)",
    :WS2M               => "Wind speed at 2m (m/s)",
    :WS10M              => "Wind speed at 10m (m/s)",
    :WD2M               => "Wind direction at 2m (°)",
    :WD10M              => "Wind direction at 10m (°)",
    :PS                 => "Surface pressure (kPa)",
    :QV2M               => "Specific humidity at 2m (g/kg)",
    :CLOUD_AMT          => "Cloud amount (%)",
    :ALLSKY_SFC_SW_DWN  => "All-sky surface shortwave downward irradiance (MJ/m²/day)",
    :CLRSKY_SFC_SW_DWN  => "Clear-sky surface shortwave downward irradiance (MJ/m²/day)",
    :ALLSKY_SFC_LW_DWN  => "All-sky surface longwave downward irradiance (W/m²)",
)

#--------------------------------------------------------------------------------# MetaData

MetaData(::NASAPower) = MetaData(
    "", "No published rate limit",
    :weather, NASA_POWER_VARIABLES,
    :raster, "55 km", "Global",
    :timeseries, Day(1), "1981-present",
    "Open Data (NASA)",
    "https://power.larc.nasa.gov/docs/services/api/";
    load_packages = Dict("DataFrames" => "a93c6f00-e57d-5684-b7b6-d8193f3e46c0"),
)

#--------------------------------------------------------------------------------# DataAccessPlan

function DataAccessPlan(source::NASAPower, extent, start_date::Date, stop_date::Date;
                        variables::Vector{Symbol} = [:T2M, :PRECTOTCORR],
                        community::String = "AG")
    trait = GI.geomtrait(extent)
    _nasa_power_plan(source, trait, extent, start_date, stop_date, variables, community)
end

# Point query
function _nasa_power_plan(source::NASAPower, ::GI.PointTrait, geom, start_date, stop_date, variables, community)
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
    url = _build_url(NASA_POWER_POINT_URL, params)
    n_days = Dates.value(stop_date - start_date) + 1

    request = RequestInfo(source, url, :GET, "Point($lat, $lon), $n_days days")

    DataAccessPlan(source, [request], _describe_extent(GI.PointTrait(), geom),
        (start_date, stop_date), variables,
        Dict{Symbol, Any}(:community => community, :query_type => :point),
        _estimate_bytes(n_days, length(variables)))
end

# Extent / bounding box → regional endpoint
function _nasa_power_plan(source::NASAPower, ::Nothing, geom, start_date, stop_date, variables, community)
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
    url = _build_url(NASA_POWER_REGIONAL_URL, params)
    n_days = Dates.value(stop_date - start_date) + 1

    request = RequestInfo(source, url, :GET, "Regional bbox, $n_days days")

    DataAccessPlan(source, [request], _describe_extent(nothing, geom),
        (start_date, stop_date), variables,
        Dict{Symbol, Any}(:community => community, :query_type => :regional),
        _estimate_bytes(n_days, length(variables)))
end

# Polygon → extract extent → regional
function _nasa_power_plan(source::NASAPower, ::GI.AbstractPolygonTrait, geom, start_date, stop_date, variables, community)
    _nasa_power_plan(source, nothing, GI.extent(geom), start_date, stop_date, variables, community)
end

# MultiPoint / LineString → multiple point queries
function _nasa_power_plan(source::NASAPower, ::Union{GI.MultiPointTrait, GI.AbstractCurveTrait}, geom,
                          start_date, stop_date, variables, community)
    points = collect(GI.getpoint(geom))
    n_days = Dates.value(stop_date - start_date) + 1
    requests = RequestInfo[]
    urls = String[]

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
        url = _build_url(NASA_POWER_POINT_URL, params)
        push!(requests, RequestInfo(source, url, :GET, "Point($lat, $lon), $n_days days"))
        push!(urls, url)
    end

    total_rows = n_days * length(points)

    DataAccessPlan(source, requests, _describe_extent(GI.geomtrait(geom), geom),
        (start_date, stop_date), variables,
        Dict{Symbol, Any}(:community => community, :query_type => :multi_point),
        _estimate_bytes(total_rows, length(variables)))
end

