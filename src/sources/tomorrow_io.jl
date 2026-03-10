#--------------------------------------------------------------------------------# Tomorrow.io

module TomorrowIO

import ..GeoDataAccess
using ..GeoDataAccess: AbstractDataSource, MetaData, DataAccessPlan, RequestInfo,
    _register_source!, _build_url, _describe_extent, _estimate_bytes, _get_api_key,
    WEATHER, RASTER, TemporalType, HTTPMethod
using Dates
import GeoInterface as GI

"""
    TomorrowIO.Source()

Global weather data from [Tomorrow.io](https://www.tomorrow.io/).

- **Coverage**: Global, 2000–present
- **Resolution**: ~4 km, hourly or daily
- **API Key**: Required (`TOMORROW_IO_API_KEY` environment variable)
- **Rate Limit**: ~500 requests/day (free tier)

### Examples

```julia
ENV["TOMORROW_IO_API_KEY"] = "your-api-key"

plan = DataAccessPlan(TomorrowIO.Source(), (-74.0, 40.7),
    Date(2024, 1, 1), Date(2024, 1, 7);
    variables = [:temperature, :humidity],
    timestep = "1d")
files = fetch(plan)
```
"""
struct Source <: AbstractDataSource end

GeoDataAccess.name(::Type{Source}) = "tomorrowio"

const URL = "https://api.tomorrow.io/v4/timelines"

const variables = (;
    temperature              = "Temperature (°C)",
    temperatureApparent      = "Apparent temperature (°C)",
    temperatureMax           = "Daily max temperature (°C)",
    temperatureMin           = "Daily min temperature (°C)",
    humidity                 = "Humidity (%)",
    dewPoint                 = "Dew point (°C)",
    windSpeed                = "Wind speed (m/s)",
    windDirection            = "Wind direction (°)",
    windGust                 = "Wind gust (m/s)",
    precipitationIntensity   = "Precipitation intensity (mm/hr)",
    precipitationProbability = "Precipitation probability (%)",
    rainIntensity            = "Rain intensity (mm/hr)",
    snowIntensity            = "Snow intensity (mm/hr)",
    pressureSurfaceLevel     = "Surface-level pressure (hPa)",
    cloudCover               = "Cloud cover (%)",
    uvIndex                  = "UV index",
    weatherCode              = "Weather condition code",
    visibility               = "Visibility (km)",
)

const metadata = MetaData(
    "TOMORROW_IO_API_KEY", "~500 req/day (free tier)",
    WEATHER, variables,
    RASTER, "4 km", "Global",
    TemporalType.timeseries, Hour(1), "2000-present",
    "Commercial (free tier available)",
    "https://docs.tomorrow.io/reference/welcome";
    load_packages = Dict("DataFrames" => "a93c6f00-e57d-5684-b7b6-d8193f3e46c0"),
)

GeoDataAccess.MetaData(::Source) = metadata

#--------------------------------------------------------------------------------# DataAccessPlan

function GeoDataAccess.DataAccessPlan(source::Source, extent, start_date::Date, stop_date::Date;
                                       variables::Vector{Symbol} = [:temperature, :humidity, :precipitationIntensity],
                                       timestep::String = "1d",
                                       retention::Union{Nothing, Dates.Period} = metadata.default_retention)
    api_key = _get_api_key(source)
    trait = GI.geomtrait(extent)
    _plan(source, trait, extent, start_date, stop_date, variables, timestep, api_key, retention)
end

function _plan(source::Source, ::GI.PointTrait, geom,
               start_date, stop_date, variables, timestep, api_key, retention)
    lat, lon = GI.y(geom), GI.x(geom)
    location = "$lat,$lon"
    params = Dict{String, String}(
        "apikey"     => api_key,
        "location"   => location,
        "fields"     => join(string.(variables), ","),
        "timesteps"  => timestep,
        "startTime"  => string(start_date) * "T00:00:00Z",
        "endTime"    => string(stop_date) * "T23:59:59Z",
        "units"      => "metric",
    )
    url = _build_url(URL, params)
    n_days = Dates.value(stop_date - start_date) + 1
    total_rows = timestep == "1h" ? n_days * 24 : n_days
    request = RequestInfo(source, url, HTTPMethod.GET, "Point($lat, $lon), $timestep, $n_days days")
    DataAccessPlan(source, [request], _describe_extent(GI.PointTrait(), geom),
        (start_date, stop_date), variables,
        Dict{Symbol, Any}(:timestep => timestep),
        _estimate_bytes(total_rows, length(variables)), retention)
end

function _plan(source::Source, ::Union{GI.MultiPointTrait, GI.AbstractCurveTrait}, geom,
               start_date, stop_date, variables, timestep, api_key, retention)
    points = collect(GI.getpoint(geom))
    n_days = Dates.value(stop_date - start_date) + 1
    rows_per_point = timestep == "1h" ? n_days * 24 : n_days
    requests = RequestInfo[]
    for p in points
        lat, lon = GI.y(p), GI.x(p)
        params = Dict{String, String}(
            "apikey"     => api_key,
            "location"   => "$lat,$lon",
            "fields"     => join(string.(variables), ","),
            "timesteps"  => timestep,
            "startTime"  => string(start_date) * "T00:00:00Z",
            "endTime"    => string(stop_date) * "T23:59:59Z",
            "units"      => "metric",
        )
        url = _build_url(URL, params)
        push!(requests, RequestInfo(source, url, HTTPMethod.GET, "Point($lat, $lon)"))
    end
    total_rows = rows_per_point * length(points)
    DataAccessPlan(source, requests, _describe_extent(GI.geomtrait(geom), geom),
        (start_date, stop_date), variables,
        Dict{Symbol, Any}(:timestep => timestep),
        _estimate_bytes(total_rows, length(variables)), retention)
end

function _plan(source::Source, ::GI.AbstractPolygonTrait, geom,
               start_date, stop_date, variables, timestep, api_key, retention)
    _plan(source, nothing, GI.extent(geom), start_date, stop_date, variables, timestep, api_key, retention)
end

function _plan(source::Source, ::Nothing, geom,
               start_date, stop_date, variables, timestep, api_key, retention)
    if !(hasproperty(geom, :X) && hasproperty(geom, :Y))
        error("Cannot extract coordinates from $(typeof(geom)) for Tomorrow.io.")
    end
    xmin, xmax = geom.X
    ymin, ymax = geom.Y
    corners = GI.MultiPoint([(xmin, ymin), (xmax, ymin), (xmin, ymax), (xmax, ymax)])
    _plan(source, GI.MultiPointTrait(), corners, start_date, stop_date, variables, timestep, api_key, retention)
end

_register_source!(Source())

end # module TomorrowIO
