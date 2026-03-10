#--------------------------------------------------------------------------------# Visual Crossing

module VisualCrossing

import ..GeoDataAccess
using ..GeoDataAccess: AbstractDataSource, MetaData, DataAccessPlan, RequestInfo,
    _register_source!, _build_url, _describe_extent, _estimate_bytes, _get_api_key,
    WEATHER, RASTER, TemporalType, HTTPMethod
using Dates
import GeoInterface as GI

"""
    VisualCrossing.Source()

Historical and forecast weather data from [Visual Crossing](https://www.visualcrossing.com/).

- **Coverage**: Global, ~50 years of history
- **Resolution**: ~1 km, daily or hourly
- **API Key**: Required (`VISUAL_CROSSING_API_KEY` environment variable)
- **Rate Limit**: 1,000 records/day (free tier)

### Examples

```julia
ENV["VISUAL_CROSSING_API_KEY"] = "your-api-key"

plan = DataAccessPlan(VisualCrossing.Source(), (-74.0, 40.7),
    Date(2024, 1, 1), Date(2024, 1, 7);
    variables = [:tempmax, :tempmin, :precip],
    include = "days")
files = fetch(plan)
```
"""
struct Source <: AbstractDataSource end

GeoDataAccess.name(::Type{Source}) = "visualcrossing"

const URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"

const variables = (;
    temp       = "Temperature (°C)",
    tempmax    = "Maximum temperature (°C)",
    tempmin    = "Minimum temperature (°C)",
    feelslike  = "Feels-like temperature (°C)",
    humidity   = "Humidity (%)",
    dew        = "Dew point (°C)",
    precip     = "Precipitation (mm)",
    snow       = "Snowfall (cm)",
    snowdepth  = "Snow depth (cm)",
    windspeed  = "Wind speed (km/h)",
    winddir    = "Wind direction (°)",
    windgust   = "Wind gust (km/h)",
    pressure   = "Sea-level pressure (hPa)",
    cloudcover = "Cloud cover (%)",
    uvindex    = "UV index",
    visibility = "Visibility (km)",
    conditions = "Weather conditions text",
    sunrise    = "Sunrise time",
    sunset     = "Sunset time",
    moonphase  = "Moon phase (0-1)",
)

const metadata = MetaData(
    "VISUAL_CROSSING_API_KEY", "1000 records/day (free tier)",
    WEATHER, variables,
    RASTER, "1 km", "Global",
    TemporalType.timeseries, Day(1), "~50 years of history",
    "Commercial (free tier available)",
    "https://www.visualcrossing.com/resources/documentation/weather-api/timeline-weather-api/";
    load_packages = Dict("DataFrames" => "a93c6f00-e57d-5684-b7b6-d8193f3e46c0"),
)

GeoDataAccess.MetaData(::Source) = metadata

#--------------------------------------------------------------------------------# DataAccessPlan

function GeoDataAccess.DataAccessPlan(source::Source, extent, start_date::Date, stop_date::Date;
                                       variables::Vector{Symbol} = [:tempmax, :tempmin, :precip, :humidity],
                                       include::String = "days",
                                       retention::Union{Nothing, Dates.Period} = metadata.default_retention)
    api_key = _get_api_key(source)
    trait = GI.geomtrait(extent)
    _plan(source, trait, extent, start_date, stop_date, variables, include, api_key, retention)
end

function _plan(source::Source, ::GI.PointTrait, geom,
               start_date, stop_date, variables, include, api_key, retention)
    lat, lon = GI.y(geom), GI.x(geom)
    location = "$lat,$lon"
    date1 = Dates.format(start_date, dateformat"yyyy-mm-dd")
    date2 = Dates.format(stop_date, dateformat"yyyy-mm-dd")
    base = "$URL/$location/$date1/$date2"
    params = Dict{String, String}(
        "key"         => api_key,
        "unitGroup"   => "metric",
        "include"     => include,
        "elements"    => join(string.(variables), ",") * ",datetime",
        "contentType" => "json",
    )
    url = _build_url(base, params)
    n_days = Dates.value(stop_date - start_date) + 1
    total_rows = include == "hours" ? n_days * 24 : n_days
    request = RequestInfo(source, url, HTTPMethod.GET, "Point($lat, $lon), $include, $n_days days")
    DataAccessPlan(source, [request], _describe_extent(GI.PointTrait(), geom),
        (start_date, stop_date), variables,
        Dict{Symbol, Any}(:include => include),
        _estimate_bytes(total_rows, length(variables)), retention)
end

function _plan(source::Source, ::Union{GI.MultiPointTrait, GI.AbstractCurveTrait}, geom,
               start_date, stop_date, variables, include, api_key, retention)
    points = collect(GI.getpoint(geom))
    n_days = Dates.value(stop_date - start_date) + 1
    rows_per_point = include == "hours" ? n_days * 24 : n_days
    requests = RequestInfo[]
    for p in points
        lat, lon = GI.y(p), GI.x(p)
        location = "$lat,$lon"
        date1 = Dates.format(start_date, dateformat"yyyy-mm-dd")
        date2 = Dates.format(stop_date, dateformat"yyyy-mm-dd")
        base = "$URL/$location/$date1/$date2"
        params = Dict{String, String}(
            "key"         => api_key,
            "unitGroup"   => "metric",
            "include"     => include,
            "elements"    => join(string.(variables), ",") * ",datetime",
            "contentType" => "json",
        )
        url = _build_url(base, params)
        push!(requests, RequestInfo(source, url, HTTPMethod.GET, "Point($lat, $lon)"))
    end
    total_rows = rows_per_point * length(points)
    DataAccessPlan(source, requests, _describe_extent(GI.geomtrait(geom), geom),
        (start_date, stop_date), variables,
        Dict{Symbol, Any}(:include => include),
        _estimate_bytes(total_rows, length(variables)), retention)
end

function _plan(source::Source, ::GI.AbstractPolygonTrait, geom,
               start_date, stop_date, variables, include, api_key, retention)
    _plan(source, nothing, GI.extent(geom), start_date, stop_date, variables, include, api_key, retention)
end

function _plan(source::Source, ::Nothing, geom,
               start_date, stop_date, variables, include, api_key, retention)
    if !(hasproperty(geom, :X) && hasproperty(geom, :Y))
        error("Cannot extract coordinates from $(typeof(geom)) for Visual Crossing.")
    end
    xmin, xmax = geom.X
    ymin, ymax = geom.Y
    corners = GI.MultiPoint([(xmin, ymin), (xmax, ymin), (xmin, ymax), (xmax, ymax)])
    _plan(source, GI.MultiPointTrait(), corners, start_date, stop_date, variables, include, api_key, retention)
end

_register_source!(Source())

end # module VisualCrossing
