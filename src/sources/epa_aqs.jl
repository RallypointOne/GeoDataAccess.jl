#--------------------------------------------------------------------------------# EPA AQS

module EPAAQS

import ..GeoDataAccess
using ..GeoDataAccess: AbstractDataSource, MetaData, DataAccessPlan, RequestInfo,
    _register_source!, _build_url, _describe_extent, _estimate_bytes,
    AIR_QUALITY, POINT, TemporalType, HTTPMethod, EPAService
using Dates
import GeoInterface as GI

"""
    EPAAQS.Source()

US air quality monitoring data from the [EPA Air Quality System](https://aqs.epa.gov/).

- **Coverage**: US, thousands of stations
- **Resolution**: Station-based, hourly or daily
- **API Key**: Required (`EPA_AQS_EMAIL` and `EPA_AQS_KEY` environment variables)
- **Rate Limit**: 10 requests/minute

### Examples

```julia
ENV["EPA_AQS_EMAIL"] = "your@email.com"
ENV["EPA_AQS_KEY"]   = "your-api-key"

using GeoInterface.Extents: Extent

plan = DataAccessPlan(EPAAQS.Source(),
    Extent(X=(-87.0, -86.7), Y=(33.3, 33.6)),
    Date(2023, 6, 1), Date(2023, 6, 30);
    parameters = [Symbol("88101")])
files = fetch(plan)
```
"""
struct Source <: AbstractDataSource end

GeoDataAccess.name(::Type{Source}) = "epaaqs"

const URL = "https://aqs.epa.gov/data/api"

const variables = (;
    var"88101" = "PM2.5 - Local Conditions (µg/m³)",
    var"88502" = "PM2.5 - Non-FRM (µg/m³)",
    var"81102" = "PM10 (µg/m³)",
    var"44201" = "Ozone (ppm)",
    var"42101" = "Carbon Monoxide (ppm)",
    var"42401" = "Sulfur Dioxide (ppb)",
    var"42602" = "Nitrogen Dioxide (ppb)",
    var"14129" = "Lead TSP (µg/m³)",
)

const metadata = MetaData(
    "EPA_AQS_KEY", "10 req/min",
    AIR_QUALITY, variables,
    POINT, "Station-based", "US",
    TemporalType.timeseries, Day(1), "Varies by station (decades)",
    "Public Domain",
    "https://aqs.epa.gov/aqsweb/documents/data_api.html";
    load_packages = Dict("DataFrames" => "a93c6f00-e57d-5684-b7b6-d8193f3e46c0"),
)

GeoDataAccess.MetaData(::Source) = metadata

#--------------------------------------------------------------------------------# Auth

function _credentials()
    email = get(ENV, "EPA_AQS_EMAIL", "")
    key = get(ENV, "EPA_AQS_KEY", "")
    (isempty(email) || isempty(key)) &&
        error("EPA AQS requires both EPA_AQS_EMAIL and EPA_AQS_KEY environment variables. " *
              "Register at https://aqs.epa.gov/data/api/signup?email=your@email.com")
    email, key
end

#--------------------------------------------------------------------------------# DataAccessPlan

function GeoDataAccess.DataAccessPlan(source::Source, extent, start_date::Date, stop_date::Date;
                                       service::EPAService.T = EPAService.dailyData,
                                       parameters::Vector{Symbol} = [Symbol("88101")],
                                       state::String = "",
                                       county::String = "",
                                       site::String = "",
                                       retention::Union{Nothing, Dates.Period} = metadata.default_retention)
    length(parameters) > 5 &&
        error("EPA AQS allows at most 5 parameter codes per request (got $(length(parameters)))")

    email, key = _credentials()

    filter_path, spatial_params, extent_desc = _filter(extent, state, county, site)

    param_str = join(string.(parameters), ",")

    year_ranges = _split_years(start_date, stop_date)

    requests = RequestInfo[]
    for (bd, ed) in year_ranges
        params = Dict{String, String}(
            "email" => email,
            "key"   => key,
            "param" => param_str,
            "bdate" => Dates.format(bd, dateformat"yyyymmdd"),
            "edate" => Dates.format(ed, dateformat"yyyymmdd"),
        )
        merge!(params, spatial_params)
        url = _build_url("$URL/$service/$filter_path", params)
        n_days = Dates.value(ed - bd) + 1
        push!(requests, RequestInfo(source, url, HTTPMethod.GET,
            "$extent_desc, $n_days days ($(Dates.year(bd)))"))
    end

    total_days = Dates.value(stop_date - start_date) + 1
    kwargs = Dict{Symbol, Any}(:service => service)
    !isempty(state) && (kwargs[:state] = state)
    !isempty(county) && (kwargs[:county] = county)
    !isempty(site) && (kwargs[:site] = site)

    DataAccessPlan(source, requests, extent_desc,
        (start_date, stop_date), parameters, kwargs,
        _estimate_bytes(total_days, length(parameters)), retention)
end

#--------------------------------------------------------------------------------# Helpers

function _filter(extent, state, county, site)
    if !isempty(state) && !isempty(county) && !isempty(site)
        params = Dict("state" => state, "county" => county, "site" => site)
        return "bySite", params, "Site $state-$county-$site"
    elseif !isempty(state) && !isempty(county)
        params = Dict("state" => state, "county" => county)
        return "byCounty", params, "County $state-$county"
    elseif !isempty(state)
        params = Dict("state" => state)
        return "byState", params, "State $state"
    else
        west, south, east, north = _bbox(extent)
        params = Dict(
            "minlat" => string(south),
            "maxlat" => string(north),
            "minlon" => string(west),
            "maxlon" => string(east),
        )
        return "byBox", params, _describe_extent(extent)
    end
end

function _bbox(extent)
    trait = GI.geomtrait(extent)
    _bbox(trait, extent)
end

function _bbox(::GI.PointTrait, geom)
    lon, lat = GI.x(geom), GI.y(geom)
    (lon - 0.1, lat - 0.1, lon + 0.1, lat + 0.1)
end

function _bbox(::GI.AbstractPolygonTrait, geom)
    _bbox(nothing, GI.extent(geom))
end

function _bbox(::Nothing, geom)
    if hasproperty(geom, :X) && hasproperty(geom, :Y)
        xmin, xmax = geom.X
        ymin, ymax = geom.Y
        return (xmin, ymin, xmax, ymax)
    end
    error("Cannot extract bounding box from $(typeof(geom)) for EPA AQS.")
end

function _bbox(::Union{GI.MultiPointTrait, GI.AbstractCurveTrait}, geom)
    points = collect(GI.getpoint(geom))
    lons = [GI.x(p) for p in points]
    lats = [GI.y(p) for p in points]
    (minimum(lons), minimum(lats), maximum(lons), maximum(lats))
end

function _split_years(start_date::Date, stop_date::Date)
    ranges = Tuple{Date, Date}[]
    current = start_date
    while current <= stop_date
        year_end = min(Date(Dates.year(current), 12, 31), stop_date)
        push!(ranges, (current, year_end))
        current = year_end + Day(1)
    end
    ranges
end

_register_source!(Source())

end # module EPAAQS
