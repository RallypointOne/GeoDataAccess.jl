#--------------------------------------------------------------------------------# EPA AQS

"""
    EPAAQS()

US air quality monitoring data from the [EPA Air Quality System](https://aqs.epa.gov/).
Provides measurements from thousands of monitoring stations across the United States.

- **Coverage**: US, thousands of stations
- **Resolution**: Station-based, hourly or daily
- **API Key**: Required (`EPA_AQS_EMAIL` and `EPA_AQS_KEY` environment variables)
- **Rate Limit**: 10 requests/minute

Register at `https://aqs.epa.gov/data/api/signup?email=your@email.com` — a key will be
emailed to you.

Supports queries by bounding box (from geometries), or by state/county/site FIPS codes via
keyword arguments.  Date ranges spanning multiple years are automatically split into per-year
requests (API constraint).

### Examples

```julia
ENV["EPA_AQS_EMAIL"] = "your@email.com"
ENV["EPA_AQS_KEY"]   = "your-api-key"

using GeoInterface.Extents: Extent

# Daily PM2.5 in a bounding box
plan = DataAccessPlan(EPAAQS(),
    Extent(X=(-87.0, -86.7), Y=(33.3, 33.6)),
    Date(2023, 6, 1), Date(2023, 6, 30);
    parameters = [Symbol("88101")])
files = fetch!(plan)

# By state and county FIPS codes
plan = DataAccessPlan(EPAAQS(), (0.0, 0.0),  # extent ignored for FIPS queries
    Date(2023, 6, 1), Date(2023, 6, 30);
    parameters = [Symbol("44201")],  # Ozone
    state = "37", county = "183")
```
"""
struct EPAAQS <: AbstractDataSource end

_register_source!(EPAAQS())

const EPA_AQS_BASE_URL = "https://aqs.epa.gov/data/api"

const EPA_AQS_VARIABLES = Dict{Symbol, String}(
    Symbol("88101") => "PM2.5 - Local Conditions (µg/m³)",
    Symbol("88502") => "PM2.5 - Non-FRM (µg/m³)",
    Symbol("81102") => "PM10 (µg/m³)",
    Symbol("44201") => "Ozone (ppm)",
    Symbol("42101") => "Carbon Monoxide (ppm)",
    Symbol("42401") => "Sulfur Dioxide (ppb)",
    Symbol("42602") => "Nitrogen Dioxide (ppb)",
    Symbol("14129") => "Lead TSP (µg/m³)",
)

#--------------------------------------------------------------------------------# MetaData

MetaData(::EPAAQS) = MetaData(
    "EPA_AQS_KEY", "10 req/min",
    :air_quality, EPA_AQS_VARIABLES,
    :point, "Station-based", "US",
    :timeseries, Day(1), "Varies by station (decades)",
    "Public Domain",
    "https://aqs.epa.gov/aqsweb/documents/data_api.html",
)

#--------------------------------------------------------------------------------# Request Headers / Auth

function _epa_aqs_credentials()
    email = get(ENV, "EPA_AQS_EMAIL", "")
    key = get(ENV, "EPA_AQS_KEY", "")
    (isempty(email) || isempty(key)) &&
        error("EPA AQS requires both EPA_AQS_EMAIL and EPA_AQS_KEY environment variables. " *
              "Register at https://aqs.epa.gov/data/api/signup?email=your@email.com")
    email, key
end

#--------------------------------------------------------------------------------# DataAccessPlan

function DataAccessPlan(source::EPAAQS, extent, start_date::Date, stop_date::Date;
                        service::Symbol = :dailyData,
                        parameters::Vector{Symbol} = [Symbol("88101")],
                        state::String = "",
                        county::String = "",
                        site::String = "")
    service in (:sampleData, :dailyData, :annualData) ||
        error("service must be :sampleData, :dailyData, or :annualData, got :$service")
    length(parameters) > 5 &&
        error("EPA AQS allows at most 5 parameter codes per request (got $(length(parameters)))")

    email, key = _epa_aqs_credentials()

    # Determine endpoint filter and spatial params
    filter_path, spatial_params, extent_desc = _epa_aqs_filter(extent, state, county, site)

    param_str = join(string.(parameters), ",")

    # Split by calendar year (API constraint)
    year_ranges = _epa_aqs_split_years(start_date, stop_date)

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
        url = _build_url("$EPA_AQS_BASE_URL/$service/$filter_path", params)
        n_days = Dates.value(ed - bd) + 1
        push!(requests, RequestInfo(source, url, :GET,
            "$extent_desc, $n_days days ($(Dates.year(bd)))"))
    end

    total_days = Dates.value(stop_date - start_date) + 1
    kwargs = Dict{Symbol, Any}(:service => service)
    !isempty(state) && (kwargs[:state] = state)
    !isempty(county) && (kwargs[:county] = county)
    !isempty(site) && (kwargs[:site] = site)

    DataAccessPlan(source, requests, extent_desc,
        (start_date, stop_date), parameters, kwargs,
        _estimate_bytes(total_days, length(parameters)))
end

#--------------------------------------------------------------------------------# Helpers

function _epa_aqs_filter(extent, state, county, site)
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
        # Use bounding box from extent
        west, south, east, north = _epa_aqs_bbox(extent)
        params = Dict(
            "minlat" => string(south),
            "maxlat" => string(north),
            "minlon" => string(west),
            "maxlon" => string(east),
        )
        return "byBox", params, _describe_extent(extent)
    end
end

function _epa_aqs_bbox(extent)
    trait = GI.geomtrait(extent)
    _epa_aqs_bbox(trait, extent)
end

function _epa_aqs_bbox(::GI.PointTrait, geom)
    lon, lat = GI.x(geom), GI.y(geom)
    (lon - 0.1, lat - 0.1, lon + 0.1, lat + 0.1)
end

function _epa_aqs_bbox(::GI.AbstractPolygonTrait, geom)
    _epa_aqs_bbox(nothing, GI.extent(geom))
end

function _epa_aqs_bbox(::Nothing, geom)
    if hasproperty(geom, :X) && hasproperty(geom, :Y)
        xmin, xmax = geom.X
        ymin, ymax = geom.Y
        return (xmin, ymin, xmax, ymax)
    end
    error("Cannot extract bounding box from $(typeof(geom)) for EPA AQS.")
end

function _epa_aqs_bbox(::Union{GI.MultiPointTrait, GI.AbstractCurveTrait}, geom)
    points = collect(GI.getpoint(geom))
    lons = [GI.x(p) for p in points]
    lats = [GI.y(p) for p in points]
    (minimum(lons), minimum(lats), maximum(lons), maximum(lats))
end

function _epa_aqs_split_years(start_date::Date, stop_date::Date)
    ranges = Tuple{Date, Date}[]
    current = start_date
    while current <= stop_date
        year_end = min(Date(Dates.year(current), 12, 31), stop_date)
        push!(ranges, (current, year_end))
        current = year_end + Day(1)
    end
    ranges
end
