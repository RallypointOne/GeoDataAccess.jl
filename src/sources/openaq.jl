#--------------------------------------------------------------------------------# OpenAQ

"""
    OpenAQ()

Global air quality data from the [OpenAQ](https://openaq.org/) platform, aggregating
measurements from government agencies, research organizations, and low-cost sensor networks.

- **Coverage**: Global, 11,000+ stations
- **Resolution**: Station-based, hourly or daily aggregations
- **API Key**: Required (`OPENAQ_API_KEY` environment variable)
- **Rate Limit**: 60 requests/minute (free tier)

Requires sensor IDs via the `sensors` keyword argument.  Use the OpenAQ Explorer at
[explore.openaq.org](https://explore.openaq.org/) to find sensor IDs, or query the
`/v3/locations` endpoint with a bounding box.

### Examples

```julia
ENV["OPENAQ_API_KEY"] = "your-api-key"

# Daily PM2.5 aggregations for a sensor
plan = DataAccessPlan(OpenAQ(), (-87.6, 41.9),
    Date(2024, 1, 1), Date(2024, 1, 31);
    sensors = [1234],
    frequency = :daily)
files = fetch!(plan)
```
"""
struct OpenAQ <: AbstractDataSource end

_register_source!(OpenAQ())

const OPENAQ_BASE_URL = "https://api.openaq.org/v3"

const OPENAQ_VARIABLES = Dict{Symbol, String}(
    :pm25  => "PM2.5 (µg/m³)",
    :pm10  => "PM10 (µg/m³)",
    :o3    => "Ozone (µg/m³)",
    :no2   => "Nitrogen dioxide (µg/m³)",
    :so2   => "Sulfur dioxide (µg/m³)",
    :co    => "Carbon monoxide (ppm)",
    :bc    => "Black carbon (µg/m³)",
    :pm1   => "PM1 (µg/m³)",
)

#--------------------------------------------------------------------------------# MetaData

MetaData(::OpenAQ) = MetaData(
    "OPENAQ_API_KEY", "60 req/min (free tier)",
    :air_quality, OPENAQ_VARIABLES,
    :point, "Station-based", "Global",
    :timeseries, Hour(1), "Varies by station",
    "CC BY 4.0",
    "https://docs.openaq.org/",
)

#--------------------------------------------------------------------------------# Request Headers

function _request_headers(source::OpenAQ)
    api_key = _get_api_key(source)
    ["X-API-Key" => api_key]
end

#--------------------------------------------------------------------------------# DataAccessPlan

function DataAccessPlan(source::OpenAQ, extent, start_date::Date, stop_date::Date;
                        sensors::Vector{Int} = Int[],
                        frequency::Symbol = :daily)
    isempty(sensors) && error("OpenAQ requires sensor IDs via `sensors` keyword. " *
                              "Find sensors at https://explore.openaq.org/")
    frequency in (:hourly, :daily) || error("frequency must be :hourly or :daily, got :$frequency")

    endpoint = frequency == :daily ? "days" : "hours"
    date_from_key = frequency == :daily ? "date_from" : "datetime_from"
    date_to_key = frequency == :daily ? "date_to" : "datetime_to"

    requests = RequestInfo[]
    for sensor_id in sensors
        params = Dict{String, String}(
            date_from_key => Dates.format(start_date, dateformat"yyyy-mm-dd"),
            date_to_key   => Dates.format(stop_date, dateformat"yyyy-mm-dd"),
            "limit"       => "1000",
        )
        url = _build_url("$OPENAQ_BASE_URL/sensors/$sensor_id/$endpoint", params)
        push!(requests, RequestInfo(source, url, :GET, "Sensor $sensor_id, $endpoint"))
    end

    n_days = Dates.value(stop_date - start_date) + 1
    rows = frequency == :daily ? n_days : n_days * 24
    total_rows = rows * length(sensors)

    kwargs = Dict{Symbol, Any}(:sensors => sensors, :frequency => frequency)

    DataAccessPlan(source, requests, "$(length(sensors)) sensor(s)",
        (start_date, stop_date), collect(keys(OPENAQ_VARIABLES)), kwargs,
        _estimate_bytes(total_rows, 1))
end
