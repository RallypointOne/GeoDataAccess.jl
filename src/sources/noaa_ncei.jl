#--------------------------------------------------------------------------------# NOAA NCEI

"""
    NOAANCEI(; dataset="daily-summaries")

Historical station-based weather observations from [NOAA's National Centers for Environmental
Information](https://www.ncei.noaa.gov/).

- **Coverage**: Global weather stations, 1763–present
- **Resolution**: Station-based, daily
- **API Key**: Not required
- **Dataset**: Defaults to `"daily-summaries"`. Other options include `"global-summary-of-the-month"`.

Requires station IDs via the `stations` keyword argument. Station IDs can be found at
[NOAA's Station Search](https://www.ncdc.noaa.gov/cdo-web/datatools/findstation).

### Examples

```julia
plan = DataAccessPlan(NOAANCEI(), (-74.0, 40.7),
    Date(2024, 1, 1), Date(2024, 1, 7);
    stations = ["USW00094728"],  # Central Park, NYC
    variables = [:TMAX, :TMIN, :PRCP])
files = fetch(plan)
```
"""
struct NOAANCEI <: AbstractDataSource
    dataset::String
end

NOAANCEI(; dataset::String = "daily-summaries") = NOAANCEI(dataset)

_register_source!(NOAANCEI())

Base.show(io::IO, s::NOAANCEI) = print(io, "NOAANCEI(\"$(s.dataset)\")")

const NOAA_NCEI_URL = "https://www.ncei.noaa.gov/access/services/data/v1"

const NOAA_NCEI_VARIABLES = Dict{Symbol, String}(
    :TMAX => "Maximum temperature (tenths of °C)",
    :TMIN => "Minimum temperature (tenths of °C)",
    :TAVG => "Average temperature (tenths of °C)",
    :PRCP => "Precipitation (tenths of mm)",
    :SNOW => "Snowfall (mm)",
    :SNWD => "Snow depth (mm)",
    :AWND => "Average wind speed (tenths of m/s)",
    :WSF2 => "Fastest 2-minute wind speed (tenths of m/s)",
    :WDF2 => "Direction of fastest 2-minute wind (degrees)",
    :WSF5 => "Fastest 5-second wind speed (tenths of m/s)",
    :WDF5 => "Direction of fastest 5-second wind (degrees)",
    :TSUN => "Total sunshine (minutes)",
    :PSUN => "Percent of possible sunshine (%)",
)

#--------------------------------------------------------------------------------# MetaData

MetaData(::NOAANCEI) = MetaData(
    "", "Undocumented",
    :weather, NOAA_NCEI_VARIABLES,
    :point, "Station-based", "Global (station-based)",
    :timeseries, Day(1), "1763-present",
    "Public Domain",
    "https://www.ncei.noaa.gov/support/access-data-service-api-user-documentation";
    load_packages = Dict("DataFrames" => "a93c6f00-e57d-5684-b7b6-d8193f3e46c0"),
)

#--------------------------------------------------------------------------------# DataAccessPlan

function DataAccessPlan(source::NOAANCEI, extent, start_date::Date, stop_date::Date;
                        variables::Vector{Symbol} = [:TMAX, :TMIN, :PRCP],
                        stations::Vector{String} = String[])
    isempty(stations) && error("NOAA NCEI requires station IDs via `stations` keyword. " *
                               "Example: `stations=[\"USW00094728\"]`")
    params = Dict{String, String}(
        "dataset" => source.dataset,
        "dataTypes" => join(string.(variables), ","),
        "startDate" => Dates.format(start_date, dateformat"yyyy-mm-dd"),
        "endDate" => Dates.format(stop_date, dateformat"yyyy-mm-dd"),
        "format" => "json",
        "units" => "metric",
        "includeStationName" => "true",
        "includeStationLocation" => "true",
        "stations" => join(stations, ","),
    )
    url = _build_url(NOAA_NCEI_URL, params)

    n_days = Dates.value(stop_date - start_date) + 1
    total_rows = n_days * length(stations)

    request = RequestInfo(source, url, :GET, "$(length(stations)) station(s), $n_days days")

    kwargs = Dict{Symbol, Any}(:stations => stations)

    DataAccessPlan(source, [request], "$(length(stations)) station(s): $(join(stations, ", "))",
        (start_date, stop_date), variables, kwargs,
        _estimate_bytes(total_rows, length(variables)))
end

