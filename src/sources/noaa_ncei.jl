#--------------------------------------------------------------------------------# NOAA NCEI

module NOAANCEI

import ..GeoDataAccess
using ..GeoDataAccess: AbstractDataSource, MetaData, DataAccessPlan, RequestInfo,
    _register_source!, _build_url, _estimate_bytes,
    WEATHER, POINT, TemporalType, HTTPMethod
using Dates

"""
    NOAANCEI.Source(; dataset="daily-summaries")

Historical station-based weather observations from [NOAA's National Centers for Environmental
Information](https://www.ncei.noaa.gov/).

- **Coverage**: Global weather stations, 1763–present
- **Resolution**: Station-based, daily
- **API Key**: Not required

### Examples

```julia
plan = DataAccessPlan(NOAANCEI.Source(), (-74.0, 40.7),
    Date(2024, 1, 1), Date(2024, 1, 7);
    stations = ["USW00094728"],
    variables = [:TMAX, :TMIN, :PRCP])
files = fetch(plan)
```
"""
struct Source <: AbstractDataSource
    dataset::String
end

Source(; dataset::String = "daily-summaries") = Source(dataset)

GeoDataAccess.name(::Type{Source}) = "noaancei"

Base.show(io::IO, s::Source) = print(io, "NOAANCEI(\"$(s.dataset)\")")

const URL = "https://www.ncei.noaa.gov/access/services/data/v1"

const variables = (;
    TMAX = "Maximum temperature (tenths of °C)",
    TMIN = "Minimum temperature (tenths of °C)",
    TAVG = "Average temperature (tenths of °C)",
    PRCP = "Precipitation (tenths of mm)",
    SNOW = "Snowfall (mm)",
    SNWD = "Snow depth (mm)",
    AWND = "Average wind speed (tenths of m/s)",
    WSF2 = "Fastest 2-minute wind speed (tenths of m/s)",
    WDF2 = "Direction of fastest 2-minute wind (degrees)",
    WSF5 = "Fastest 5-second wind speed (tenths of m/s)",
    WDF5 = "Direction of fastest 5-second wind (degrees)",
    TSUN = "Total sunshine (minutes)",
    PSUN = "Percent of possible sunshine (%)",
)

const metadata = MetaData(
    "", "Undocumented",
    WEATHER, variables,
    POINT, "Station-based", "Global (station-based)",
    TemporalType.timeseries, Day(1), "1763-present",
    "Public Domain",
    "https://www.ncei.noaa.gov/support/access-data-service-api-user-documentation";
    load_packages = Dict("DataFrames" => "a93c6f00-e57d-5684-b7b6-d8193f3e46c0"),
)

GeoDataAccess.MetaData(::Source) = metadata

function GeoDataAccess.DataAccessPlan(source::Source, extent, start_date::Date, stop_date::Date;
                                       variables::Vector{Symbol} = [:TMAX, :TMIN, :PRCP],
                                       stations::Vector{String} = String[],
                                       retention::Union{Nothing, Dates.Period} = metadata.default_retention)
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
    url = _build_url(URL, params)

    n_days = Dates.value(stop_date - start_date) + 1
    total_rows = n_days * length(stations)

    request = RequestInfo(source, url, HTTPMethod.GET, "$(length(stations)) station(s), $n_days days")

    kwargs = Dict{Symbol, Any}(:stations => stations)

    DataAccessPlan(source, [request], "$(length(stations)) station(s): $(join(stations, ", "))",
        (start_date, stop_date), variables, kwargs,
        _estimate_bytes(total_rows, length(variables)), retention)
end

_register_source!(Source())

end # module NOAANCEI
