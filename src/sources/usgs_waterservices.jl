#--------------------------------------------------------------------------------# USGS Water Services

module USGSWaterServices

import ..GeoDataAccess
using ..GeoDataAccess: AbstractDataSource, MetaData, DataAccessPlan, RequestInfo,
    _register_source!, _build_url, _estimate_bytes,
    HYDROLOGY, POINT, TemporalType, HTTPMethod
using Dates

"""
    USGSWaterServices.Source(; service="dv")

Daily and instantaneous streamflow and water quality data from the
[USGS National Water Information System](https://waterservices.usgs.gov/).

- **Coverage**: US, 1.5 million+ sites
- **Resolution**: Station-based, daily (`"dv"`) or instantaneous ~15-min (`"iv"`)
- **API Key**: Not required
- **Rate Limit**: ~5-10 requests/second (informal)

### Examples

```julia
plan = DataAccessPlan(USGSWaterServices.Source(), (-77.1, 38.9),
    Date(2024, 1, 1), Date(2024, 12, 31);
    sites = ["01646500"],
    variables = [Symbol("00060")])
files = fetch(plan)
```
"""
struct Source <: AbstractDataSource
    service::String
end

Source(; service::String = "dv") = Source(service)

GeoDataAccess.name(::Type{Source}) = "usgswaterservices"

Base.show(io::IO, s::Source) = print(io, "USGSWaterServices(\"$(s.service)\")")

const DV_URL = "https://waterservices.usgs.gov/nwis/dv/"
const IV_URL = "https://waterservices.usgs.gov/nwis/iv/"

const variables = (;
    var"00060" = "Streamflow / Discharge (ft³/s)",
    var"00065" = "Gage height (ft)",
    var"00010" = "Water temperature (°C)",
    var"00011" = "Air temperature (°C)",
    var"00045" = "Precipitation (in)",
    var"00095" = "Specific conductance (µS/cm at 25°C)",
    var"00300" = "Dissolved oxygen (mg/L)",
    var"00400" = "pH (standard units)",
    var"63680" = "Turbidity (FNU)",
)

const metadata = MetaData(
    "", "~5-10 req/s (informal)",
    HYDROLOGY, variables,
    POINT, "Station-based", "US",
    TemporalType.timeseries, Day(1), "Varies by site (many decades)",
    "Public Domain",
    "https://waterservices.usgs.gov/docs/";
    load_packages = Dict("DataFrames" => "a93c6f00-e57d-5684-b7b6-d8193f3e46c0"),
)

GeoDataAccess.MetaData(::Source) = metadata

function GeoDataAccess.DataAccessPlan(source::Source, extent, start_date::Date, stop_date::Date;
                                       variables::Vector{Symbol} = [Symbol("00060")],
                                       sites::Vector{String} = String[],
                                       statCd::String = "00003",
                                       retention::Union{Nothing, Dates.Period} = metadata.default_retention)
    isempty(sites) && error("USGS Water Services requires site numbers via `sites` keyword. " *
                            "Example: `sites=[\"01646500\"]`")
    base_url = source.service == "iv" ? IV_URL : DV_URL
    params = Dict{String, String}(
        "format"      => "json",
        "sites"       => join(sites, ","),
        "parameterCd" => join(string.(variables), ","),
        "startDT"     => Dates.format(start_date, dateformat"yyyy-mm-dd"),
        "endDT"       => Dates.format(stop_date, dateformat"yyyy-mm-dd"),
    )
    if source.service == "dv"
        params["statCd"] = statCd
    end
    url = _build_url(base_url, params)

    n_days = Dates.value(stop_date - start_date) + 1
    total_rows = n_days * length(sites)

    request = RequestInfo(source, url, HTTPMethod.GET, "$(length(sites)) site(s), $n_days days")

    kwargs = Dict{Symbol, Any}(:sites => sites, :service => source.service)
    source.service == "dv" && (kwargs[:statCd] = statCd)

    DataAccessPlan(source, [request], "$(length(sites)) site(s): $(join(sites, ", "))",
        (start_date, stop_date), variables, kwargs,
        _estimate_bytes(total_rows, length(variables)), retention)
end

_register_source!(Source())

end # module USGSWaterServices
