#--------------------------------------------------------------------------------# USGS Water Services

"""
    USGSWaterServices(; service="dv")

Daily and instantaneous streamflow and water quality data from the
[USGS National Water Information System](https://waterservices.usgs.gov/).

- **Coverage**: US, 1.5 million+ sites
- **Resolution**: Station-based, daily (`"dv"`) or instantaneous ~15-min (`"iv"`)
- **API Key**: Not required
- **Rate Limit**: ~5-10 requests/second (informal)

Requires USGS site numbers via the `sites` keyword argument.  Site numbers can be found at
[USGS Water Data](https://waterdata.usgs.gov/).

### Examples

```julia
# Daily mean discharge for the Potomac River near Washington, DC
plan = DataAccessPlan(USGSWaterServices(), (-77.1, 38.9),
    Date(2024, 1, 1), Date(2024, 12, 31);
    sites = ["01646500"],
    variables = [Symbol("00060")])  # Discharge
files = fetch(plan)
```
"""
struct USGSWaterServices <: AbstractDataSource
    service::String
end

USGSWaterServices(; service::String = "dv") = USGSWaterServices(service)

_register_source!(USGSWaterServices())

Base.show(io::IO, s::USGSWaterServices) = print(io, "USGSWaterServices(\"$(s.service)\")")

const USGS_WATER_DV_URL = "https://waterservices.usgs.gov/nwis/dv/"
const USGS_WATER_IV_URL = "https://waterservices.usgs.gov/nwis/iv/"

const USGS_WATER_VARIABLES = Dict{Symbol, String}(
    Symbol("00060") => "Streamflow / Discharge (ft³/s)",
    Symbol("00065") => "Gage height (ft)",
    Symbol("00010") => "Water temperature (°C)",
    Symbol("00011") => "Air temperature (°C)",
    Symbol("00045") => "Precipitation (in)",
    Symbol("00095") => "Specific conductance (µS/cm at 25°C)",
    Symbol("00300") => "Dissolved oxygen (mg/L)",
    Symbol("00400") => "pH (standard units)",
    Symbol("63680") => "Turbidity (FNU)",
)

#--------------------------------------------------------------------------------# MetaData

MetaData(::USGSWaterServices) = MetaData(
    "", "~5-10 req/s (informal)",
    :hydrology, USGS_WATER_VARIABLES,
    :point, "Station-based", "US",
    :timeseries, Day(1), "Varies by site (many decades)",
    "Public Domain",
    "https://waterservices.usgs.gov/docs/";
    load_packages = Dict("DataFrames" => "a93c6f00-e57d-5684-b7b6-d8193f3e46c0"),
)

#--------------------------------------------------------------------------------# DataAccessPlan

function DataAccessPlan(source::USGSWaterServices, extent, start_date::Date, stop_date::Date;
                        variables::Vector{Symbol} = [Symbol("00060")],
                        sites::Vector{String} = String[],
                        statCd::String = "00003")
    isempty(sites) && error("USGS Water Services requires site numbers via `sites` keyword. " *
                            "Example: `sites=[\"01646500\"]`")
    base_url = source.service == "iv" ? USGS_WATER_IV_URL : USGS_WATER_DV_URL
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

    request = RequestInfo(source, url, :GET, "$(length(sites)) site(s), $n_days days")

    kwargs = Dict{Symbol, Any}(:sites => sites, :service => source.service)
    source.service == "dv" && (kwargs[:statCd] = statCd)

    DataAccessPlan(source, [request], "$(length(sites)) site(s): $(join(sites, ", "))",
        (start_date, stop_date), variables, kwargs,
        _estimate_bytes(total_rows, length(variables)))
end
