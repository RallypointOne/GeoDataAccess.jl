#--------------------------------------------------------------------------------# NASA FIRMS

"""
    NASAFIRMS()

Active fire hotspot data from the [NASA FIRMS](https://firms.modaps.eosdis.nasa.gov/)
(Fire Information for Resource Management System).  Aggregates detections from MODIS and
VIIRS satellite instruments.

- **Coverage**: Global, near real-time + archive
- **Resolution**: 375 m (VIIRS) / 1 km (MODIS)
- **API Key**: Required (`FIRMS_MAP_KEY` environment variable)
- **Rate Limit**: 5,000 requests per 10 minutes
- **Response Format**: CSV

Supports bounding box queries and country-code queries.  Requests longer than 10 days are
automatically split into 10-day chunks.

Register for a free MAP_KEY at <https://firms.modaps.eosdis.nasa.gov/api/map_key/>.

### Examples

```julia
ENV["FIRMS_MAP_KEY"] = "your-map-key"

using GeoInterface.Extents: Extent

# VIIRS fire detections in California, 5 days
plan = DataAccessPlan(NASAFIRMS(),
    Extent(X=(-125.0, -114.0), Y=(32.0, 42.0)),
    Date(2024, 7, 1), Date(2024, 7, 5))
files = fetch!(plan)

# Country-level query
plan = DataAccessPlan(NASAFIRMS(), (0.0, 0.0),  # extent ignored for country queries
    Date(2024, 7, 1), Date(2024, 7, 3);
    country = "AUS")
```
"""
struct NASAFIRMS <: AbstractDataSource end

_register_source!(NASAFIRMS())

const FIRMS_AREA_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
const FIRMS_COUNTRY_URL = "https://firms.modaps.eosdis.nasa.gov/api/country/csv"

const FIRMS_VARIABLES = Dict{Symbol, String}(
    :latitude    => "Center latitude of fire pixel (°)",
    :longitude   => "Center longitude of fire pixel (°)",
    :bright_ti4  => "VIIRS I-4 brightness temperature (K)",
    :bright_ti5  => "VIIRS I-5 brightness temperature (K)",
    :frp         => "Fire Radiative Power (MW)",
    :confidence  => "Detection confidence",
    :acq_date    => "Acquisition date (YYYY-MM-DD)",
    :acq_time    => "Acquisition time UTC (HHMM)",
    :satellite   => "Satellite platform",
    :daynight    => "Day (D) or Night (N) observation",
    :scan        => "Along-scan pixel size (km)",
    :track       => "Along-track pixel size (km)",
)

#--------------------------------------------------------------------------------# MetaData

MetaData(::NASAFIRMS) = MetaData(
    "FIRMS_MAP_KEY", "5,000 req/10 min",
    :natural_hazards, FIRMS_VARIABLES,
    :point, "375 m (VIIRS) / 1 km (MODIS)", "Global",
    :timeseries, nothing, "Near real-time + archive",
    "NASA EOSDIS",
    "https://firms.modaps.eosdis.nasa.gov/api/",
)

#--------------------------------------------------------------------------------# DataAccessPlan

function DataAccessPlan(source::NASAFIRMS, extent, start_date::Date, stop_date::Date;
                        satellite::String = "VIIRS_SNPP_NRT",
                        country::String = "")
    api_key = _get_api_key(source)
    n_days = Dates.value(stop_date - start_date) + 1
    n_days < 1 && error("stop_date must be on or after start_date")

    if !isempty(country)
        base_url = FIRMS_COUNTRY_URL
        spatial_param = country
        extent_desc = "Country: $country"
    else
        base_url = FIRMS_AREA_URL
        west, south, east, north = _firms_bbox(extent)
        spatial_param = "$west,$south,$east,$north"
        extent_desc = _describe_extent(extent)
    end

    # Chunk into ≤10-day segments (API limit)
    requests = RequestInfo[]
    current = start_date
    while current <= stop_date
        remaining = Dates.value(stop_date - current) + 1
        chunk = min(remaining, 10)
        date_str = Dates.format(current, dateformat"yyyy-mm-dd")
        url = "$base_url/$api_key/$satellite/$spatial_param/$chunk/$date_str"
        push!(requests, RequestInfo(source, url, :GET,
            "$satellite, $chunk days from $date_str"))
        current += Day(chunk)
    end

    kwargs = Dict{Symbol, Any}(:satellite => satellite)
    !isempty(country) && (kwargs[:country] = country)

    DataAccessPlan(source, requests, extent_desc,
        (start_date, stop_date), collect(keys(FIRMS_VARIABLES)), kwargs,
        _estimate_bytes(n_days * 100, length(FIRMS_VARIABLES)))
end

#--------------------------------------------------------------------------------# Helpers

function _firms_bbox(extent)
    trait = GI.geomtrait(extent)
    _firms_bbox(trait, extent)
end

function _firms_bbox(::GI.PointTrait, geom)
    lon, lat = GI.x(geom), GI.y(geom)
    (lon - 0.5, lat - 0.5, lon + 0.5, lat + 0.5)
end

function _firms_bbox(::GI.AbstractPolygonTrait, geom)
    _firms_bbox(nothing, GI.extent(geom))
end

function _firms_bbox(::Nothing, geom)
    if hasproperty(geom, :X) && hasproperty(geom, :Y)
        xmin, xmax = geom.X
        ymin, ymax = geom.Y
        return (xmin, ymin, xmax, ymax)
    end
    error("Cannot extract bounding box from $(typeof(geom)) for NASA FIRMS.")
end

function _firms_bbox(::Union{GI.MultiPointTrait, GI.AbstractCurveTrait}, geom)
    points = collect(GI.getpoint(geom))
    lons = [GI.x(p) for p in points]
    lats = [GI.y(p) for p in points]
    (minimum(lons), minimum(lats), maximum(lons), maximum(lats))
end
