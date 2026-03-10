#--------------------------------------------------------------------------------# ERA5

module ERA5

import ..GeoDataAccess
using ..GeoDataAccess: AbstractDataSource, MetaData, DataAccessPlan, RequestInfo,
    _register_source!, _describe_extent, _get_api_key, Cache,
    TemporalType, HTTPMethod
using GeoDataAccess: JSON3, Downloads
using Dates
import GeoInterface as GI

"""
    ERA5.Source()

ERA5 global climate reanalysis data from ECMWF's
[Climate Data Store (CDS)](https://cds.climate.copernicus.eu/).

- **Coverage**: Global, 0.25° (~25 km) resolution
- **Temporal**: Hourly, 1940–present
- **API Key**: Required (`CDSAPI_KEY` environment variable)
- **Workflow**: Async job (submit → poll → download)

### Examples

```julia
using GeoInterface.Extents: Extent
using Dates

ext = Extent(X=(-10.0, 30.0), Y=(35.0, 60.0))
plan = DataAccessPlan(ERA5.Source(), ext, Date(2024, 1, 1), Date(2024, 1, 3);
    variables = [:temperature_2m, :total_precipitation])
files = fetch(plan)
```
"""
struct Source <: AbstractDataSource end

GeoDataAccess.name(::Type{Source}) = "era5"

const API_URL = "https://cds.climate.copernicus.eu/api/retrieve/v1"

const variables = (;
    temperature_2m             = "2m temperature (K)",
    dewpoint_temperature_2m    = "2m dewpoint temperature (K)",
    u_wind_10m                 = "10m u-component of wind (m/s)",
    v_wind_10m                 = "10m v-component of wind (m/s)",
    total_precipitation        = "Total precipitation (m)",
    surface_pressure           = "Surface pressure (Pa)",
    mean_sea_level_pressure    = "Mean sea level pressure (Pa)",
    skin_temperature           = "Skin temperature (K)",
    sea_surface_temperature    = "Sea surface temperature (K)",
    total_cloud_cover          = "Total cloud cover (0–1)",
    low_cloud_cover            = "Low cloud cover (0–1)",
    high_cloud_cover           = "High cloud cover (0–1)",
    surface_solar_radiation    = "Surface solar radiation downwards (J/m²)",
    surface_thermal_radiation  = "Surface thermal radiation downwards (J/m²)",
    snowfall                   = "Snowfall (m of water equivalent)",
    snow_depth                 = "Snow depth (m of water equivalent)",
    soil_temperature_level_1   = "Soil temperature level 1 (K)",
    volumetric_soil_water_1    = "Volumetric soil water layer 1 (m³/m³)",
    boundary_layer_height      = "Boundary layer height (m)",
    evaporation                = "Total evaporation (m of water equivalent)",
)

const CDS_NAMES = Dict{Symbol, String}(
    :temperature_2m             => "2m_temperature",
    :dewpoint_temperature_2m    => "2m_dewpoint_temperature",
    :u_wind_10m                 => "10m_u_component_of_wind",
    :v_wind_10m                 => "10m_v_component_of_wind",
    :total_precipitation        => "total_precipitation",
    :surface_pressure           => "surface_pressure",
    :mean_sea_level_pressure    => "mean_sea_level_pressure",
    :skin_temperature           => "skin_temperature",
    :sea_surface_temperature    => "sea_surface_temperature",
    :total_cloud_cover          => "total_cloud_cover",
    :low_cloud_cover            => "low_cloud_cover",
    :high_cloud_cover           => "high_cloud_cover",
    :surface_solar_radiation    => "surface_solar_radiation_downwards",
    :surface_thermal_radiation  => "surface_thermal_radiation_downwards",
    :snowfall                   => "snowfall",
    :snow_depth                 => "snow_depth",
    :soil_temperature_level_1   => "soil_temperature_level_1",
    :volumetric_soil_water_1    => "volumetric_soil_water_layer_1",
    :boundary_layer_height      => "boundary_layer_height",
    :evaporation                => "total_evaporation",
)

const metadata = MetaData(
    "CDSAPI_KEY", "1 concurrent job",
    GeoDataAccess.WEATHER, variables,
    GeoDataAccess.RASTER, "0.25° (~25 km)", "Global",
    TemporalType.timeseries, Dates.Hour(1), "1940–present",
    "CC BY 4.0",
    "https://cds.climate.copernicus.eu/";
    load_packages = Dict("Rasters" => "a3a2b9e3-a471-40c9-b274-f788e487c689"),
)

GeoDataAccess.MetaData(::Source) = metadata

GeoDataAccess._request_headers(::Source) = ["PRIVATE-TOKEN" => _get_api_key(Source())]

#--------------------------------------------------------------------------------# DataAccessPlan

function GeoDataAccess.DataAccessPlan(source::Source, extent, start_date::Date, stop_date::Date;
                                       variables::Vector{Symbol} = [:temperature_2m],
                                       dataset::String = "reanalysis-era5-single-levels",
                                       product_type::String = "reanalysis",
                                       times::Vector{String} = ["00:00", "06:00", "12:00", "18:00"],
                                       data_format::String = "netcdf",
                                       retention::Union{Nothing, Dates.Period} = metadata.default_retention)
    start_date <= stop_date || error("start_date must be <= stop_date")
    for v in variables
        haskey(CDS_NAMES, v) || error("Unknown ERA5 variable: $v. " *
            "Use one of: $(join(sort(collect(keys(CDS_NAMES))), ", "))")
    end

    north, west, south, east = _bbox(extent)
    cds_vars = [CDS_NAMES[v] for v in variables]

    requests = RequestInfo[]
    request_bodies = Dict{String, Any}[]
    cursor = start_date
    while cursor <= stop_date
        month_end = min(stop_date, lastdayofmonth(cursor))
        days = [lpad(d, 2, '0') for d in Dates.day(cursor):Dates.day(month_end)]

        body = Dict{String, Any}(
            "inputs" => Dict{String, Any}(
                "variable"     => cds_vars,
                "product_type" => [product_type],
                "year"         => [string(Dates.year(cursor))],
                "month"        => [lpad(Dates.month(cursor), 2, '0')],
                "day"          => days,
                "time"         => times,
                "data_format"  => data_format,
                "area"         => [north, west, south, east],
            ),
        )

        ext = data_format == "netcdf" ? ".nc" : ".grib"
        body_json = JSON3.write(body)
        synthetic_url = "$API_URL/processes/$dataset/execution#$(body_json)"
        desc = "ERA5 $(Dates.year(cursor))-$(lpad(Dates.month(cursor), 2, '0'))"
        push!(requests, RequestInfo(source, synthetic_url, HTTPMethod.POST, desc; ext))
        push!(request_bodies, body)

        cursor = month_end + Day(1)
    end

    extent_desc = _describe_extent(extent)
    kwargs = Dict{Symbol, Any}(
        :dataset => dataset,
        :product_type => product_type,
        :times => times,
        :data_format => data_format,
        :request_bodies => request_bodies,
    )

    n_timesteps = Dates.value(stop_date - start_date + Day(1)) * length(times)
    estimated = n_timesteps * length(variables) * 1_000_000

    DataAccessPlan(source, requests, extent_desc,
        (start_date, stop_date), variables, kwargs, estimated, retention)
end

#--------------------------------------------------------------------------------# fetch

function GeoDataAccess.fetch(plan::DataAccessPlan{Source})
    headers = GeoDataAccess._request_headers(plan.source)
    dataset = plan.kwargs[:dataset]::String
    bodies = plan.kwargs[:request_bodies]::Vector{Dict{String, Any}}

    paths = String[]
    for (req, body) in zip(plan.requests, bodies)
        cache_path = req.cache_path
        if isfile(cache_path)
            push!(paths, cache_path)
            continue
        end

        exec_url = "$API_URL/processes/$dataset/execution"
        job = _cds_post(exec_url, body, headers)
        job_id = string(job["jobID"])

        status_url = "$API_URL/jobs/$job_id"
        _cds_poll(status_url, headers)

        results_url = "$API_URL/jobs/$job_id/results"
        results = _cds_get_json(results_url, headers)
        download_url = results["asset"]["value"]["href"]

        mkpath(dirname(cache_path))
        Downloads.download(download_url, cache_path; headers)
        if !isnothing(plan.retention)
            Cache._write_retention(cache_path, plan.retention)
        end
        push!(paths, cache_path)
    end
    return paths
end

#--------------------------------------------------------------------------------# CDS API Helpers

function _cds_post(url::String, body::Dict, headers)
    body_json = JSON3.write(body)
    output = IOBuffer()
    resp = Downloads.request(url;
        method = "POST",
        headers = vcat(headers, ["Content-Type" => "application/json"]),
        input = IOBuffer(body_json),
        output = output,
    )
    resp.status == 200 || resp.status == 201 || resp.status == 202 ||
        error("CDS API error (HTTP $(resp.status)): $(String(take!(output)))")
    JSON3.read(String(take!(output)))
end

function _cds_get_json(url::String, headers)
    output = IOBuffer()
    resp = Downloads.request(url; headers, output)
    resp.status == 200 ||
        error("CDS API error (HTTP $(resp.status)): $(String(take!(output)))")
    JSON3.read(String(take!(output)))
end

function _cds_poll(url::String, headers; max_wait::Int = 3600, interval::Int = 10)
    elapsed = 0
    while elapsed < max_wait
        data = _cds_get_json(url, headers)
        status = string(data["status"])
        status == "successful" && return data
        status in ("failed", "rejected", "dismissed") &&
            error("CDS job $status: $(get(data, "message", "no details"))")
        sleep(interval)
        elapsed += interval
    end
    error("CDS job timed out after $(max_wait)s. Check status at $url")
end

#--------------------------------------------------------------------------------# Extent Helpers

function _bbox(extent)
    trait = GI.geomtrait(extent)
    _bbox(trait, extent)
end

function _bbox(::GI.PointTrait, geom)
    lon, lat = GI.x(geom), GI.y(geom)
    (lat + 0.5, lon - 0.5, lat - 0.5, lon + 0.5)  # N, W, S, E
end

function _bbox(::GI.AbstractPolygonTrait, geom)
    _bbox(nothing, GI.extent(geom))
end

function _bbox(::Nothing, geom)
    if hasproperty(geom, :X) && hasproperty(geom, :Y)
        xmin, xmax = geom.X
        ymin, ymax = geom.Y
        return (ymax, xmin, ymin, xmax)  # N, W, S, E
    end
    error("Cannot extract bounding box from $(typeof(geom)) for ERA5.")
end

function _bbox(::Union{GI.MultiPointTrait, GI.AbstractCurveTrait}, geom)
    points = collect(GI.getpoint(geom))
    lons = [GI.x(p) for p in points]
    lats = [GI.y(p) for p in points]
    (maximum(lats), minimum(lons), minimum(lats), maximum(lons))  # N, W, S, E
end

_register_source!(Source())

end # module ERA5
