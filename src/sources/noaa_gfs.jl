#--------------------------------------------------------------------------------# NOAA GFS

"""
    NOAAGFS()

Global Forecast System (GFS) data from [NOAA NOMADS](https://nomads.ncep.noaa.gov/).
Downloads subsetted GRIB2 files via the NOMADS grib filter, which allows selecting
specific variables, levels, and spatial subregions.

- **Coverage**: Global, 0.25° (~25 km) resolution
- **Forecast**: 4 cycles/day (00z, 06z, 12z, 18z), out to 240 hours
- **API Key**: None required
- **Rate Limit**: ~10 second delay between requests recommended
- **Response Format**: GRIB2 binary

Each forecast hour produces a separate GRIB2 file.  Use `fetch(plan)` to download files.
Loading GRIB2 data requires the `Rasters` package extension.

### Examples

```julia
using GeoInterface.Extents: Extent

# Temperature and wind at 2m for the eastern US, current cycle, analysis hour
plan = DataAccessPlan(NOAAGFS(),
    Extent(X=(-120.0, -80.0), Y=(30.0, 50.0));
    variables = [:TMP, :UGRD, :VGRD],
    levels = ["2_m_above_ground", "10_m_above_ground"],
    forecast_hours = [0, 3, 6])
files = fetch(plan)

# Specific model run
plan = DataAccessPlan(NOAAGFS(),
    Extent(X=(-100.0, -90.0), Y=(35.0, 45.0));
    run_date = Date(2025, 6, 15),
    cycle = 12,
    variables = [:TMP, :RH],
    levels = ["surface"],
    forecast_hours = [0])
```
"""
struct NOAAGFS <: AbstractDataSource end

_register_source!(NOAAGFS())

const GFS_BASE_URL = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"

const GFS_VARIABLES = Dict{Symbol, String}(
    :TMP    => "Temperature (K)",
    :UGRD   => "U-component of wind (m/s)",
    :VGRD   => "V-component of wind (m/s)",
    :RH     => "Relative humidity (%)",
    :PRATE  => "Precipitation rate (kg/m²/s)",
    :PRMSL  => "Pressure reduced to MSL (Pa)",
    :APCP   => "Total precipitation (kg/m²)",
    :DSWRF  => "Downward short-wave radiation flux (W/m²)",
    :DLWRF  => "Downward long-wave radiation flux (W/m²)",
    :CAPE   => "Convective Available Potential Energy (J/kg)",
    :CIN    => "Convective Inhibition (J/kg)",
    :PWAT   => "Precipitable water (kg/m²)",
    :HGT    => "Geopotential height (gpm)",
    :ABSV   => "Absolute vorticity (1/s)",
    :CLWMR  => "Cloud mixing ratio (kg/kg)",
    :TCDC   => "Total cloud cover (%)",
)

const GFS_LEVELS = [
    "surface",
    "2_m_above_ground",
    "10_m_above_ground",
    "entire_atmosphere",
    "mean_sea_level",
    "1000_mb", "975_mb", "950_mb", "925_mb", "900_mb",
    "850_mb", "800_mb", "750_mb", "700_mb", "650_mb",
    "600_mb", "550_mb", "500_mb", "450_mb", "400_mb",
    "350_mb", "300_mb", "250_mb", "200_mb", "150_mb",
    "100_mb", "70_mb", "50_mb", "30_mb", "20_mb", "10_mb",
]

#--------------------------------------------------------------------------------# MetaData

MetaData(::NOAAGFS) = MetaData(
    "", "~10 sec between requests",
    :weather, GFS_VARIABLES,
    :raster, "0.25° (~25 km)", "Global",
    :forecast, nothing, "Forecast (4 cycles/day, 0–240 h)",
    "Public Domain",
    "https://nomads.ncep.noaa.gov/";
    load_packages = Dict("Rasters" => "a3a2b9e3-a471-40c9-b274-f788e487c689"),
)

#--------------------------------------------------------------------------------# DataAccessPlan

function DataAccessPlan(source::NOAAGFS, extent;
                        run_date::Date = today(),
                        cycle::Int = 0,
                        forecast_hours::Vector{Int} = [0],
                        variables::Vector{Symbol} = [:TMP, :UGRD, :VGRD],
                        levels::Vector{String} = ["2_m_above_ground", "10_m_above_ground"])
    cycle in (0, 6, 12, 18) || error("cycle must be 0, 6, 12, or 18 (got $cycle)")
    all(h -> 0 <= h <= 240, forecast_hours) || error("forecast_hours must be in 0:240")

    leftlon, bottomlat, rightlon, toplat = _gfs_bbox(extent)
    date_str = Dates.format(run_date, dateformat"yyyymmdd")
    cc = lpad(cycle, 2, '0')

    requests = RequestInfo[]
    for fh in forecast_hours
        fff = lpad(fh, 3, '0')
        file = "gfs.t$(cc)z.pgrb2.0p25.f$fff"
        dir = "/gfs.$date_str/$cc/atmos"

        params = ["file=$file", "dir=$dir",
                  "subregion=",
                  "leftlon=$leftlon", "rightlon=$rightlon",
                  "toplat=$toplat", "bottomlat=$bottomlat"]
        for v in variables
            push!(params, "var_$(v)=on")
        end
        for l in levels
            push!(params, "lev_$(l)=on")
        end

        url = GFS_BASE_URL * "?" * join(params, "&")
        desc = "GFS $date_str $(cc)z f$fff"
        push!(requests, RequestInfo(source, url, :GET, desc; ext=".grb2"))
    end

    extent_desc = _describe_extent(extent)
    kwargs = Dict{Symbol, Any}(
        :run_date => run_date,
        :cycle => cycle,
        :forecast_hours => forecast_hours,
        :levels => levels,
    )

    DataAccessPlan(source, requests, extent_desc,
        nothing, variables, kwargs,
        length(forecast_hours) * length(variables) * length(levels) * 50_000)
end

#--------------------------------------------------------------------------------# Helpers

function _gfs_bbox(extent)
    trait = GI.geomtrait(extent)
    _gfs_bbox(trait, extent)
end

function _gfs_bbox(::GI.PointTrait, geom)
    lon, lat = GI.x(geom), GI.y(geom)
    (lon - 0.5, lat - 0.5, lon + 0.5, lat + 0.5)
end

function _gfs_bbox(::GI.AbstractPolygonTrait, geom)
    _gfs_bbox(nothing, GI.extent(geom))
end

function _gfs_bbox(::Nothing, geom)
    if hasproperty(geom, :X) && hasproperty(geom, :Y)
        xmin, xmax = geom.X
        ymin, ymax = geom.Y
        return (xmin, ymin, xmax, ymax)
    end
    error("Cannot extract bounding box from $(typeof(geom)) for NOAA GFS.")
end

function _gfs_bbox(::Union{GI.MultiPointTrait, GI.AbstractCurveTrait}, geom)
    points = collect(GI.getpoint(geom))
    lons = [GI.x(p) for p in points]
    lats = [GI.y(p) for p in points]
    (minimum(lons), minimum(lats), maximum(lons), maximum(lats))
end
