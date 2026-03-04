[![CI](https://github.com/RallypointOne/GeoDataAccess.jl/actions/workflows/CI.yml/badge.svg)](https://github.com/RallypointOne/GeoDataAccess.jl/actions/workflows/CI.yml)
[![Docs Build](https://github.com/RallypointOne/GeoDataAccess.jl/actions/workflows/Docs.yml/badge.svg)](https://github.com/RallypointOne/GeoDataAccess.jl/actions/workflows/Docs.yml)
[![Stable Docs](https://img.shields.io/badge/docs-stable-blue)](https://RallypointOne.github.io/GeoDataAccess.jl/stable/)
[![Dev Docs](https://img.shields.io/badge/docs-dev-blue)](https://RallypointOne.github.io/GeoDataAccess.jl/dev/)

# GeoDataAccess.jl

A Julia package for collecting geospatial data from multiple public APIs. It provides a unified interface across weather, hydrology, seismology, air quality, and more — with built-in caching and [GeoInterface.jl](https://github.com/JuliaGeo/GeoInterface.jl) integration for flexible spatial queries.

## Data Sources

| Source | Type | Coverage | Resolution | API Key |
|--------|------|----------|------------|---------|
| `OpenMeteoArchive` | Raster (ERA5) | Global, 1940–present | Hourly/Daily, 25 km | No |
| `OpenMeteoForecast` | Raster | Global, 16-day forecast | Hourly/Daily, 9 km | No |
| `NOAANCEI` | Station-based | Global stations, 1763–present | Daily | No |
| `NASAPower` | Raster | Global, 1981–present | Daily, 55 km | No |
| `TomorrowIO` | Raster | Global, 2000–present | Hourly/Daily, 4 km | Yes |
| `VisualCrossing` | Raster | Global, ~50 years | Daily/Hourly, 1 km | Yes |
| `USGSEarthquake` | Event-based | Global catalog | Event-based | No |
| `USGSWaterServices` | Station-based | US, 1.5M+ sites | Daily/15-min | No |
| `OpenAQ` | Station-based | Global, 11K+ stations | Hourly/Daily | Yes |
| `NASAFIRMS` | Event-based | Global, near real-time | 375 m / 1 km | Yes |
| `EPAAQS` | Station-based | US stations | Hourly/Daily | Yes |
| `LandfireSource` | Raster | CONUS, AK, HI | 30 m | No (email) |
| `NOAAGFS` | Raster (GRIB2) | Global, forecast | 0.25° (~25 km) | No |
| `ERA5` | Raster (NetCDF/GRIB) | Global, 1940–present | 0.25° (~25 km) | Yes |

## Installation

```julia
using Pkg
Pkg.add("GeoDataAccess")
```

## Quickstart

The core workflow is: create a `DataAccessPlan`, inspect it, then `fetch` to download the data.

```julia
using GeoDataAccess
using GeoDataAccess: DataAccessPlan, fetch, OpenMeteoArchive
using Dates

# Create a plan: hourly temperature and precipitation for NYC
plan = DataAccessPlan(OpenMeteoArchive(), (-74.0, 40.7),
    Date(2024, 7, 1), Date(2024, 7, 3);
    variables = [:temperature_2m, :precipitation],
    frequency = :hourly)

# Execute the plan — returns file paths to cached data
files = fetch(plan)
```

Or skip the plan and call `fetch` directly on a source:

```julia
files = fetch(OpenMeteoArchive(), (-74.0, 40.7),
    Date(2024, 7, 1), Date(2024, 7, 3);
    variables = [:temperature_2m, :precipitation],
    frequency = :hourly)
```
