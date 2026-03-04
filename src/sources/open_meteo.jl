#--------------------------------------------------------------------------------# OpenMeteo

"""
    OpenMeteoArchive()

Global historical weather data from the ERA5 reanalysis dataset via [Open-Meteo](https://open-meteo.com/).

- **Coverage**: Global, 1940–present
- **Resolution**: ~25 km, hourly or daily
- **API Key**: Not required
- **Rate Limit**: 10,000 calls/day (non-commercial)

### Examples

```julia
plan = DataAccessPlan(OpenMeteoArchive(), (-74.0, 40.7),
    Date(2024, 1, 1), Date(2024, 1, 7);
    variables = [:temperature_2m, :precipitation],
    frequency = :hourly)
files = fetch(plan)
```
"""
struct OpenMeteoArchive <: AbstractDataSource end

"""
    OpenMeteoForecast()

Up to 16-day global weather forecast via [Open-Meteo](https://open-meteo.com/).

- **Coverage**: Global
- **Resolution**: ~9 km, hourly or daily
- **API Key**: Not required
- **Rate Limit**: 10,000 calls/day (non-commercial)

Shares the same variable set as [`OpenMeteoArchive`](@ref).

### Examples

```julia
plan = DataAccessPlan(OpenMeteoForecast(), (-74.0, 40.7),
    today(), today() + Day(3);
    variables = [:temperature_2m, :precipitation],
    frequency = :hourly)
files = fetch(plan)
```
"""
struct OpenMeteoForecast <: AbstractDataSource end

_register_source!(OpenMeteoArchive())
_register_source!(OpenMeteoForecast())

const OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
const OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

const OPEN_METEO_VARIABLES = Dict{Symbol, String}(
    :temperature_2m             => "Air temperature at 2m (°C)",
    :relative_humidity_2m       => "Relative humidity at 2m (%)",
    :dew_point_2m               => "Dew point at 2m (°C)",
    :apparent_temperature       => "Apparent (feels-like) temperature (°C)",
    :precipitation              => "Total precipitation (mm)",
    :rain                       => "Rain (mm)",
    :snowfall                   => "Snowfall (cm)",
    :snow_depth                 => "Snow depth (m)",
    :pressure_msl               => "Mean sea level pressure (hPa)",
    :surface_pressure           => "Surface pressure (hPa)",
    :cloud_cover                => "Total cloud cover (%)",
    :wind_speed_10m             => "Wind speed at 10m (km/h)",
    :wind_direction_10m         => "Wind direction at 10m (°)",
    :wind_gusts_10m             => "Wind gusts at 10m (km/h)",
    :shortwave_radiation        => "Shortwave radiation (W/m²)",
    :weather_code               => "WMO weather code",
    :et0_fao_evapotranspiration => "Reference evapotranspiration (mm)",
    :soil_temperature_0_to_7cm  => "Soil temperature 0-7cm (°C)",
    :soil_moisture_0_to_7cm     => "Soil moisture 0-7cm (m³/m³)",
    :temperature_2m_max         => "Daily maximum temperature at 2m (°C)",
    :temperature_2m_min         => "Daily minimum temperature at 2m (°C)",
    :temperature_2m_mean        => "Daily mean temperature at 2m (°C)",
    :apparent_temperature_max   => "Daily maximum apparent temperature (°C)",
    :apparent_temperature_min   => "Daily minimum apparent temperature (°C)",
    :precipitation_sum          => "Daily total precipitation (mm)",
    :rain_sum                   => "Daily total rain (mm)",
    :snowfall_sum               => "Daily total snowfall (cm)",
    :wind_speed_10m_max         => "Daily maximum wind speed at 10m (km/h)",
    :wind_gusts_10m_max         => "Daily maximum wind gusts at 10m (km/h)",
    :wind_direction_10m_dominant => "Daily dominant wind direction (°)",
    :precipitation_hours        => "Daily hours with precipitation",
    :sunrise                    => "Sunrise time (ISO8601)",
    :sunset                     => "Sunset time (ISO8601)",
    :sunshine_duration          => "Sunshine duration (s)",
    :shortwave_radiation_sum    => "Daily total shortwave radiation (MJ/m²)",
)

#--------------------------------------------------------------------------------# MetaData

MetaData(::OpenMeteoArchive) = MetaData(
    "", "10,000 calls/day (non-commercial)",
    :weather, OPEN_METEO_VARIABLES,
    :raster, "25 km", "Global",
    :timeseries, Hour(1), "1940-present",
    "CC BY 4.0",
    "https://open-meteo.com/en/docs/historical-weather-api";
    load_packages = Dict("DataFrames" => "a93c6f00-e57d-5684-b7b6-d8193f3e46c0"),
)

MetaData(::OpenMeteoForecast) = MetaData(
    "", "10,000 calls/day (non-commercial)",
    :weather, OPEN_METEO_VARIABLES,
    :raster, "9 km", "Global",
    :forecast, Hour(1), "16-day forecast",
    "CC BY 4.0",
    "https://open-meteo.com/en/docs";
    load_packages = Dict("DataFrames" => "a93c6f00-e57d-5684-b7b6-d8193f3e46c0"),
)

#--------------------------------------------------------------------------------# DataAccessPlan

function DataAccessPlan(source::Union{OpenMeteoArchive, OpenMeteoForecast}, extent,
                        start_date::Date, stop_date::Date;
                        variables::Vector{Symbol} = [:temperature_2m, :precipitation],
                        frequency::Symbol = :hourly,
                        timezone::String = "GMT",
                        past_days::Int = 0)
    frequency in (:hourly, :daily) || error("frequency must be :hourly or :daily, got :$frequency")
    lats, lons = _extent_to_latlon(extent)
    n_points = count(',', lats) + 1

    base_url = source isa OpenMeteoArchive ? OPEN_METEO_ARCHIVE_URL : OPEN_METEO_FORECAST_URL
    params = Dict{String, String}(
        "latitude" => lats,
        "longitude" => lons,
        "start_date" => Dates.format(start_date, dateformat"yyyy-mm-dd"),
        "end_date" => Dates.format(stop_date, dateformat"yyyy-mm-dd"),
        "timezone" => timezone,
        string(frequency) => join(string.(variables), ","),
    )
    if source isa OpenMeteoForecast && past_days > 0
        params["past_days"] = string(past_days)
    end
    url = _build_url(base_url, params)

    n_days = Dates.value(stop_date - start_date) + 1
    rows_per_point = frequency == :hourly ? n_days * 24 : n_days
    total_rows = rows_per_point * n_points

    request = RequestInfo(source, url, :GET, "$n_points point(s), $frequency $n_days days")

    kwargs = Dict{Symbol, Any}(:frequency => frequency, :timezone => timezone)
    if source isa OpenMeteoForecast && past_days > 0
        kwargs[:past_days] = past_days
    end

    DataAccessPlan(source, [request], _describe_extent(extent),
        (start_date, stop_date), variables, kwargs,
        _estimate_bytes(total_rows, length(variables)))
end

