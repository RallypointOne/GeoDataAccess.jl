using GeoDataAccess
using GeoDataAccess: Cache, MetaData, DataAccessPlan, RequestInfo, fetch, name,
                   all_sources, available_sources, has_api_key, is_available
using Dates
using Test
import GeoInterface as GI
using GeoInterface.Extents: Extent

# GeoInterface treats tuples as (x, y) = (lon, lat)
const NYC = (-74.0, 40.7)

@testset "GeoDataAccess.jl" begin
    #------------------------------------------------------------------------# Source Registry
    @testset "Source Registry" begin
        sources = all_sources()
        @test length(sources) >= 5
        @test any(s -> s isa GeoDataAccess.OpenMeteoArchive, sources)
        @test any(s -> s isa GeoDataAccess.OpenMeteoForecast, sources)
        @test any(s -> s isa GeoDataAccess.NOAANCEI, sources)
        @test any(s -> s isa GeoDataAccess.NASAPower, sources)
        @test any(s -> s isa GeoDataAccess.TomorrowIO, sources)
        @test any(s -> s isa GeoDataAccess.VisualCrossing, sources)

        weather_sources = all_sources(domain=:weather)
        @test length(weather_sources) >= 5
        @test all(s -> MetaData(s).domain == :weather, weather_sources)
    end

    #------------------------------------------------------------------------# MetaData
    @testset "MetaData" begin
        @testset "OpenMeteoArchive" begin
            m = MetaData(GeoDataAccess.OpenMeteoArchive())
            @test m.api_key_env_var == ""
            @test m.domain == :weather
            @test m.spatial_type == :raster
            @test m.spatial_resolution == "25 km"
            @test m.coverage == "Global"
            @test m.temporal_type == :timeseries
            @test m.temporal_resolution == Hour(1)
            @test haskey(m.variables, :temperature_2m)
            @test haskey(m.variables, :precipitation)
        end
        @testset "OpenMeteoForecast" begin
            m = MetaData(GeoDataAccess.OpenMeteoForecast())
            @test m.api_key_env_var == ""
            @test m.spatial_resolution == "9 km"
            @test m.temporal_type == :forecast
        end
        @testset "NOAANCEI" begin
            m = MetaData(GeoDataAccess.NOAANCEI())
            @test m.api_key_env_var == ""
            @test m.spatial_type == :point
            @test m.temporal_resolution == Day(1)
            @test haskey(m.variables, :TMAX)
        end
        @testset "NASAPower" begin
            m = MetaData(GeoDataAccess.NASAPower())
            @test m.api_key_env_var == ""
            @test m.domain == :weather
            @test m.spatial_resolution == "55 km"
            @test m.temporal_resolution == Day(1)
            @test haskey(m.variables, :T2M)
            @test haskey(m.variables, :PRECTOTCORR)
        end
        @testset "TomorrowIO" begin
            m = MetaData(GeoDataAccess.TomorrowIO())
            @test m.api_key_env_var == "TOMORROW_IO_API_KEY"
            @test has_api_key(GeoDataAccess.TomorrowIO())
            @test haskey(m.variables, :temperature)
            @test haskey(m.variables, :humidity)
        end
        @testset "VisualCrossing" begin
            m = MetaData(GeoDataAccess.VisualCrossing())
            @test m.api_key_env_var == "VISUAL_CROSSING_API_KEY"
            @test has_api_key(GeoDataAccess.VisualCrossing())
            @test haskey(m.variables, :tempmax)
            @test haskey(m.variables, :precip)
        end
    end

    #------------------------------------------------------------------------# name
    @testset "name" begin
        @test name(GeoDataAccess.OpenMeteoArchive) == "openmeteoarchive"
        @test name(GeoDataAccess.OpenMeteoForecast) == "openmeteoforecast"
        @test name(GeoDataAccess.NOAANCEI) == "noaancei"
        @test name(GeoDataAccess.NASAPower) == "nasapower"
        @test name(GeoDataAccess.TomorrowIO) == "tomorrowio"
        @test name(GeoDataAccess.VisualCrossing) == "visualcrossing"
    end

    #------------------------------------------------------------------------# Cache
    @testset "Cache" begin
        @test Cache.ENABLED[] == true

        Cache.enable!(false)
        @test Cache.ENABLED[] == false
        Cache.enable!(true)
        @test Cache.ENABLED[] == true

        @test isdir(Cache.dir())
        @test Cache.list() isa Vector{String}
    end

    #------------------------------------------------------------------------# DataAccessPlan
    @testset "DataAccessPlan" begin
        @testset "OpenMeteoArchive plan" begin
            plan = DataAccessPlan(GeoDataAccess.OpenMeteoArchive(), NYC,
                Date(2023, 1, 1), Date(2023, 1, 3);
                variables = [:temperature_2m, :precipitation],
                frequency = :hourly)
            @test plan isa DataAccessPlan
            @test plan.source isa GeoDataAccess.OpenMeteoArchive
            @test length(plan.requests) == 1
            @test plan.time_range == (Date(2023, 1, 1), Date(2023, 1, 3))
            @test plan.variables == [:temperature_2m, :precipitation]
            @test plan.estimated_bytes == 72 * 2 * 8  # 3 days * 24 hours * 2 vars * 8 bytes
            @test plan.estimated_bytes > 0
        end

        @testset "NASAPower plan - point" begin
            plan = DataAccessPlan(GeoDataAccess.NASAPower(), NYC,
                Date(2023, 1, 1), Date(2023, 1, 3);
                variables = [:T2M, :PRECTOTCORR])
            @test plan isa DataAccessPlan
            @test length(plan.requests) == 1
            @test plan.estimated_bytes == 3 * 2 * 8  # 3 days * 2 vars * 8 bytes
            @test plan.kwargs[:query_type] == :point
        end

        @testset "NASAPower plan - extent" begin
            ext = Extent(X=(-76.0, -73.0), Y=(39.0, 42.0))
            plan = DataAccessPlan(GeoDataAccess.NASAPower(), ext,
                Date(2023, 1, 1), Date(2023, 1, 3);
                variables = [:T2M])
            @test plan isa DataAccessPlan
            @test length(plan.requests) == 1
            @test plan.kwargs[:query_type] == :regional
        end

        @testset "NASAPower plan - multipoint" begin
            mp = GI.MultiPoint([(-74.0, 40.7), (-73.5, 40.8)])
            plan = DataAccessPlan(GeoDataAccess.NASAPower(), mp,
                Date(2023, 1, 1), Date(2023, 1, 3);
                variables = [:T2M])
            @test length(plan.requests) == 2
            @test plan.kwargs[:query_type] == :multi_point
        end

        @testset "NOAANCEI plan requires stations" begin
            @test_throws ErrorException DataAccessPlan(GeoDataAccess.NOAANCEI(), NYC,
                Date(2023, 1, 1), Date(2023, 1, 7))
        end

        @testset "invalid frequency" begin
            @test_throws ErrorException DataAccessPlan(GeoDataAccess.OpenMeteoArchive(), NYC,
                Date(2023, 1, 1), Date(2023, 1, 3); frequency = :monthly)
        end

        @testset "show method" begin
            plan = DataAccessPlan(GeoDataAccess.OpenMeteoArchive(), NYC,
                Date(2023, 1, 1), Date(2023, 1, 3))
            buf = IOBuffer()
            show(buf, MIME("text/plain"), plan)
            output = String(take!(buf))
            @test occursin("DataAccessPlan", output)
            @test occursin("openmeteoarchive", output)
            @test occursin("API calls:", output)
        end
    end

    #------------------------------------------------------------------------# fetch returns file paths (live)
    @testset "fetch returns file paths (live)" begin
        @testset "OpenMeteoArchive" begin
            files = fetch(GeoDataAccess.OpenMeteoArchive(), NYC,
                Date(2023, 1, 1), Date(2023, 1, 3);
                variables = [:temperature_2m, :precipitation],
                frequency = :hourly)
            @test files isa Vector{String}
            @test length(files) == 1
            @test isfile(files[1])
        end

        @testset "via plan then fetch" begin
            plan = DataAccessPlan(GeoDataAccess.OpenMeteoArchive(), NYC,
                Date(2023, 1, 1), Date(2023, 1, 3);
                variables = [:temperature_2m_max], frequency = :daily)
            files = fetch(plan)
            @test files isa Vector{String}
            @test length(files) == 1
            @test isfile(files[1])
        end

        @testset "OpenMeteoForecast" begin
            files = fetch(GeoDataAccess.OpenMeteoForecast(), NYC,
                today(), today() + Day(2);
                variables = [:temperature_2m, :precipitation],
                frequency = :hourly)
            @test files isa Vector{String}
            @test length(files) == 1
            @test isfile(files[1])
        end

        @testset "NOAANCEI" begin
            files = fetch(GeoDataAccess.NOAANCEI(), NYC,
                Date(2023, 1, 1), Date(2023, 1, 7);
                stations = ["USW00094728"],
                variables = [:TMAX, :TMIN, :PRCP])
            @test files isa Vector{String}
            @test length(files) == 1
            @test isfile(files[1])
        end

        @testset "NASAPower point" begin
            files = fetch(GeoDataAccess.NASAPower(), NYC,
                Date(2023, 1, 1), Date(2023, 1, 7);
                variables = [:T2M, :PRECTOTCORR])
            @test files isa Vector{String}
            @test length(files) == 1
            @test isfile(files[1])
        end

        @testset "NASAPower extent" begin
            ext = Extent(X=(-76.0, -73.0), Y=(39.0, 42.0))
            files = fetch(GeoDataAccess.NASAPower(), ext,
                Date(2023, 1, 1), Date(2023, 1, 3);
                variables = [:T2M])
            @test files isa Vector{String}
            @test length(files) == 1
            @test isfile(files[1])
        end

        @testset "NASAPower multipoint" begin
            mp = GI.MultiPoint([(-74.0, 40.7), (-73.5, 40.8)])
            files = fetch(GeoDataAccess.NASAPower(), mp,
                Date(2023, 1, 1), Date(2023, 1, 3);
                variables = [:T2M])
            @test files isa Vector{String}
            @test length(files) == 2
            @test all(isfile, files)
        end
    end

    #------------------------------------------------------------------------# TomorrowIO (live, gated)
    if haskey(ENV, "TOMORROW_IO_API_KEY")
        @testset "TomorrowIO fetch (live)" begin
            files = fetch(GeoDataAccess.TomorrowIO(), NYC,
                Date(2023, 1, 1), Date(2023, 1, 3);
                variables = [:temperature, :humidity],
                timestep = "1d")
            @test files isa Vector{String}
            @test length(files) == 1
            @test isfile(files[1])
        end
    end

    #------------------------------------------------------------------------# VisualCrossing (live, gated)
    if haskey(ENV, "VISUAL_CROSSING_API_KEY")
        @testset "VisualCrossing fetch (live)" begin
            files = fetch(GeoDataAccess.VisualCrossing(), NYC,
                Date(2023, 1, 1), Date(2023, 1, 7);
                variables = [:tempmax, :tempmin, :precip])
            @test files isa Vector{String}
            @test length(files) == 1
            @test isfile(files[1])
        end
    end

    #------------------------------------------------------------------------# Caching
    @testset "Caching" begin
        Cache.clear!()
        Cache.enable!(true)
        @test isempty(Cache.list())

        fetch(GeoDataAccess.OpenMeteoArchive(), NYC,
            Date(2023, 6, 1), Date(2023, 6, 2);
            variables = [:temperature_2m])
        cached = Cache.list()
        @test length(cached) >= 1
        @test any(endswith(".json"), cached)

        n_before = length(Cache.list())
        fetch(GeoDataAccess.OpenMeteoArchive(), NYC,
            Date(2023, 6, 1), Date(2023, 6, 2);
            variables = [:temperature_2m])
        @test length(Cache.list()) == n_before

        Cache.enable!(false)
        fetch(GeoDataAccess.OpenMeteoArchive(), NYC,
            Date(2023, 7, 1), Date(2023, 7, 2);
            variables = [:temperature_2m])
        @test length(Cache.list()) == n_before

        Cache.enable!(true)
        Cache.clear!()
        @test isempty(Cache.list())
    end
end
