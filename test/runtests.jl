using GeoDataAccess
using GeoDataAccess: Cache, MetaData, DataAccessPlan, RequestInfo, fetch, name,
                     all_sources, available_sources, has_api_key, is_available,
                     Domain, SpatialType, TemporalType, Frequency, QueryType,
                     HTTPMethod, EPAService, HRRRLevel, HRRRRunType,
                     WEATHER, TERRAIN, AIR_QUALITY, HYDROLOGY, NATURAL_HAZARDS, INFRASTRUCTURE,
                     RASTER, POINT, VECTOR_FEATURE,
                     LazyData, prefetch!, status, reset!,
                     AbstractZarrSource, store_url,
                     OpenMeteoArchive, OpenMeteoForecast, NOAANCEI, NASAPower,
                     TomorrowIO, VisualCrossing, USGSEarthquake, USGSWaterServices,
                     OpenAQ, NASAFIRMS, EPAAQS, NOAAGFS, ERA5, CopernicusDEM,
                     OpenStreetMap, NOAAOISST,
                     ARCOERA5, NASAPowerZarr, GFSZarr, HRRRZarr
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
        @test any(s -> s isa OpenMeteoArchive.Source, sources)
        @test any(s -> s isa OpenMeteoForecast.Source, sources)
        @test any(s -> s isa NOAANCEI.Source, sources)
        @test any(s -> s isa NASAPower.Source, sources)
        @test any(s -> s isa TomorrowIO.Source, sources)
        @test any(s -> s isa VisualCrossing.Source, sources)
        @test any(s -> s isa ARCOERA5.Source, sources)
        @test any(s -> s isa NASAPowerZarr.Source, sources)
        @test any(s -> s isa GFSZarr.Source, sources)
        @test any(s -> s isa HRRRZarr.Source, sources)

        # Zarr sources are a subtype of AbstractDataSource
        zarr_sources = filter(s -> s isa AbstractZarrSource, sources)
        @test length(zarr_sources) == 4

        weather_sources = all_sources(domain=WEATHER)
        @test length(weather_sources) >= 5
        @test all(s -> MetaData(s).domain == WEATHER, weather_sources)
    end

    #------------------------------------------------------------------------# MetaData
    @testset "MetaData" begin
        @testset "OpenMeteoArchive" begin
            m = MetaData(OpenMeteoArchive.Source())
            @test m.api_key_env_var == ""
            @test m.domain == WEATHER
            @test m.spatial_type == RASTER
            @test m.spatial_resolution == "25 km"
            @test m.coverage == "Global"
            @test m.temporal_type == TemporalType.timeseries
            @test m.temporal_resolution == Hour(1)
            @test haskey(m.variables, :temperature_2m)
            @test haskey(m.variables, :precipitation)
        end
        @testset "OpenMeteoForecast" begin
            m = MetaData(OpenMeteoForecast.Source())
            @test m.api_key_env_var == ""
            @test m.spatial_resolution == "9 km"
            @test m.temporal_type == TemporalType.forecast
        end
        @testset "NOAANCEI" begin
            m = MetaData(NOAANCEI.Source())
            @test m.api_key_env_var == ""
            @test m.spatial_type == POINT
            @test m.temporal_resolution == Day(1)
            @test haskey(m.variables, :TMAX)
        end
        @testset "NASAPower" begin
            m = MetaData(NASAPower.Source())
            @test m.api_key_env_var == ""
            @test m.domain == WEATHER
            @test m.spatial_resolution == "55 km"
            @test m.temporal_resolution == Day(1)
            @test haskey(m.variables, :T2M)
            @test haskey(m.variables, :PRECTOTCORR)
        end
        @testset "TomorrowIO" begin
            m = MetaData(TomorrowIO.Source())
            @test m.api_key_env_var == "TOMORROW_IO_API_KEY"
            @test has_api_key(TomorrowIO.Source())
            @test haskey(m.variables, :temperature)
            @test haskey(m.variables, :humidity)
        end
        @testset "VisualCrossing" begin
            m = MetaData(VisualCrossing.Source())
            @test m.api_key_env_var == "VISUAL_CROSSING_API_KEY"
            @test has_api_key(VisualCrossing.Source())
            @test haskey(m.variables, :tempmax)
            @test haskey(m.variables, :precip)
        end
        @testset "CopernicusDEM" begin
            m = MetaData(CopernicusDEM.Source())
            @test m.api_key_env_var == ""
            @test m.domain == TERRAIN
            @test m.spatial_type == RASTER
            @test m.spatial_resolution == "30 m"
            @test m.coverage == "Global"
            @test m.temporal_type == TemporalType.snapshot
            @test m.temporal_resolution === nothing
            @test haskey(m.variables, :elevation)

            m90 = MetaData(CopernicusDEM.Source(resolution=90))
            @test m90.spatial_resolution == "90 m"

            @test_throws ErrorException CopernicusDEM.Source(resolution=10)
        end
        @testset "OpenStreetMap" begin
            m = MetaData(OpenStreetMap.Source())
            @test m.api_key_env_var == ""
            @test m.domain == INFRASTRUCTURE
            @test m.spatial_type == VECTOR_FEATURE
            @test m.license == "ODbL 1.0"
            @test m.temporal_type == TemporalType.snapshot
            @test haskey(m.variables, :building)
            @test haskey(m.variables, :highway)
        end
        @testset "NOAAOISST" begin
            m = MetaData(NOAAOISST.Source())
            @test m.api_key_env_var == ""
            @test m.domain == WEATHER
            @test m.spatial_type == RASTER
            @test m.spatial_resolution == "0.25° (~25 km)"
            @test m.coverage == "Global"
            @test m.temporal_type == TemporalType.timeseries
            @test m.temporal_resolution == Day(1)
            @test m.license == "Public Domain"
            @test haskey(m.variables, :sst)
            @test haskey(m.variables, :anom)
            @test haskey(m.variables, :ice)
            @test haskey(m.variables, :err)
        end
        @testset "ARCOERA5" begin
            m = MetaData(ARCOERA5.Source())
            @test m.api_key_env_var == ""
            @test m.domain == WEATHER
            @test m.spatial_type == RASTER
            @test m.temporal_type == TemporalType.timeseries
            @test m.temporal_resolution == Hour(1)
            @test haskey(m.variables, Symbol("2m_temperature"))
            @test haskey(m.variables, Symbol("total_precipitation"))
            @test ARCOERA5.Source() isa AbstractZarrSource
            @test occursin("gcp-public-data-arco-era5", store_url(ARCOERA5.Source()))
        end
        @testset "NASAPowerZarr" begin
            m = MetaData(NASAPowerZarr.Source())
            @test m.api_key_env_var == ""
            @test m.domain == WEATHER
            @test m.spatial_type == RASTER
            @test m.temporal_resolution == Day(1)
            @test haskey(m.variables, :T2M)
            @test haskey(m.variables, :PRECTOTCORR)
            @test NASAPowerZarr.Source() isa AbstractZarrSource
            @test occursin("nasa-power", store_url(NASAPowerZarr.Source()))

            # Custom product/frequency/orientation (scoped enums)
            s = NASAPowerZarr.Source(
                product=NASAPowerZarr.Product.syn1deg,
                frequency=NASAPowerZarr.Frequency.hourly,
                orientation=NASAPowerZarr.Orientation.spatial)
            @test occursin("syn1deg", store_url(s))
            @test occursin("hourly", store_url(s))
            @test occursin("spatial", store_url(s))

            # Enum types are correct
            @test NASAPowerZarr.Source().product === NASAPowerZarr.Product.merra2
            @test NASAPowerZarr.Source().frequency === NASAPowerZarr.Frequency.daily
            @test NASAPowerZarr.Source().orientation === NASAPowerZarr.Orientation.temporal
        end
        @testset "GFSZarr" begin
            m = MetaData(GFSZarr.Source())
            @test m.api_key_env_var == ""
            @test m.domain == WEATHER
            @test m.spatial_type == RASTER
            @test m.temporal_type == TemporalType.forecast
            @test haskey(m.variables, :temperature_2m)
            @test haskey(m.variables, :wind_u_10m)
            @test GFSZarr.Source() isa AbstractZarrSource
            @test occursin("dynamical.org", store_url(GFSZarr.Source()))
        end
        @testset "HRRRZarr" begin
            m = MetaData(HRRRZarr.Source())
            @test m.api_key_env_var == ""
            @test m.domain == WEATHER
            @test m.spatial_type == RASTER
            @test m.spatial_resolution == "3 km"
            @test m.coverage == "CONUS"
            @test m.temporal_type == TemporalType.timeseries
            @test m.temporal_resolution == Hour(1)
            @test haskey(m.variables, :TMP)
            @test haskey(m.variables, :UGRD)
            @test haskey(m.variables, :REFC)
            @test HRRRZarr.Source() isa AbstractZarrSource

            # store_url requires date keyword
            @test_throws ErrorException store_url(HRRRZarr.Source())
            url = store_url(HRRRZarr.Source(); date="20230601", cycle="00",
                            run=HRRRRunType.anl, level="surface", variable="TMP")
            @test occursin("hrrrzarr", url)
            @test occursin("20230601", url)
            @test occursin("anl", url)
            @test occursin("surface/TMP/surface", url)

            # level kwarg
            @test HRRRZarr.Source(level=HRRRLevel.prs) isa AbstractZarrSource
            url_prs = store_url(HRRRZarr.Source(level=HRRRLevel.prs); date="20230601",
                                level="500mb", variable="HGT")
            @test occursin("prs/20230601", url_prs)
            @test_throws TypeError HRRRZarr.Source(level=:invalid)
        end
    end

    #------------------------------------------------------------------------# name
    @testset "name" begin
        @test name(OpenMeteoArchive.Source) == "openmeteoarchive"
        @test name(OpenMeteoForecast.Source) == "openmeteoforecast"
        @test name(NOAANCEI.Source) == "noaancei"
        @test name(NASAPower.Source) == "nasapower"
        @test name(TomorrowIO.Source) == "tomorrowio"
        @test name(VisualCrossing.Source) == "visualcrossing"
    end

    #------------------------------------------------------------------------# Cache
    @testset "Cache" begin
        @test isdir(Cache.dir())
        @test Cache.list() isa Vector{String}
    end

    #------------------------------------------------------------------------# DataAccessPlan
    @testset "DataAccessPlan" begin
        @testset "OpenMeteoArchive plan" begin
            plan = DataAccessPlan(OpenMeteoArchive.Source(), NYC,
                Date(2023, 1, 1), Date(2023, 1, 3);
                variables = [:temperature_2m, :precipitation],
                frequency = Frequency.hourly)
            @test plan isa DataAccessPlan
            @test plan.source isa OpenMeteoArchive.Source
            @test length(plan.requests) == 1
            @test plan.time_range == (Date(2023, 1, 1), Date(2023, 1, 3))
            @test plan.variables == [:temperature_2m, :precipitation]
            @test plan.estimated_bytes == 72 * 2 * 8  # 3 days * 24 hours * 2 vars * 8 bytes
            @test plan.estimated_bytes > 0
        end

        @testset "NASAPower plan - point" begin
            plan = DataAccessPlan(NASAPower.Source(), NYC,
                Date(2023, 1, 1), Date(2023, 1, 3);
                variables = [:T2M, :PRECTOTCORR])
            @test plan isa DataAccessPlan
            @test length(plan.requests) == 1
            @test plan.estimated_bytes == 3 * 2 * 8  # 3 days * 2 vars * 8 bytes
            @test plan.kwargs[:query_type] == QueryType.point
        end

        @testset "NASAPower plan - extent" begin
            ext = Extent(X=(-76.0, -73.0), Y=(39.0, 42.0))
            plan = DataAccessPlan(NASAPower.Source(), ext,
                Date(2023, 1, 1), Date(2023, 1, 3);
                variables = [:T2M])
            @test plan isa DataAccessPlan
            @test length(plan.requests) == 1
            @test plan.kwargs[:query_type] == QueryType.regional
        end

        @testset "NASAPower plan - multipoint" begin
            mp = GI.MultiPoint([(-74.0, 40.7), (-73.5, 40.8)])
            plan = DataAccessPlan(NASAPower.Source(), mp,
                Date(2023, 1, 1), Date(2023, 1, 3);
                variables = [:T2M])
            @test length(plan.requests) == 2
            @test plan.kwargs[:query_type] == QueryType.multi_point
        end

        @testset "NOAANCEI plan requires stations" begin
            @test_throws ErrorException DataAccessPlan(NOAANCEI.Source(), NYC,
                Date(2023, 1, 1), Date(2023, 1, 7))
        end

        @testset "CopernicusDEM plan - point" begin
            plan = DataAccessPlan(CopernicusDEM.Source(), NYC)
            @test plan isa DataAccessPlan
            @test plan.source isa CopernicusDEM.Source
            @test length(plan.requests) == 1
            @test plan.time_range === nothing
            @test plan.variables == [:elevation]
        end

        @testset "CopernicusDEM plan - extent" begin
            ext = Extent(X=(-76.0, -73.0), Y=(39.0, 42.0))
            plan = DataAccessPlan(CopernicusDEM.Source(), ext)
            @test plan isa DataAccessPlan
            @test length(plan.requests) == 4 * 4  # 4 lon tiles × 4 lat tiles
        end

        @testset "CopernicusDEM plan - 90m" begin
            plan = DataAccessPlan(CopernicusDEM.Source(resolution=90), NYC)
            @test length(plan.requests) == 1
            @test plan.kwargs[:resolution] == 90
            @test occursin("copernicus-dem-90m", plan.requests[1].url)
            @test occursin("COG_30", plan.requests[1].url)
        end

        @testset "OpenStreetMap plan - point" begin
            plan = DataAccessPlan(OpenStreetMap.Source(), NYC;
                variables = [:building])
            @test plan isa DataAccessPlan
            @test plan.source isa OpenStreetMap.Source
            @test length(plan.requests) == 1
            @test plan.time_range === nothing
            @test plan.variables == [:building]
            @test occursin("overpass", plan.requests[1].url)
        end

        @testset "OpenStreetMap plan - extent, multiple variables" begin
            ext = Extent(X=(-74.01, -73.99), Y=(40.70, 40.72))
            plan = DataAccessPlan(OpenStreetMap.Source(), ext;
                variables = [:building, :highway])
            @test length(plan.requests) == 2
            @test plan.variables == [:building, :highway]
        end

        @testset "NOAAOISST plan - point" begin
            plan = DataAccessPlan(NOAAOISST.Source(), NYC,
                Date(2024, 1, 1), Date(2024, 1, 3);
                variables = [:sst])
            @test plan isa DataAccessPlan
            @test plan.source isa NOAAOISST.Source
            @test length(plan.requests) == 3  # one per day
            @test plan.time_range == (Date(2024, 1, 1), Date(2024, 1, 3))
            @test plan.variables == [:sst]
            @test occursin("oisst-avhrr", plan.requests[1].url)
            @test occursin("20240101", plan.requests[1].url)
            @test occursin("20240103", plan.requests[3].url)
        end

        @testset "NOAAOISST plan - extent" begin
            ext = Extent(X=(-80.0, -60.0), Y=(30.0, 45.0))
            plan = DataAccessPlan(NOAAOISST.Source(), ext,
                Date(2024, 6, 1), Date(2024, 6, 2);
                variables = [:sst, :anom])
            @test length(plan.requests) == 2
            @test plan.variables == [:sst, :anom]
        end

        @testset "NOAAOISST plan - invalid dates" begin
            @test_throws ErrorException DataAccessPlan(NOAAOISST.Source(), NYC,
                Date(2024, 1, 5), Date(2024, 1, 1))
        end

        @testset "invalid frequency" begin
            @test_throws TypeError DataAccessPlan(OpenMeteoArchive.Source(), NYC,
                Date(2023, 1, 1), Date(2023, 1, 3); frequency = :monthly)
        end

        @testset "show method" begin
            plan = DataAccessPlan(OpenMeteoArchive.Source(), NYC,
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
            files = fetch(OpenMeteoArchive.Source(), NYC,
                Date(2023, 1, 1), Date(2023, 1, 3);
                variables = [:temperature_2m, :precipitation],
                frequency = Frequency.hourly)
            @test files isa Vector{String}
            @test length(files) == 1
            @test isfile(files[1])
        end

        @testset "via plan then fetch" begin
            plan = DataAccessPlan(OpenMeteoArchive.Source(), NYC,
                Date(2023, 1, 1), Date(2023, 1, 3);
                variables = [:temperature_2m_max], frequency = Frequency.daily)
            files = fetch(plan)
            @test files isa Vector{String}
            @test length(files) == 1
            @test isfile(files[1])
        end

        @testset "OpenMeteoForecast" begin
            files = fetch(OpenMeteoForecast.Source(), NYC,
                today(), today() + Day(2);
                variables = [:temperature_2m, :precipitation],
                frequency = Frequency.hourly)
            @test files isa Vector{String}
            @test length(files) == 1
            @test isfile(files[1])
        end

        @testset "NOAANCEI" begin
            files = fetch(NOAANCEI.Source(), NYC,
                Date(2023, 1, 1), Date(2023, 1, 7);
                stations = ["USW00094728"],
                variables = [:TMAX, :TMIN, :PRCP])
            @test files isa Vector{String}
            @test length(files) == 1
            @test isfile(files[1])
        end

        @testset "NASAPower point" begin
            files = fetch(NASAPower.Source(), NYC,
                Date(2023, 1, 1), Date(2023, 1, 7);
                variables = [:T2M, :PRECTOTCORR])
            @test files isa Vector{String}
            @test length(files) == 1
            @test isfile(files[1])
        end

        @testset "NASAPower extent" begin
            ext = Extent(X=(-76.0, -73.0), Y=(39.0, 42.0))
            files = fetch(NASAPower.Source(), ext,
                Date(2023, 1, 1), Date(2023, 1, 3);
                variables = [:T2M])
            @test files isa Vector{String}
            @test length(files) == 1
            @test isfile(files[1])
        end

        @testset "NASAPower multipoint" begin
            mp = GI.MultiPoint([(-74.0, 40.7), (-73.5, 40.8)])
            files = fetch(NASAPower.Source(), mp,
                Date(2023, 1, 1), Date(2023, 1, 3);
                variables = [:T2M])
            @test files isa Vector{String}
            @test length(files) == 2
            @test all(isfile, files)
        end


        @testset "OpenStreetMap" begin
            files = fetch(OpenStreetMap.Source(), NYC;
                variables = [:building])
            @test files isa Vector{String}
            @test length(files) == 1
            @test isfile(files[1])
        end

        @testset "NOAAOISST" begin
            files = fetch(NOAAOISST.Source(), NYC,
                Date(2024, 1, 1), Date(2024, 1, 1);
                variables = [:sst])
            @test files isa Vector{String}
            @test length(files) == 1
            @test isfile(files[1])
            @test endswith(files[1], ".nc")
        end
    end

    #------------------------------------------------------------------------# TomorrowIO (live, gated)
    if haskey(ENV, "TOMORROW_IO_API_KEY")
        @testset "TomorrowIO fetch (live)" begin
            files = fetch(TomorrowIO.Source(), NYC,
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
            files = fetch(VisualCrossing.Source(), NYC,
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
        @test isempty(Cache.list())

        fetch(OpenMeteoArchive.Source(), NYC,
            Date(2023, 6, 1), Date(2023, 6, 2);
            variables = [:temperature_2m])
        cached = Cache.list()
        @test length(cached) >= 1
        @test any(endswith(".json"), cached)

        # Same request uses cache (no new files)
        n_before = length(Cache.list())
        fetch(OpenMeteoArchive.Source(), NYC,
            Date(2023, 6, 1), Date(2023, 6, 2);
            variables = [:temperature_2m])
        @test length(Cache.list()) == n_before

        Cache.clear!()
        @test isempty(Cache.list())
    end

    #------------------------------------------------------------------------# Cache Retention
    @testset "Cache Retention" begin
        Cache.clear!()

        # default_retention on MetaData
        m_osm = MetaData(OpenStreetMap.Source())
        @test m_osm.default_retention == Day(7)

        m_gfs = MetaData(NOAAGFS.Source())
        @test m_gfs.default_retention == Day(1)

        m_archive = MetaData(OpenMeteoArchive.Source())
        @test m_archive.default_retention === nothing

        # retention field on DataAccessPlan - inherits from MetaData default
        plan_osm = DataAccessPlan(OpenStreetMap.Source(), NYC;
            variables = [:building])
        @test plan_osm.retention == Day(7)

        # retention field on DataAccessPlan - user override
        plan_osm2 = DataAccessPlan(OpenStreetMap.Source(), NYC;
            variables = [:building], retention = Day(30))
        @test plan_osm2.retention == Day(30)

        # retention=nothing when source has no default
        plan_archive = DataAccessPlan(OpenMeteoArchive.Source(), NYC,
            Date(2023, 9, 1), Date(2023, 9, 2);
            variables = [:temperature_2m])
        @test plan_archive.retention === nothing

        # fetch with retention writes to __retention__.toml
        files = fetch(plan_osm)
        @test length(files) >= 1
        @test isfile(files[1])
        @test isfile(Cache.dir(Cache.RETENTION_FILE))

        # retention is readable via TOML
        r = Cache._read_retention(files[1])
        @test r == Day(7)

        # __retention__.toml excluded from Cache.list()
        all_files = Cache.list()
        @test !any(f -> basename(f) == Cache.RETENTION_FILE, all_files)

        # Cache.table() returns correct structure
        tbl = Cache.table()
        @test tbl isa Vector
        @test length(tbl) >= 1
        entry = first(filter(e -> e.path == files[1], tbl))
        @test entry.path == files[1]
        @test entry.size > 0
        @test entry.retention == 7
        @test entry.retention_unit == Day

        # fetch without retention has nothing in table
        files2 = fetch(plan_archive)
        @test length(files2) >= 1
        entry2 = first(filter(e -> e.path == files2[1], Cache.table()))
        @test entry2.retention === nothing
        @test entry2.retention_unit === nothing

        # cleanup! with no expired files is a no-op
        n = length(Cache.list())
        result = Cache.cleanup!()
        @test result.removed == 0
        @test result.freed == 0
        @test length(Cache.list()) == n

        Cache.clear!()
    end

    #------------------------------------------------------------------------# LazyData
    @testset "LazyData" begin
        Cache.clear!()

        plan = DataAccessPlan(OpenMeteoArchive.Source(), NYC,
            Date(2023, 10, 1), Date(2023, 10, 3);
            variables = [:temperature_2m], frequency = Frequency.hourly)

        # Construction — no downloads happen
        data = LazyData(plan)
        @test length(data) == 1
        @test size(data) == (1,)
        @test firstindex(data) == 1
        @test lastindex(data) == 1

        # status before any access
        s = status(data)
        @test s.total == 1
        @test s.loaded == 0
        @test s.pending == 1

        # Indexing triggers download
        result = data[1]
        @test result isa Vector{UInt8}
        @test length(result) > 0

        # In-memory cache is populated
        s2 = status(data)
        @test s2.loaded == 1
        @test s2.on_disk == 1
        @test s2.pending == 0

        # Second access uses in-memory cache (same object)
        @test data[1] === result

        # reset! clears in-memory but keeps disk cache
        reset!(data)
        @test status(data).loaded == 0
        @test status(data).on_disk == 1

        # Re-access reads from disk cache
        result2 = data[1]
        @test result2 == result

        # Custom readfn
        data_str = LazyData(plan, readfn=path -> String(read(path)))
        @test data_str[1] isa String
        @test length(data_str[1]) > 0

        # show method
        buf = IOBuffer()
        show(buf, MIME("text/plain"), data)
        output = String(take!(buf))
        @test occursin("LazyData", output)
        @test occursin("chunks", output)

        # Multi-chunk source
        plan_multi = DataAccessPlan(NASAPower.Source(),
            GI.MultiPoint([(-74.0, 40.7), (-73.5, 40.8)]),
            Date(2023, 10, 1), Date(2023, 10, 3);
            variables = [:T2M])
        data_multi = LazyData(plan_multi)
        @test length(data_multi) == 2

        # Fetch only chunk 2
        r2 = data_multi[2]
        @test r2 isa Vector{UInt8}
        @test status(data_multi).loaded == 1  # only chunk 2

        # Range indexing
        all_data = data_multi[1:2]
        @test all_data isa Vector
        @test length(all_data) == 2
        @test status(data_multi).loaded == 2

        # Iteration
        data_iter = LazyData(plan_multi)
        collected = collect(data_iter)
        @test length(collected) == 2
        @test status(data_iter).loaded == 2

        # prefetch! downloads without reading
        Cache.clear!()
        data_pf = LazyData(plan)
        prefetch!(data_pf)
        @test status(data_pf).on_disk == 1
        @test status(data_pf).loaded == 0  # not read yet

        Cache.clear!()
    end
end
