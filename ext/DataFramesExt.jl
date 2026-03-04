module DataFramesExt

using GeoDataAccess
using DataFrames

import GeoDataAccess: DataAccessPlan, load, fetch,
    OpenMeteoArchive, OpenMeteoForecast, NOAANCEI, NASAPower, TomorrowIO,
    VisualCrossing, USGSEarthquake, USGSWaterServices, OpenAQ, EPAAQS

using GeoDataAccess: JSON3

#--------------------------------------------------------------------------------# OpenMeteo (Archive + Forecast)

function GeoDataAccess.load(plan::DataAccessPlan{<:Union{OpenMeteoArchive, OpenMeteoForecast}})
    files = fetch(plan)
    frequency = plan.kwargs[:frequency]
    dfs = DataFrame[]
    for (i, file) in enumerate(files)
        json = JSON3.read(read(file, String))
        data = json[frequency]
        cols = Dict{Symbol, Any}()
        for (k, v) in pairs(data)
            cols[Symbol(k)] = collect(v)
        end
        push!(dfs, DataFrame(cols))
    end
    length(dfs) == 1 ? dfs[1] : vcat(dfs...)
end

#--------------------------------------------------------------------------------# NOAA NCEI

function GeoDataAccess.load(plan::DataAccessPlan{NOAANCEI})
    files = fetch(plan)
    dfs = DataFrame[]
    for file in files
        json = JSON3.read(read(file, String))
        push!(dfs, DataFrame(json))
    end
    length(dfs) == 1 ? dfs[1] : vcat(dfs...; cols=:union)
end

#--------------------------------------------------------------------------------# NASA POWER

function GeoDataAccess.load(plan::DataAccessPlan{NASAPower})
    files = fetch(plan)
    query_type = get(plan.kwargs, :query_type, :point)
    dfs = DataFrame[]
    for (i, file) in enumerate(files)
        json = JSON3.read(read(file, String))
        if query_type == :regional
            # Regional: json.properties.parameter.{PARAM}.{YYYYMMDD} → value
            params = json.properties.parameter
            param_data = Dict{Symbol, Any}()
            dates = nothing
            for (param_name, date_dict) in pairs(params)
                date_keys = collect(keys(date_dict))
                vals = [date_dict[k] for k in date_keys]
                if isnothing(dates)
                    dates = [string(k) for k in date_keys]
                end
                param_data[Symbol(param_name)] = vals
            end
            df = DataFrame(param_data)
            insertcols!(df, 1, :date => dates)
            push!(dfs, df)
        else
            # Point / multi_point: json.properties.parameter.{PARAM}.{YYYYMMDD} → value
            params = json.properties.parameter
            param_data = Dict{Symbol, Any}()
            dates = nothing
            for (param_name, date_dict) in pairs(params)
                date_keys = collect(keys(date_dict))
                vals = [date_dict[k] for k in date_keys]
                if isnothing(dates)
                    dates = [string(k) for k in date_keys]
                end
                param_data[Symbol(param_name)] = vals
            end
            df = DataFrame(param_data)
            insertcols!(df, 1, :date => dates)
            if length(files) > 1
                insertcols!(df, 1, :point_index => i)
            end
            push!(dfs, df)
        end
    end
    length(dfs) == 1 ? dfs[1] : vcat(dfs...; cols=:union)
end

#--------------------------------------------------------------------------------# Tomorrow.io

function GeoDataAccess.load(plan::DataAccessPlan{TomorrowIO})
    files = fetch(plan)
    dfs = DataFrame[]
    for (i, file) in enumerate(files)
        json = JSON3.read(read(file, String))
        timelines = json.data.timelines
        for timeline in timelines
            for interval in timeline.intervals
                row = Dict{Symbol, Any}(:startTime => string(interval.startTime))
                for (k, v) in pairs(interval.values)
                    row[Symbol(k)] = v
                end
                push!(dfs, DataFrame([row]))
            end
        end
        if length(files) > 1
            # Tag rows from this file with point_index
            start_idx = length(dfs) - length(timelines) + 1
            # Actually we need to count intervals, not timelines
        end
    end
    # Simpler approach: build per-file DataFrames
    _tomorrow_io_load(files)
end

function _tomorrow_io_load(files)
    all_dfs = DataFrame[]
    for (i, file) in enumerate(files)
        json = JSON3.read(read(file, String))
        timelines = json.data.timelines
        rows = Dict{Symbol, Vector}()
        first_row = true
        for timeline in timelines
            for interval in timeline.intervals
                if first_row
                    rows[:startTime] = [string(interval.startTime)]
                    for (k, v) in pairs(interval.values)
                        rows[Symbol(k)] = [v]
                    end
                    first_row = false
                else
                    push!(rows[:startTime], string(interval.startTime))
                    for (k, v) in pairs(interval.values)
                        push!(rows[Symbol(k)], v)
                    end
                end
            end
        end
        df = DataFrame(rows)
        if length(files) > 1
            insertcols!(df, 1, :point_index => i)
        end
        push!(all_dfs, df)
    end
    length(all_dfs) == 1 ? all_dfs[1] : vcat(all_dfs...; cols=:union)
end

#--------------------------------------------------------------------------------# Visual Crossing

function GeoDataAccess.load(plan::DataAccessPlan{VisualCrossing})
    files = fetch(plan)
    include_type = get(plan.kwargs, :include, "days")
    all_dfs = DataFrame[]
    for (i, file) in enumerate(files)
        json = JSON3.read(read(file, String))
        if include_type == "hours"
            rows = Dict{Symbol, Vector}()
            first_row = true
            for day in json.days
                for hour in day.hours
                    if first_row
                        for (k, v) in pairs(hour)
                            rows[Symbol(k)] = [v]
                        end
                        first_row = false
                    else
                        for (k, v) in pairs(hour)
                            push!(rows[Symbol(k)], v)
                        end
                    end
                end
            end
            df = DataFrame(rows)
        else
            rows = Dict{Symbol, Vector}()
            first_row = true
            for day in json.days
                if first_row
                    for (k, v) in pairs(day)
                        k == :hours && continue
                        rows[Symbol(k)] = [v]
                    end
                    first_row = false
                else
                    for (k, v) in pairs(day)
                        k == :hours && continue
                        push!(rows[Symbol(k)], v)
                    end
                end
            end
            df = DataFrame(rows)
        end
        if length(files) > 1
            insertcols!(df, 1, :point_index => i)
        end
        push!(all_dfs, df)
    end
    length(all_dfs) == 1 ? all_dfs[1] : vcat(all_dfs...; cols=:union)
end

#--------------------------------------------------------------------------------# USGS Earthquake

function GeoDataAccess.load(plan::DataAccessPlan{USGSEarthquake})
    files = fetch(plan)
    all_dfs = DataFrame[]
    for file in files
        json = JSON3.read(read(file, String))
        features = json.features
        rows = Dict{Symbol, Vector}()
        first_row = true
        for feature in features
            coords = feature.geometry.coordinates
            props = feature.properties
            if first_row
                rows[:id] = [string(feature.id)]
                rows[:longitude] = [coords[1]]
                rows[:latitude] = [coords[2]]
                rows[:depth] = [length(coords) >= 3 ? coords[3] : missing]
                for (k, v) in pairs(props)
                    rows[Symbol(k)] = [v]
                end
                first_row = false
            else
                push!(rows[:id], string(feature.id))
                push!(rows[:longitude], coords[1])
                push!(rows[:latitude], coords[2])
                push!(rows[:depth], length(coords) >= 3 ? coords[3] : missing)
                for (k, v) in pairs(props)
                    push!(rows[Symbol(k)], v)
                end
            end
        end
        isempty(rows) || push!(all_dfs, DataFrame(rows))
    end
    isempty(all_dfs) && return DataFrame()
    length(all_dfs) == 1 ? all_dfs[1] : vcat(all_dfs...; cols=:union)
end

#--------------------------------------------------------------------------------# USGS Water Services

function GeoDataAccess.load(plan::DataAccessPlan{USGSWaterServices})
    files = fetch(plan)
    all_rows = NamedTuple[]
    for file in files
        json = JSON3.read(read(file, String))
        time_series = json.value.timeSeries
        for ts in time_series
            site_code = string(ts.sourceInfo.siteCode[1].value)
            site_name = string(ts.sourceInfo.siteName)
            variable_code = string(ts.variable.variableCode[1].value)
            variable_name = string(ts.variable.variableName)
            unit = string(ts.variable.unit.unitCode)
            for val_set in ts.values
                for v in val_set.value
                    push!(all_rows, (
                        site_code = site_code,
                        site_name = site_name,
                        variable_code = variable_code,
                        variable_name = variable_name,
                        unit = unit,
                        datetime = string(v.dateTime),
                        value = v.value,
                    ))
                end
            end
        end
    end
    isempty(all_rows) && return DataFrame()
    DataFrame(all_rows)
end

#--------------------------------------------------------------------------------# OpenAQ

function GeoDataAccess.load(plan::DataAccessPlan{OpenAQ})
    files = fetch(plan)
    frequency = get(plan.kwargs, :frequency, :daily)
    all_rows = NamedTuple[]
    for file in files
        json = JSON3.read(read(file, String))
        results = json.results
        for r in results
            if frequency == :daily
                row = (
                    sensor_id = r.sensorsId,
                    datetime_from = string(get(r.period, :datetimeFrom, missing)),
                    datetime_to = string(get(r.period, :datetimeTo, missing)),
                    value = get(r, :value, missing),
                    min = get(r.summary, :min, missing),
                    max = get(r.summary, :max, missing),
                    avg = get(r.summary, :avg, missing),
                )
            else
                row = (
                    sensor_id = r.sensorsId,
                    datetime_from = string(get(r.period, :datetimeFrom, missing)),
                    datetime_to = string(get(r.period, :datetimeTo, missing)),
                    value = get(r, :value, missing),
                    min = get(r.summary, :min, missing),
                    max = get(r.summary, :max, missing),
                    avg = get(r.summary, :avg, missing),
                )
            end
            push!(all_rows, row)
        end
    end
    isempty(all_rows) && return DataFrame()
    DataFrame(all_rows)
end

#--------------------------------------------------------------------------------# EPA AQS

function GeoDataAccess.load(plan::DataAccessPlan{EPAAQS})
    files = fetch(plan)
    all_dfs = DataFrame[]
    for file in files
        json = JSON3.read(read(file, String))
        data = json.Data
        isempty(data) && continue
        push!(all_dfs, DataFrame(data))
    end
    isempty(all_dfs) && return DataFrame()
    length(all_dfs) == 1 ? all_dfs[1] : vcat(all_dfs...; cols=:union)
end

end # module
