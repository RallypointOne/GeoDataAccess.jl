module DataFramesExt

using GeoDataAccess
using DataFrames

import GeoDataAccess: DataAccessPlan, load, fetch

using GeoDataAccess: OpenMeteoArchive, OpenMeteoForecast, NOAANCEI, NASAPower, TomorrowIO,
    VisualCrossing, USGSEarthquake, USGSWaterServices, OpenAQ, EPAAQS,
    OpenStreetMap

using GeoDataAccess: JSON3

#--------------------------------------------------------------------------------# Helpers

function _merge_dfs(dfs::Vector{DataFrame}; cols=:union)
    isempty(dfs) && return DataFrame()
    length(dfs) == 1 ? dfs[1] : vcat(dfs...; cols)
end

function _collect_rows(items; skip_keys=Symbol[])
    cols = Dict{Symbol, Vector}()
    for item in items
        for (k, v) in pairs(item)
            sym = Symbol(k)
            sym in skip_keys && continue
            if haskey(cols, sym)
                push!(cols[sym], v)
            else
                cols[sym] = [v]
            end
        end
    end
    cols
end

#--------------------------------------------------------------------------------# OpenMeteo (Archive + Forecast)

function GeoDataAccess.load(plan::DataAccessPlan{<:Union{OpenMeteoArchive.Source, OpenMeteoForecast.Source}})
    files = fetch(plan)
    frequency = plan.kwargs[:frequency]
    dfs = DataFrame[]
    for file in files
        json = JSON3.read(read(file, String))
        data = json[Symbol(frequency)]
        cols = Dict{Symbol, Any}(Symbol(k) => collect(v) for (k, v) in pairs(data))
        push!(dfs, DataFrame(cols))
    end
    _merge_dfs(dfs; cols=:orderequal)
end

#--------------------------------------------------------------------------------# NOAA NCEI

function GeoDataAccess.load(plan::DataAccessPlan{NOAANCEI.Source})
    files = fetch(plan)
    dfs = [DataFrame(JSON3.read(read(f, String))) for f in files]
    _merge_dfs(dfs)
end

#--------------------------------------------------------------------------------# NASA POWER

function _nasa_power_parse(json)
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
    df
end

function GeoDataAccess.load(plan::DataAccessPlan{NASAPower.Source})
    files = fetch(plan)
    dfs = DataFrame[]
    for (i, file) in enumerate(files)
        json = JSON3.read(read(file, String))
        df = _nasa_power_parse(json)
        if length(files) > 1
            insertcols!(df, 1, :point_index => i)
        end
        push!(dfs, df)
    end
    _merge_dfs(dfs)
end

#--------------------------------------------------------------------------------# Tomorrow.io

function GeoDataAccess.load(plan::DataAccessPlan{TomorrowIO.Source})
    files = fetch(plan)
    all_dfs = DataFrame[]
    for (i, file) in enumerate(files)
        json = JSON3.read(read(file, String))
        rows = NamedTuple[]
        for timeline in json.data.timelines
            for interval in timeline.intervals
                row = Dict{Symbol, Any}(:startTime => string(interval.startTime))
                for (k, v) in pairs(interval.values)
                    row[Symbol(k)] = v
                end
                push!(rows, (; (Symbol(k) => v for (k, v) in row)...))
            end
        end
        df = isempty(rows) ? DataFrame() : DataFrame(rows)
        if length(files) > 1
            insertcols!(df, 1, :point_index => i)
        end
        push!(all_dfs, df)
    end
    _merge_dfs(all_dfs)
end

#--------------------------------------------------------------------------------# Visual Crossing

function GeoDataAccess.load(plan::DataAccessPlan{VisualCrossing.Source})
    files = fetch(plan)
    include_type = get(plan.kwargs, :include, "days")
    all_dfs = DataFrame[]
    for (i, file) in enumerate(files)
        json = JSON3.read(read(file, String))
        if include_type == "hours"
            items = [hour for day in json.days for hour in day.hours]
            cols = _collect_rows(items)
        else
            items = json.days
            cols = _collect_rows(items; skip_keys=[:hours])
        end
        df = isempty(cols) ? DataFrame() : DataFrame(cols)
        if length(files) > 1
            insertcols!(df, 1, :point_index => i)
        end
        push!(all_dfs, df)
    end
    _merge_dfs(all_dfs)
end

#--------------------------------------------------------------------------------# USGS Earthquake

function GeoDataAccess.load(plan::DataAccessPlan{USGSEarthquake.Source})
    files = fetch(plan)
    all_dfs = DataFrame[]
    for file in files
        json = JSON3.read(read(file, String))
        rows = NamedTuple[]
        for feature in json.features
            coords = feature.geometry.coordinates
            base = (
                id = string(feature.id),
                longitude = coords[1],
                latitude = coords[2],
                depth = length(coords) >= 3 ? coords[3] : missing,
            )
            props = (; (Symbol(k) => v for (k, v) in pairs(feature.properties))...)
            push!(rows, merge(base, props))
        end
        isempty(rows) || push!(all_dfs, DataFrame(rows))
    end
    _merge_dfs(all_dfs)
end

#--------------------------------------------------------------------------------# USGS Water Services

function GeoDataAccess.load(plan::DataAccessPlan{USGSWaterServices.Source})
    files = fetch(plan)
    all_rows = NamedTuple[]
    for file in files
        json = JSON3.read(read(file, String))
        for ts in json.value.timeSeries
            site_code = string(ts.sourceInfo.siteCode[1].value)
            site_name = string(ts.sourceInfo.siteName)
            variable_code = string(ts.variable.variableCode[1].value)
            variable_name = string(ts.variable.variableName)
            unit = string(ts.variable.unit.unitCode)
            for val_set in ts.values
                for v in val_set.value
                    push!(all_rows, (;
                        site_code, site_name, variable_code, variable_name, unit,
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

function GeoDataAccess.load(plan::DataAccessPlan{OpenAQ.Source})
    files = fetch(plan)
    all_rows = NamedTuple[]
    for file in files
        json = JSON3.read(read(file, String))
        for r in json.results
            push!(all_rows, (
                sensor_id = r.sensorsId,
                datetime_from = string(get(r.period, :datetimeFrom, missing)),
                datetime_to = string(get(r.period, :datetimeTo, missing)),
                value = get(r, :value, missing),
                min = get(r.summary, :min, missing),
                max = get(r.summary, :max, missing),
                avg = get(r.summary, :avg, missing),
            ))
        end
    end
    isempty(all_rows) && return DataFrame()
    DataFrame(all_rows)
end

#--------------------------------------------------------------------------------# EPA AQS

function GeoDataAccess.load(plan::DataAccessPlan{EPAAQS.Source})
    files = fetch(plan)
    all_dfs = DataFrame[]
    for file in files
        json = JSON3.read(read(file, String))
        data = json.Data
        isempty(data) && continue
        push!(all_dfs, DataFrame(data))
    end
    _merge_dfs(all_dfs)
end

#--------------------------------------------------------------------------------# OpenStreetMap

function GeoDataAccess.load(plan::DataAccessPlan{OpenStreetMap.Source})
    files = fetch(plan)
    all_rows = NamedTuple[]
    for (file, var) in zip(files, plan.variables)
        json = JSON3.read(read(file, String))
        for elem in json.elements
            hasproperty(elem, :tags) || continue
            lat = if hasproperty(elem, :lat)
                elem.lat
            elseif hasproperty(elem, :center)
                elem.center.lat
            else
                missing
            end
            lon = if hasproperty(elem, :lon)
                elem.lon
            elseif hasproperty(elem, :center)
                elem.center.lon
            else
                missing
            end
            tag_val = hasproperty(elem.tags, var) ? string(getproperty(elem.tags, var)) : missing
            name_val = hasproperty(elem.tags, :name) ? string(elem.tags.name) : missing
            push!(all_rows, (
                osm_type = string(elem.type),
                osm_id = elem.id,
                latitude = lat,
                longitude = lon,
                tag_key = string(var),
                tag_value = tag_val,
                name = name_val,
                tags = JSON3.write(elem.tags),
            ))
        end
    end
    isempty(all_rows) && return DataFrame()
    DataFrame(all_rows)
end

end # module
