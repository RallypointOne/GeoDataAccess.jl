using BenchmarkTools, JSON3, Dates
using GeoDataAccess

#--------------------------------------------------------------------------------# Benchmark Suite
#--------------------------------------------------------------------------------
suite = BenchmarkGroup()

suite["greet"] = BenchmarkGroup()
suite["greet"]["no_args"] = @benchmarkable greet()
suite["greet"]["with_name"] = @benchmarkable greet("Julia")

suite["transform"] = BenchmarkGroup()
suite["transform"]["small"] = @benchmarkable transform([1, 2, 3]; scale=2.0)
suite["transform"]["medium"] = @benchmarkable transform(x; scale=2.0) setup=(x = rand(1000))
suite["transform"]["large"] = @benchmarkable transform(x; scale=2.0) setup=(x = rand(100_000))

#--------------------------------------------------------------------------------# Run Benchmarks
#--------------------------------------------------------------------------------
println("Running benchmarks...")
results = run(suite, verbose=true)

#--------------------------------------------------------------------------------# Collect Results
#--------------------------------------------------------------------------------
function collect_results(group::BenchmarkGroup, prefix="")
    entries = []
    for (key, val) in group
        name = isempty(prefix) ? string(key) : "$prefix/$key"
        if val isa BenchmarkGroup
            append!(entries, collect_results(val, name))
        else
            t = median(val)
            push!(entries, (;
                name,
                time_ns = t.time,
                memory_bytes = t.memory,
                allocs = t.allocs,
            ))
        end
    end
    return entries
end

benchmarks = collect_results(results)

output = (;
    julia_version = string(VERSION),
    cpu = Sys.cpu_info()[1].model,
    timestamp = Dates.format(now(UTC), dateformat"yyyy-mm-ddTHH:MM:SSZ"),
    benchmarks,
)

outfile = joinpath(@__DIR__, "results.json")
open(outfile, "w") do io
    JSON3.pretty(io, output)
end

println("Results written to $outfile")
