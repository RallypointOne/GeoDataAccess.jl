module CSVExt

using GeoDataAccess
using DataFrames
using CSV

import GeoDataAccess: DataAccessPlan, load, fetch, NASAFIRMS

#--------------------------------------------------------------------------------# NASA FIRMS

function GeoDataAccess.load(plan::DataAccessPlan{NASAFIRMS})
    files = fetch(plan)
    dfs = [CSV.read(f, DataFrame) for f in files]
    length(dfs) == 1 ? dfs[1] : vcat(dfs...; cols=:union)
end

end # module
