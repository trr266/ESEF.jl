module ESEF

import Base: @invokelatest

include("helpers/helpers.jl")

include("dataset/dataset.jl")

include("wikidata/wikidata.jl")

include("lei/lei.jl")

include("xbrl/xbrl.jl")

include("local_sparql_db/local_sparql_db.jl")

include("exploration/analysis.jl")
include("exploration/visualizations.jl")

end
