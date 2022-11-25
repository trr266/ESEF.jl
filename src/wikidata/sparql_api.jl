using HTTP
using DataFrames
using Chain
using JSON
using Mustache
using Retry

function query_wikidata_sparql(sparql_query_file; params=Dict())
    @chain "https://query.wikidata.org/bigdata/namespace/wdq/sparql" begin
        query_sparql(sparql_query_file, params=params)
    end
end
