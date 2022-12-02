using Chain

function query_wikidata_sparql(sparql_query_file; params=Dict())
    sleep(5)
    @chain "https://query.wikidata.org/bigdata/namespace/wdq/sparql" begin
        query_sparql(sparql_query_file; params=params)
    end
end
