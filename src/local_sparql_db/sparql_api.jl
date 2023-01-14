using Chain

function query_local_db_sparql(sparql_query_file, oxigraph_port; params=Dict())
    # TODO: Consider requesting verbose format, parsing based on data type
    # TODO: Check and error if query limit is reached by results

    return @chain "http://localhost:$oxigraph_port/query" begin
        query_sparql(sparql_query_file; params=params)
    end
end
