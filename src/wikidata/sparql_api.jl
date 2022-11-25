using HTTP
using DataFrames
using Chain
using JSON
using Mustache
using Retry

function query_wikidata(sparql_query_file; params=Dict())
    # TODO: Consider requesting verbose format, parsing based on data type
    # TODO: Check and error if query limit is reached by results
    # headers = [
    #     "Accept" => "application/sparql-results+json",
    #     "Content-Type" => "application/x-www-form-urlencoded",
    # ]

    headers = [
        "Content-Type" => "application/sparql-query",
        "Accept" => "application/sparql-results+json",
    ]

    url = "https://query.wikidata.org/bigdata/namespace/wdq/sparql"

    df = @chain sparql_query_file begin
        # Format query string, inject parameters
        read(String)
        render(params)
        HTTP.escapeuri()
        "query=$(_)"

        # Query wikidata sparql endpoint
        patient_post(url, headers, _)

        # Reshape as dataframe
        [DataFrame(r) for r in _["results"]["bindings"]]
        reduce(vcat, _)
    end

    return df
end
