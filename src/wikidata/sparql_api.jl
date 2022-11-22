using HTTP
using DataFrames
using Chain
using JSON
using Mustache

function query_wikidata(sparql_query_file; params=Dict())
    # TODO: Consider requesting verbose format, parsing based on data type
    # TODO: Check and error if query limit is reached by results
    headers = [
        "Accept" => "application/sparql-results+json",
        "Content-Type" => "application/x-www-form-urlencoded",
    ]
    url = "https://query.wikidata.org/bigdata/namespace/wdq/sparql"

    query_string = @chain sparql_query_file read(String) render(params) HTTP.escapeuri()

    body = "query=$query_string"

    r = nothing

    for i in 1:3
        r = HTTP.post(url, headers, body)
        if r.status == 200
            break
        end
    end

    d = JSON.parse(String(r.body))

    df = DataFrame()

    for r in d["results"]["bindings"]
        df1 = DataFrame(r)
        append!(df, df1; cols=:union)
    end

    return df
end
