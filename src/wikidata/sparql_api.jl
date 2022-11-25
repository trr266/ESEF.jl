using HTTP
using DataFrames
using Chain
using JSON
using Mustache
using Retry

function query_wikidata(sparql_query_file; params=Dict())
    # TODO: Consider requesting verbose format, parsing based on data type
    # TODO: Check and error if query limit is reached by results
    headers = [
        "Accept" => "application/sparql-results+json",
        "Content-Type" => "application/x-www-form-urlencoded",
    ]
    url = "https://query.wikidata.org/bigdata/namespace/wdq/sparql"

    @chain sparql_query_file begin
        # Format query string, inject parameters
        read(String)
        render(params)
        HTTP.escapeuri()
        "query=$(_)"

        # Query wikidata sparql endpoint
        @repeat 3 try
            HTTP.post(url, headers, _)
        catch e
            @delay_retry if http_status(e) < 200 &&
                            http_status(e) >= 500 end
        end

        # Parse response
        _.body
        String()
        JSON.parse()
    end

    df = DataFrame()

    for r in d["results"]["bindings"]
        df1 = DataFrame(r)
        append!(df, df1; cols=:union)
    end

    return df
end
