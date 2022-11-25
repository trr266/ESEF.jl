using HTTP
using DataFrames
using Chain
using JSON
using Mustache
using Retry

function query_sparql(api_url, sparql_query_file; params=Dict())
    # TODO: Consider requesting verbose format, parsing based on data type
    # TODO: Check and error if query limit is reached by results

    headers = [
        "Content-Type" => "application/sparql-query",
        "Accept" => "application/sparql-results+json",
    ]

    df = @chain sparql_query_file begin
        # Format query string, inject parameters
        read(String)
        render(params)

        # Query sparql api url
        patient_post(api_url, headers, _)

        # Reshape as dataframe
        
        
        reduce(vcat, _; cols=:union)
    end

    return df
end
