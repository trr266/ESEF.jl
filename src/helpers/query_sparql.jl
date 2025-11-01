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

    response = @chain sparql_query_file begin
        # Format query string, inject parameters
        read(String)
        render(params)

        # Query sparql api url
        patient_post(api_url, headers, _)
    end

    # Get column names from the vars field in the SPARQL response
    col_names = get(get(response, "head", Dict()), "vars", String[])
    
    bindings = response["results"]["bindings"]
    
    if isempty(bindings)
        # Create empty DataFrame with proper column structure
        # Each column should be able to hold Dict types (with "value" field)
        df = DataFrame([Symbol(col) => Dict{String, Any}[] for col in col_names])
    else
        # Convert each binding to a DataFrame and combine
        dfs = [DataFrame(r) for r in bindings]
        # Always use vcat for consistency, even with single DataFrame
        df = vcat(dfs...; cols=:union)
    end

    return df
end
