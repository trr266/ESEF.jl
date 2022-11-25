using HTTP
using JSON
using Chain
using Mustache

function query_local_db(sparql_query_file; params=Dict())
    # TODO: Consider requesting verbose format, parsing based on data type
    # TODO: Check and error if query limit is reached by results

    headers = [
        "Content-Type" => "application/sparql-query",
        "Accept" => "application/sparql-results+json",
    ]
    url = "http://localhost:7878/query"

    raw_data = @chain url begin
        patient_post(headers, query)
    end

    return raw_data
end
