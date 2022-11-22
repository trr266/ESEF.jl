using HTTP
using JSON
using Chain

function sparql_query(query)
    headers = [
        "Content-Type" => "application/sparql-query",
        "Accept" => "application/sparql-results+json",
    ]
    url = "http://localhost:7878/query"

    r = HTTP.request("POST", url, headers, query)

    # Check 200 HTTP status code
    @assert(r.status == 200)

    raw_data = @chain r.body begin
        String()
        JSON.parse()
    end

    return raw_data
end
