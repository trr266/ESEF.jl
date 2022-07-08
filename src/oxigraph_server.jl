using HTTP
using JSON
using Chain
using Arrow

function sparql_query(query)
    headers = ["Content-Type" => "application/sparql-query", "Accept" => "application/sparql-results+json"]
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

function serve_oxigraph(; nt_file_path = "", keep_open = false)
    # 1. Install oxigraph server
    run(`cargo install oxigraph_server`)

    # 2. Download rdf triples data 
    if nt_file_path == ""
        run(`git clone https://github.com/ad-freiburg/qlever`)
        run(`xz -d qlever/examples/olympics.nt.xz`)
        nt_file_path = "qlever/examples/olympics.nt"
    end

    # 2. Load data into database
    run(`$(ENV["HOME"])/.cargo/bin/oxigraph_server --location esef_oxigraph_data load --file $nt_file_path`)

    # 3. Spin up database
    oxigraph_process = run(`$(ENV["HOME"])/.cargo/bin/oxigraph_server --location esef_oxigraph_data serve`; wait=false)

    try
        # 4. Test query database
        query_response = @chain "SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o } LIMIT 10" sparql_query

        n_items = @chain query_response["results"]["bindings"][1]["count"]["value"] parse(Int64, _)

        # 5. Check that we got the right number of items
        @assert n_items == countlines(nt_file_path)
    catch
        @assert n_items == countlines(nt_file_path), "Basic integrity check failed, check whether dataset has duplicates!"
        kill(oxigraph_process)
    finally
        # 6. Stop database
        if keep_open
            return oxigraph_process
        else
            kill(oxigraph_process)
        end
    end
end




