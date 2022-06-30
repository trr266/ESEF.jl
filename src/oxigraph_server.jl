using HTTP
using JSON
using Chain

function serve_oxigraph(; nt_file_path = "")
    # 1. Install oxigraph server
    run(`cargo install oxigraph_server`)

    # 2. Download rdf triples data 
    if nt_file_path == ""
        run(`git clone https://github.com/ad-freiburg/qlever`)
        run(`xz -d qlever/examples/olympics.nt.xz`)
        nt_file_path = "qlever/examples/olympics.nt"
    end

    # 2. Load data into database
    run(`oxigraph_server --location esef_oxigraph_data load --file $nt_file_path`)

    # 3. Spin up database
    oxigraph_process = run(`oxigraph_server --location esef_oxigraph_data serve`; wait=false)

    try
        # 4. Query database
        headers = ["Content-Type" => "application/sparql-query", "Accept" => "application/sparql-results+json"]
        url = "http://localhost:7878/query"
        query = "SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o } LIMIT 10"
        r = HTTP.request("POST", url, headers, query)

        # Check 200 HTTP status code
        @assert(r.status == 200)

        raw_data = @chain r.body begin
            String()
            JSON.parse()
        end

        n_items = @chain raw_data["results"]["bindings"][1]["count"]["value"] parse(Int64, _)

        # 5. Check that we got the right number of items
        @assert n_items == countlines(nt_file_path)
    catch
        kill(oxigraph_process)
    finally
        # 6. Stop database
        kill(oxigraph_process)
    end
end
