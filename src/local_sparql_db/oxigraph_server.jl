using HTTP
using JSON
using Chain
using Arrow
using DataFrameMacros
using oxigraph_server_jll
using XZ_jll

function serve_oxigraph(;
    nt_file_path="", db_path=".cache/esef_oxigraph_data", rebuild_db=false, keep_open=false
)
    if rebuild_db
        rm(db_path; force=true, recursive=true)
    end

    # 1. Download rdf triples data 
    if nt_file_path == ""
        qlever_path = ".cache/qlever"
        nt_file_path = "$qlever_path/examples/olympics.nt"

        if !isdir(qlever_path)
            rm(qlever_path; force=true, recursive=true)
            run(`git clone https://github.com/ad-freiburg/qlever $qlever_path`)
            run(`$(xz()) -d $nt_file_path.xz`)
        end
    end

    # 2. Load data into database
    run(`$(oxigraph_server()) --location $db_path load --file $nt_file_path`)

    # 3. Spin up database
    oxigraph_port = rand(7001:7999, 1)[1]
    oxigraph_process = run(
        `$(oxigraph_server()) --location $db_path serve --bind localhost:$oxigraph_port`;
        wait=false,
    )
    sleep(2)

    # 4. Test query database
    q_path = joinpath(@__DIR__, "..", "..", "queries", "local", "local_query_test.sparql")
    n_items = @chain q_path begin
        query_local_db_sparql(oxigraph_port)
        unpack_value_cols([:count])
        @transform(:count = parse(Int64, :count))
        _[1, "count"]
    end

    # 5. Check that we got the right number of items
    @assert n_items == countlines(nt_file_path) "Basic integrity check failed, check whether dataset has duplicates!"

    # 6. Stop database
    if keep_open
        return oxigraph_process, oxigraph_port
    else
        kill(oxigraph_process)
        return oxigraph_process, oxigraph_port
    end
end
