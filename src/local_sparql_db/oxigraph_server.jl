using HTTP
using JSON
using Chain
using Arrow
using DataFrameMacros

function serve_oxigraph(;
    nt_file_path="", db_path=".cache/esef_oxigraph_data", rebuild_db=false, keep_open=false
)
    if rebuild_db
        rm(db_path; force=true, recursive=true)
    end

    # 1. Install oxigraph server via Cargo
    r_status = try
        read(`cargo -v`, String)
    catch
    end

    r_status !== nothing || error("Cargo not installed")
    run(`cargo install oxigraph_server`)

    # 2. Download rdf triples data 
    if nt_file_path == ""
        qlever_path = ".cache/qlever"
        if !isdir(qlever_path)
            nt_file_path = "$qlever_path/examples/olympics.nt"
            rm(qlever_path; force=true, recursive=true)
            run(`git clone https://github.com/ad-freiburg/qlever $qlever_path`)
            run(`xz -d $nt_file_path.xz`)
        end
    end

    # 3. Load data into database
    run(
        `$(ENV["HOME"])/.cargo/bin/oxigraph_server --location $db_path load --file $nt_file_path`,
    )

    # 4. Spin up database
    oxigraph_process = run(
        `$(ENV["HOME"])/.cargo/bin/oxigraph_server --location $db_path serve`; wait=false
    )

    try
        # 5. Test query database
        q_path = joinpath(
            @__DIR__, "..", "..", "queries", "local", "local_query_test.sparql"
        )
        n_items = @chain q_path begin
            query_local_db_sparql()
            unpack_value_cols([:count])
            @transform(:count = parse(Int64, :count))
            _[1, "count"]
        end

        # 6. Check that we got the right number of items
        @assert n_items == countlines(nt_file_path),
        "Basic integrity check failed, check whether dataset has duplicates!"
    catch e
        kill(oxigraph_process)
        throw(e)
    finally
        # 7. Stop database
        if keep_open
            return oxigraph_process
        else
            kill(oxigraph_process)
        end
    end
end
