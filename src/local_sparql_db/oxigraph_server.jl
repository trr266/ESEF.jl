using HTTP
using JSON
using Chain
using Arrow
using XZ_jll

function serve_oxigraph(;
    nt_file_path="", db_path=".cache/esef_oxigraph_data", rebuild_db=true, keep_open=false
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
        rm(".qlever"; force=true, recursive=true)
        run(`git clone https://github.com/ad-freiburg/qlever .qlever`)
        run(`$(xz()) -d .cache/.qlever/examples/olympics.nt.xz`)
        nt_file_path = ".cache/.qlever/examples/olympics.nt"
    end

    # 2. Load data into database
    run(
        `$(ENV["HOME"])/.cargo/bin/oxigraph_server --location $db_path load --file $nt_file_path`,
    )

    # 3. Spin up database
    oxigraph_process = run(
        `$(ENV["HOME"])/.cargo/bin/oxigraph_server --location $db_path serve`; wait=false
    )

    try
        # 4. Test query database
        q_path = joinpath(
            @__DIR__, "..", "..", "queries", "local", "local_query_test.sparql"
        )
        df = query_local_db_sparql(q_path)

        n_items = @chain df[!, "count"][1]["value"] parse(Int64, _)

        # 5. Check that we got the right number of items
        @assert n_items == countlines(nt_file_path)
    catch
        @assert n_items == countlines(nt_file_path),
        "Basic integrity check failed, check whether dataset has duplicates!"
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
