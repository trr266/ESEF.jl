using Chain
using DataFrameMacros
using HTTP
using JSON
using DataFrames
using DelimitedFiles
using Arrow

function export_concept_count_table(oxigraph_port)
    q_path = joinpath(@__DIR__, "..", "..", "queries", "local", "concept_count.sparql")
    results_df = @chain q_path query_local_db_sparql(oxigraph_port)

    df_concepts = @chain results_df begin
        unpack_value_cols([:concept, :frequency])
        @transform(
            :concept = rehydrate_uri_entity(:concept), :frequency = parse(Int, :frequency)
        )
    end

    return df_concepts
end

function unpack_raw_concept_result(df)
    df = @chain df begin
        unpack_value_cols([:entity, :period, :unit, :decimals, :value])
        @select(
            :entity = rehydrate_uri_entity(:entity),
            :period = rehydrate_uri_entity(:period),
            :unit = rehydrate_uri_entity(:unit),
            :decimals = parse(Int, rehydrate_uri_entity(:decimals)),
            :value = parse(Float64, rehydrate_uri_entity(:value)),
        )
    end
    return df
end

function export_profit_table(oxigraph_port)
    q_path = joinpath(@__DIR__, "..", "..", "queries", "local", "profit_data.sparql")
    results_df = @chain q_path query_local_db_sparql(oxigraph_port)

    # Check that we didn't hit query row limit
    @assert nrow(results_df) != 1000000
    df_profit = unpack_raw_concept_result(results_df)
    return df_profit
end

function export_total_assets_table(oxigraph_port)
    q_path = joinpath(@__DIR__, "..", "..", "queries", "local", "total_assets_data.sparql")
    results_df = @chain q_path query_local_db_sparql(oxigraph_port)

    # Check that we didn't hit query row limit
    @assert nrow(results_df) != 1000000
    df_profit = unpack_raw_concept_result(results_df)
    return df_profit
end

function export_equity_table(oxigraph_port)
    q_path = joinpath(@__DIR__, "..", "..", "queries", "local", "equity_data.sparql")
    results_df = @chain q_path query_local_db_sparql(oxigraph_port)

    # Check that we didn't hit query row limit
    @assert nrow(results_df) != 1000000
    df_profit = unpack_raw_concept_result(results_df)
    return df_profit
end

function build_df_esef_rdf(df_xbrl_raw)
    df_esef_rdf = DataFrame()
    for r in eachrow(df_xbrl_raw)
        xbrl_json_path = r[:xbrl_json_path]
        df_ = get_xbrl_json_doc(xbrl_json_path)
        df_rdf = @chain df_ begin
            # TODO: Rethink normalization, instead of using uuid for facts at RDF subject field
            @transform(
                :rdf_line =
                    "<http://example.org/" *
                    HTTP.escapeuri(string("https://filings.xbrl.org" * xbrl_json_path, :subject)) *
                    "> <http://example.org/" *
                    HTTP.escapeuri(:predicate) *
                    "> <http://example.org/" *
                    HTTP.escapeuri(:object) *
                    "> ."
            )
        end
        append!(df_esef_rdf, df_rdf)
    end
    return df_esef_rdf
end

function download_xbrl_data(; debug=false)
    df_xbrl_raw = get_esef_xbrl_filings(debug=debug)

    if debug
        df_xbrl_raw = first(df_xbrl_raw, 5)
    end

    df_xbrl_raw = @chain df_xbrl_raw begin
        @subset(:xbrl_json_path != nothing)
        @transform(:xbrl_json_path = replace(:xbrl_json_path, " " => "%20"))
    end

    esef_rdf_folder = ".cache/esef_rdf$(debug ? "_debug" : "")"
    if !isdir(esef_rdf_folder)
        mkdir(esef_rdf_folder)
    end

    for country_alpha_2 in unique(df_xbrl_raw[!, :country_alpha_2])
        arrow_file = joinpath(esef_rdf_folder, "df_esef_rdf_$(country_alpha_2)$(debug ? "_debug" : "").arrow")
        
        if isfile(arrow_file)
            @info "Reading filings for $country_alpha_2 from cache."
            df_country = @chain arrow_file begin
                Arrow.Table()
                DataFrame()
            end
        else
            @info "Fetching filings for $country_alpha_2."
            df_country = @chain df_xbrl_raw begin
                @subset(:country_alpha_2 .== country_alpha_2)
                build_df_esef_rdf(_)
            end
            Arrow.write(arrow_file, df_country)
        end
    end
end

function format_nt(s_p_o_string)
    if startswith(s_p_o_string, "http://")
        return "<" * s_p_o_string * ">"
    else
        return " \"$(HTTP.escapeuri(s_p_o_string))\" "
    end
end

function build_wikidata_dataframe(; debug=false)
    df_wikidata_rdf = get_accounting_facts()

    return df_wikidata_rdf = @chain df_wikidata_rdf begin
        @transform(
            :rdf_line =
                join(
                    [format_nt(:subject), format_nt(:predicate) * format_nt(:object)], " "
                ) * " ."
        )
        unique
    end
end

"""
    serve_esef_data(; keep_open::Bool=false, rebuild_db::Bool=true, debug::Bool=false, skip_download::Bool=false)

Loads and serves ESEF data into a local SPARQL database, with options to manage connection persistence, database rebuilding, debugging, and data downloading behavior.

# Keyword Arguments
- `keep_open::Bool`: 
    If `true`, maintains an open connection after processing, which can be useful for further database operations.
- `rebuild_db::Bool`: 
    If `true`, forces a rebuild of the local database prior to loading the new ESEF data. Set to `false` to preserve the current database structure.
- `debug::Bool`: 
    If `true`, enables debug mode and uses abridged test data.
- `skip_download::Bool`: 
    If `true`, bypasses the download step for the ESEF data, using the locally cached version instead.

# Details
This function orchestrates the process of loading ESEF data and preparing it within a local SPARQL database setup. It is designed to be flexible, offering control over database state, connection persistence, and troubleshooting options via its keyword arguments.

# Returns
- The function may return a process object with the database server and the port number for the server.
"""
function serve_esef_data(; keep_open=false, rebuild_db=true, debug=false, skip_download=true)
    if !isdir(".cache")
        mkdir(".cache")
    end

    if !skip_download
        download_xbrl_data(debug=debug)
    end

    f_wikidata = ".cache/df_wikidata_rdf$(debug ? "_debug" : "").arrow"
    if !isfile(f_wikidata)
        df_wikidata_rdf = @chain build_wikidata_dataframe(; debug=debug) begin
            @aside Arrow.write(f_wikidata, _)
        end

    else
        df_wikidata_rdf = @chain f_wikidata begin
            Arrow.Table()
            DataFrame()
        end
    end

    nt_file_path = ".cache/oxigraph_rdf$(debug ? "_debug" : "").nt"

    rm(nt_file_path; force=true)

    # TODO: Figure out why predicate and object are reversed for wikidata, making queries fail
    # TODO: Import statements for Wikidata (e.g. LEIs)

    open(nt_file_path, "w") do io
        for arrow_file in filter(f -> endswith(f, ".arrow"), readdir(".cache/esef_rdf$(debug ? "_debug" : "")", join=true))
            df_tmp = DataFrame(Arrow.Table(arrow_file))
            writedlm(io, df_tmp[:, :rdf_line])
        end
        writedlm(io, df_wikidata_rdf[:, :rdf_line]; quotes=false)
    end

    oxigraph_process, oxigraph_port = serve_oxigraph(;
        nt_file_path=nt_file_path, rebuild_db=rebuild_db, keep_open=keep_open
    )

    return oxigraph_process, oxigraph_port
end

function process_xbrl_filings(; out_dir=".cache/", debug=false)
    if !isdir(out_dir)
        mkdir(out_dir)
    end

    debug_flag = debug ? "_debug" : ""
    process, port = serve_esef_data(; keep_open=true, debug=debug)

    # Rollup of all concepts available from ESEF data using XBRL's filings API
    df_concepts = export_concept_count_table(port)
    @chain df_concepts begin
        @sort(-:frequency)
        Arrow.write(out_dir * "/concept_df$debug_flag.arrow", _)
    end

    df_profit = export_profit_table(port)

    @chain df_profit begin
        Arrow.write(out_dir * "/profit_df$debug_flag.arrow", _)
    end

    return kill(process)
end
