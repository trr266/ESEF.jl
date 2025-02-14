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

function build_xbrl_dataframe(; debug=false)
    df_xbrl_raw = get_esef_xbrl_filings(debug=debug)

    if debug
        df_xbrl_raw = first(df_xbrl_raw, 5)
    end

    df_xbrl_raw = @chain df_xbrl_raw begin
        @subset(:xbrl_json_path != nothing)
        @transform(:xbrl_json_path = replace(:xbrl_json_path, " " => "%20"))
    end

    df_esef_rdf = DataFrame()

    for wdata_country_id in unique(df_xbrl_raw[!, :country])
        country_name = @chain ESEF.get_wikidata_country_iso2_lookup() @subset(:country == wdata_country_id) @select(:countryLabel) _[1, 1]

        arrow_file = joinpath(".cache", "df_esef_rdf_$(country_name)$(debug ? "_debug" : "").arrow")
        
        if isfile(arrow_file)
            @info "Reading filings for $country_name from cache."
            df_country = @chain arrow_file begin
                Arrow.Table()
                DataFrame()
            end
        else
            @info "Fetching filings for $country_name."
            df_country = @chain df_xbrl_raw begin
                @subset(:country .== wdata_country_id)
                build_df_esef_rdf(_)
            end
            Arrow.write(arrow_file, df_country)
        end
        
        df_esef_rdf = vcat(df_esef_rdf, df_country)
    end

    return df_esef_rdf
end

function format_nt(s_p_o_string)
    if startswith(s_p_o_string, "http://")
        return "<" * s_p_o_string * ">"
    else
        return " \"$(HTTP.escapeuri(s_p_o_string))\" "
    end
end

function build_wikidata_dataframe()
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

function serve_esef_data(; keep_open=false, rebuild_db=true, debug=false)
    if !isdir(".cache")
        mkdir(".cache")
    end

    debug_flag = debug ? "_debug" : ""
    f_esef_arrow = ".cache/df_esef_rdf_full_$debug_flag.arrow"
    if !isfile(f_esef_arrow)
        df_esef_rdf = @chain build_xbrl_dataframe(debug=debug) begin
            @aside Arrow.write(f_esef_arrow, _)
        end
    else
        df_esef_rdf = @chain f_esef_arrow begin
            Arrow.Table()
            DataFrame()
        end
    end

    f_wikidata = ".cache/df_wikidata_rdf$debug_flag.arrow"
    if !isfile(f_wikidata)
        df_wikidata_rdf = @chain build_wikidata_dataframe() begin
            @aside Arrow.write(f_wikidata, _)
        end

    else
        df_wikidata_rdf = @chain f_wikidata begin
            Arrow.Table()
            DataFrame()
        end
    end

    nt_file_path = ".cache/oxigraph_rdf$debug_flag.nt"

    rm(nt_file_path; force=true)

    # TODO: Figure out why predicate and object are reversed for wikidata, making queries fail
    # TODO: Import statements for Wikidata (e.g. LEIs)

    open(nt_file_path, "w") do io
        writedlm(io, df_esef_rdf[:, :rdf_line])
        writedlm(io, df_wikidata_rdf[:, :rdf_line]; quotes=false)
    end

    oxigraph_process, oxigraph_port = serve_oxigraph(;
        nt_file_path=nt_file_path, rebuild_db=true, keep_open=keep_open
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
