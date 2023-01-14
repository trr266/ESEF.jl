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

function export_profit_table(oxigraph_port)
    q_path = joinpath(@__DIR__, "..", "..", "queries", "local", "profit_data.sparql")
    results_df = @chain q_path query_local_db_sparql(oxigraph_port)

    # Check that we didn't hit query row limit
    @assert nrow(results_df) != 1000000

    df_profit = @chain results_df begin
        unpack_value_cols([:entity, :period, :unit, :decimals, :value])
        @select(
            :entity = rehydrate_uri_entity(:entity),
            :period = rehydrate_uri_entity(:period),
            :unit = rehydrate_uri_entity(:unit),
            :decimals = parse(Int, rehydrate_uri_entity(:decimals)),
            :value = parse(Int, rehydrate_uri_entity(:value)),
        )
    end

    return df_profit
end

function build_xbrl_dataframe(; test=false)
    df_xbrl_raw = get_esef_xbrl_filings()[1]

    if test
        df_xbrl_raw = first(df_xbrl_raw, 5)
    end

    df_xbrl_raw = @chain df_xbrl_raw begin
        @subset(:xbrl_json_path != nothing)
        @transform(
            :xbrl_json_url =
                "https://filings.xbrl.org/" *
                :filing_key *
                "/" *
                HTTP.escapeuri(:xbrl_json_path)
        )
        @select(:xbrl_json_url)
    end

    df_esef_rdf = DataFrame()

    for r in eachrow(df_xbrl_raw)
        xbrl_json_url = r[:xbrl_json_url]
        df_ = pluck_xbrl_json(xbrl_json_url)
        df_rdf = @chain df_ begin
            # TODO: Rethink normalization, instead of using uuid for facts at RDF subject field
            @transform(
                :rdf_line =
                    "<http://example.org/" *
                    HTTP.escapeuri(string(xbrl_json_url, :subject)) *
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

function serve_esef_data(; keep_open=false, rebuild_db=true, test=false)
    if !isdir(".cache")
        mkdir(".cache")
    end

    if !isfile(".cache/df_esef_rdf.arrow")
        df_esef_rdf = @chain build_xbrl_dataframe(test=test) begin
            @aside Arrow.write(".cache/df_esef_rdf.arrow", _)
        end
    else
        df_esef_rdf = @chain ".cache/df_esef_rdf.arrow" begin
            Arrow.Table()
            DataFrame()
        end
    end

    if !isfile(".cache/df_wikidata_rdf.arrow")
        df_wikidata_rdf = @chain build_wikidata_dataframe() begin
            @aside Arrow.write(".cache/df_wikidata_rdf.arrow", _)
        end

    else
        df_wikidata_rdf = @chain ".cache/df_wikidata_rdf.arrow" begin
            Arrow.Table()
            DataFrame()
        end
    end

    nt_file_path = ".cache/oxigraph_rdf.nt"

    rm(nt_file_path; force=true)

    # TODO: Figure out why predicate and object are reversed for wikidata, making queries fail
    # TODO: Import statements for Wikidata (e.g. LEIs)

    open(nt_file_path, "w") do io
        writedlm(io, df_esef_rdf[:, :rdf_line])
        writedlm(io, df_wikidata_rdf[:, :rdf_line]; quotes=false)
    end

    oxigraph_process, oxigraph_port = serve_oxigraph(;
        nt_file_path=".cache/oxigraph_rdf.nt", rebuild_db=true, keep_open=keep_open
    )

    return oxigraph_process, oxigraph_port
end

function process_xbrl_filings(; out_dir=".cache/", test=false)
    if !isdir(out_dir)
        mkdir(out_dir)
    end

    process, port = serve_esef_data(; keep_open=true, test=test)

    # Rollup of all concepts available from ESEF data using XBRL's filings API
    df_concepts = export_concept_count_table(port)
    @chain df_concepts begin
        @sort(-:frequency)
        Arrow.write(out_dir * "/concept_df.arrow", _)
    end

    df_profit = export_profit_table(port)

    @chain df_profit begin
        Arrow.write(out_dir * "/profit_df.arrow", _)
    end

    return kill(process)
end
