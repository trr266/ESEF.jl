using Chain
using DataFrameMacros
using HTTP
using JSON
using DataFrames
using DelimitedFiles
using Arrow

function export_concept_count_table()
    q_path = joinpath(@__DIR__, "..", "..", "queries", "local", "concept_count.sparql")
    query_response = @chain q_path query_local_db_sparql

    df_concepts = DataFrame(; concept=String[], frequency=Int[])

    for i in query_response["results"]["bindings"]
        push!(
            df_concepts,
            [
                HTTP.unescapeuri(replace(i["obj_1"]["value"], "http://example.org/" => "")),
                parse(Int, i["obj_count"]["value"]),
            ],
        )
    end

    return df_concepts
end

function export_profit_table()
    query_profit_data = """
        SELECT ?sub ?entity ?period ?unit ?decimals ?value WHERE {
            ?sub <http://example.org/dimensions.concept> <http://example.org/ifrs-full%3AProfitLoss> .
            ?sub <http://example.org/dimensions.period> ?period .
            ?sub <http://example.org/decimals> ?decimals .
            ?sub <http://example.org/dimensions.entity> ?entity .
            ?sub <http://example.org/value> ?value .
            ?sub <http://example.org/dimensions.unit> ?unit .
        }
        LIMIT 1000000
    """
    query_response = @chain query_profit_data query_local_db
    query_response = query_response["results"]["bindings"]

    # Check that we didn't hit query row limit
    @assert length(query_response) != 1000000

    # Map query to empty dataframe
    df_profit = DataFrame(;
        entity=String[], period=String[], unit=String[], decimals=Int[], value=Float64[]
    )

    for i in query_response
        push!(
            df_profit,
            [
                HTTP.unescapeuri(
                    replace(i["entity"]["value"], "http://example.org/" => "")
                ),
                HTTP.unescapeuri(
                    replace(i["period"]["value"], "http://example.org/" => "")
                ),
                HTTP.unescapeuri(replace(i["unit"]["value"], "http://example.org/" => "")),
                parse(
                    Int,
                    HTTP.unescapeuri(
                        replace(i["decimals"]["value"], "http://example.org/" => "")
                    ),
                ),
                parse(
                    Float64,
                    HTTP.unescapeuri(
                        replace(i["value"]["value"], "http://example.org/" => "")
                    ),
                ),
            ],
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

    df_wikidata_rdf = @chain df_wikidata_rdf begin
        @transform(
            :rdf_line =
                join(
                    [format_nt(:subject), format_nt(:predicate) * format_nt(:object)],
                    " ",
                ) * " ."
        )
        unique
    end
end

function serve_esef_data()
    if !isdir(".cache")
        mkdir(".cache")
    end
    
    if !isfile(".cache/df_esef_rdf.arrow")
        df_esef_rdf = @chain build_xbrl_dataframe() begin
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

    oxigraph_process = serve_oxigraph(; nt_file_path=".cache/oxigraph_rdf.nt", keep_open=true)

    return oxigraph_process
end

function process_xbrl_filings()
    oxigraph_process = serve_esef_data()

    # Rollup of all concepts available from ESEF data using XBRL's filings API
    df_concepts = export_concept_count_table()
    @chain df_concepts begin
        @sort(-:frequency)
        Arrow.write(".cache/concept_df.arrow", _) 
    end

    df_profit = export_profit_table()

    @chain df_profit begin
        Arrow.write(".cache/profit_df.arrow", _)
    end

    return kill(oxigraph_process)
end
