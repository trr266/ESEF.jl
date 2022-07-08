using ESEF
using Chain
using DataFrameMacros
using HTTP
using JSON
using DataFrames
using DelimitedFiles
using Arrow

include("oxigraph_server.jl")
include("wikidata_public_companies.jl")

function pluck_xbrl_json(url)
    r = HTTP.get(url)

    # Check 200 HTTP status code
    @assert(r.status == 200)

    raw_data = @chain r.body begin
        String()
        JSON.parse()
    end

    finished_facts = DataFrame()

    for (k, fact) in raw_data["facts"]
        flat_fact = rec_flatten_dict(fact)

        if haskey(flat_fact, "dimensions.entity")
            flat_fact["dimensions.entity"] = replace(flat_fact["dimensions.entity"], "scheme:" => "")
        end

        for (k_subfact, v_subfact) in flat_fact
            push!(finished_facts, NamedTuple{(:subject, :predicate, :object)}([k, k_subfact, string(v_subfact)]))
        end
    end

    return finished_facts
end


function rec_flatten_dict(d, prefix_delim = ".")
    # Source: https://discourse.julialang.org/t/question-is-there-a-function-to-flatten-a-nested-dictionary/54462/2
    new_d = empty(d)
    for (key, value) in pairs(d)
        if isa(value, Dict)
             flattened_value = rec_flatten_dict(value, prefix_delim)
             for (ikey, ivalue) in pairs(flattened_value)
                 new_d["$key.$ikey"] = ivalue
             end
        else
            new_d[key] = value
        end
    end
    return new_d
end


function export_concept_count_table()
    query_item_types = """
        # Get count of all 'concepts' included in ESEF dataset
        SELECT (str(?obj) as ?obj_1)  (str(COUNT(?obj)) as ?obj_count) WHERE {
        ?sub <http://example.org/dimensions.concept> ?obj .
        } GROUP BY ?obj
        ORDER BY DESC(?obj_count)
        LIMIT 1000000
    """

    query_response = @chain query_item_types sparql_query

    df_concepts = DataFrame(concept = String[], frequency = Int[])

    for i in query_response["results"]["bindings"]
        push!(df_concepts, [HTTP.unescapeuri(replace(i["obj_1"]["value"], "http://example.org/" => "")), parse(Int, i["obj_count"]["value"])])
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
    query_response = @chain query_profit_data sparql_query
    query_response = query_response["results"]["bindings"]
    
    # Check that we didn't hit query row limit
    @assert length(query_response) != 1000000

    # Map query to empty dataframe
    df_profit = DataFrame(entity = String[], period = String[], unit = String[], decimals = Int[], value = Float64[])

    for i in query_response
        push!(df_profit,
        [
            HTTP.unescapeuri(replace(i["entity"]["value"], "http://example.org/" => "")),
            HTTP.unescapeuri(replace(i["period"]["value"], "http://example.org/" => "")),
            HTTP.unescapeuri(replace(i["unit"]["value"], "http://example.org/" => "")),
            parse(Int, HTTP.unescapeuri(replace(i["decimals"]["value"], "http://example.org/" => ""))),
            parse(Float64, HTTP.unescapeuri(replace(i["value"]["value"], "http://example.org/" => ""))),
        ])
    end


    return df_profit
end

function process_xbrl_filings()

    if isfile("df_wikidata_rdf.arrow") & isfile("df_esef_rdf.arrow")
        df, df_error = ESEF.get_esef_xbrl_filings()

        df = @chain df begin
            @subset(:xbrl_json_path != nothing)
            @transform(:xbrl_json_url = "https://filings.xbrl.org/" * :filing_key * "/" * HTTP.escapeuri(:xbrl_json_path))
            @select(:xbrl_json_url)
        end
    end

    if isfile("df_esef_rdf.arrow")
        df_esef_rdf = DataFrame(Arrow.read("df_esef_rdf.arrow"))
    else

        df_esef_rdf = DataFrame()

        for r in eachrow(df)
            xbrl_json_url = r[:xbrl_json_url]
            df_ = pluck_xbrl_json(xbrl_json_url)
            df_rdf = @chain df_ begin
                # TODO: Rethink normalization, instead of using uuid for facts at RDF subject field
                @transform(:rdf_line = "<http://example.org/" * HTTP.escapeuri(string(xbrl_json_url, :subject)) * "> <http://example.org/" * HTTP.escapeuri(:predicate) * "> <http://example.org/" * HTTP.escapeuri(:object) * "> .")
            end
            append!(df_esef_rdf, df_rdf)
        end

        @chain df_esef_rdf Arrow.write("df_esef_rdf.arrow", _)

    end


    if isfile("df_wikidata_rdf.arrow")
        df_wikidata_rdf = DataFrame(Arrow.read("df_wikidata_rdf.arrow"))
    else
        df_wikidata_rdf = get_company_facts()

        df_wikidata_rdf = @chain df_wikidata_rdf begin
            @subset(startswith(:subject, "http:") & startswith(:predicate, "http://") & startswith(:object, "http://") )
            @transform(:rdf_line = "<" * :subject * "> <" * :predicate * "> <" * :object * "> .")        
        end

        @chain df_wikidata_rdf Arrow.write("df_wikidata_rdf.arrow", _)
    end

    nt_file_path = "oxigraph_rdf.nt"

    rm(nt_file_path, force=true)
    rm("esef_oxigraph_data", recursive=true, force=true)

    # TODO: Figure out why predicate and object are reversed for wikidata, making queries fail
    # TODO: Import statements for Wikidata (e.g. LEIs)

    open(nt_file_path, "w") do io
        writedlm(io, df_esef_rdf[:, :rdf_line])
        writedlm(io, df_wikidata_rdf[:, :rdf_line])
    end

    oxigraph_process = serve_oxigraph(; nt_file_path = "oxigraph_rdf.nt", keep_open = true)

    # Rollup of all concepts available from ESEF data using XBRL's filings API
    df_concepts = export_concept_count_table()
    @chain df_concepts @sort(-:frequency) Arrow.write("concept_df.arrow", _)

    # table = Arrow.Table("concept_df.arrow")

    df_profit = export_profit_table()
    @chain df_profit Arrow.write("profit_df.arrow", _)
    kill(oxigraph_process)
end
