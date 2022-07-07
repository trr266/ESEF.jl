using ESEF
using Chain
using DataFrameMacros
using HTTP
using JSON
using DataFrames
using DelimitedFiles
using UUIDs
using Arrow

include("oxigraph_server.jl")



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

function process_xbrl_filings()

    df, df_error = ESEF.get_esef_xbrl_filings()

    df = @chain df begin
        @subset(:xbrl_json_path != nothing)
        @transform(:xbrl_json_url = "https://filings.xbrl.org/" * :filing_key * "/" * HTTP.escapeuri(:xbrl_json_path))
        @select(:xbrl_json_url)
    end

    df_facts = DataFrame()

    for r in eachrow(df)
        df_ = pluck_xbrl_json(r[:xbrl_json_url])
        df_rdf = @chain df_ begin
            # TODO: Rethink normalization, instead of using uuid for facts at RDF subject field
            @transform(:rdf_line = "<http://example.org/" * string(uuid4()) * "> <http://example.org/" * HTTP.escapeuri(:predicate) * "> <http://example.org/" * HTTP.escapeuri(:object) * "> .")
            @select(:rdf_line)
        end
        append!(df_facts, df_rdf)
    end

    rm("oxigraph_rdf.nt")

    open("oxigraph_rdf.nt", "w") do io
        writedlm(io, df_facts[:, :rdf_line])
    end


    oxigraph_process = serve_oxigraph(; nt_file_path = "oxigraph_rdf.nt", keep_open = true)

    # Rollup of all concepts available from ESEF data using XBRL's filings API
    df_concepts = export_concept_count_table()
    Arrow.write("concept_df.arrow", df_concepts)

    # table = Arrow.Table("concept_df.arrow")

    kill(oxigraph_process)
end
