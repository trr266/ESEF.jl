using ESEF
using Chain
using DataFrameMacros
using HTTP
using JSON
using DataFrames

df, df_error = ESEF.get_esef_xbrl_filings()

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
        flat_fact["dimensions.entity"] = replace(flat_fact["dimensions.entity"], "scheme:" => "")
        for (k_subfact, v_subfact) in flat_fact
            push!(finished_facts, NamedTuple{(:subject, :predicate, :object)}([k, k_subfact, string(v_subfact)]))
        end
    end

    return finished_facts
end

df1 = @chain df begin
    @transform(:xbrl_json_url = "https://filings.xbrl.org/" * :filing_key * "/" * :xbrl_json_path)
    @select(:xbrl_json_url)
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

strip

@chain 

df_ = pluck_xbrl_json(df1[1, 1])
df_rdf = @chain df_ begin
    @transform(:rdf_line = "<http://example.org/" * HTTP.escapeuri(:subject) * "> <http://example.org/" * HTTP.escapeuri(:predicate) * "> <http://example.org/" * HTTP.escapeuri(:object) * "> .")
    @select(:rdf_line)
end


using DelimitedFiles

open("oxigraph_rdf.nt", "w") do io
    writedlm(io, df_rdf[:, :rdf_line])
end


serve_oxigraph(; nt_file_path = "oxigraph_rdf.nt")
