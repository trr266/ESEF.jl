using HTTP
using Chain
using DataFrames
using DataFrameMacros
using CSV
using JSON
using Memoization

@memoize function get_xbrl_json_doc(xbrl_json_path)
    url = "https://filings.xbrl.org" * xbrl_json_path

    @info "Fetching: $url"
    
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
            flat_fact["dimensions.entity"] = replace(
                flat_fact["dimensions.entity"], "scheme:" => ""
            )
        end

        for (k_subfact, v_subfact) in flat_fact
            push!(
                finished_facts,
                NamedTuple{(:subject, :predicate, :object)}([
                    k, k_subfact, string(v_subfact)
                ]),
            )
        end
    end

    return finished_facts
end

function get_error_messages(error_json_path)
    sleep(0.9)
    url = "https://filings.xbrl.org" * error_json_path

    @info "Fetching: $url"

    r = HTTP.get(url)

    # Check 200 HTTP status code
    @assert(r.status == 200)

    raw_data = @chain r.body begin
        String()
        JSON.parse()
    end
    
    if length(raw_data["data"]) == 0
        return DataFrame(attributes=[], id=[], type=[])
    end
    return DataFrame(raw_data["data"]) #[!, [:attributes, :id, :type]]
end

function get_error_messages(; debug=false)
    debug_flag = debug ? "_debug" : ""
    f = ".cache/esef_error_messages$debug_flag.arrow"

    if !debug
        if !isdir(".cache")
            mkdir(".cache")
        end
        
        if isfile(f)
            df = DataFrame(Arrow.Table(f))
            return df
        end
    end

    df_xbrl_raw = get_esef_xbrl_filings(debug=debug)

    if debug
        df_xbrl_raw = first(df_xbrl_raw, 5)
    end

    df_xbrl_raw = @chain df_xbrl_raw begin
        @subset(:error_json_path != nothing)
        @transform(:error_json_path = replace(:error_json_path, " " => "%20"))
    end

    df_esef_error = DataFrame()

    for r in eachrow(df_xbrl_raw)
        error_json_path = r[:error_json_path]
        df_ = get_error_messages(error_json_path)
        df_[!, :error_json_path] .= r[:error_json_path]
        append!(df_esef_error, df_)
    end

    df_esef_error = leftjoin(df_xbrl_raw, df_esef_error, on=:error_json_path)

    df_esef_error = @chain df_esef_error begin
        @transform(
            :severity = :attributes["severity"],
            :message = :attributes["message"],
            :error_code = :attributes["code"],
        )
    end
    return df_esef_error
end

@memoize function get_esef_xbrl_filings(url)
    sleep(0.9)

    r = HTTP.get(url)
    
    # Check 200 HTTP status code
    @assert(r.status == 200)

    raw_data = @chain r.body begin
        String()
        JSON.parse()
    end

    df = DataFrame()
    row_names = (
        :entity_name,
        :country_alpha_2,
        :date,
        :filing_key,
        :error_count,
        :xbrl_json_path,
        :error_json_path,
    )

    df_error = DataFrame()

    next_url = nothing
    if haskey(raw_data["links"], "next")
        next_url = raw_data["links"]["next"]
    end

    # Parse XBRL ESEF Index Object
    for d_value in raw_data["data"]
        entity_name = split(d_value["relationships"]["entity"]["links"]["related"], "/")[end]

        attributes = d_value["attributes"]
        filing_key = attributes["fxo_id"]
        error_count = attributes["error_count"]

        country = attributes["country"]
        date = attributes["period_end"]

        xbrl_json_path = nothing

        # TODO: Figure out why this errors / make missing-field tolerant
        if haskey(attributes, "json_url")
            xbrl_json_path = attributes["json_url"]
            xbrl_json_path = xbrl_json_path == "" ? nothing : xbrl_json_path
        end

        error_json_path = nothing
        if haskey(d_value["relationships"], "validation_messages")
            error_json_path = d_value["relationships"]["validation_messages"]["links"]["related"]
        end

        new_row = NamedTuple{row_names}([
            entity_name,
            country,
            date,
            filing_key,
            error_count,
            xbrl_json_path,
            error_json_path,
        ])
        push!(df, new_row; promote=true)
    end

    return df, next_url
end

function get_esef_xbrl_filings(; debug=false)
    debug_flag = debug ? "_debug" : ""
    f = ".cache/esef_xbrl_filings_list$debug_flag.arrow"

    if !debug
        if !isdir(".cache")
            mkdir(".cache")
        end
        
        if isfile(f)
            df = DataFrame(Arrow.Table(f))
            return df
        end
    end
    df = DataFrame()

    next_url = "https://filings.xbrl.org/api/filings?page[size]=200"

    while !isnothing(next_url)
        @info "Fetching: $next_url"
        df_, next_url = get_esef_xbrl_filings(next_url)
        append!(df, df_)
        if debug
            break
        end
    end

    df = @transform! df @subset(
        begin
            :country_alpha_2 == "CS"
        end
    ) begin
        :country_alpha_2 = "CZ"
    end

    # Add in country names
    country_lookup = get_wikidata_country_iso2_lookup()
    # TODO: Make sure Czechia is joined correctly
    df = @chain df begin
        leftjoin(_, country_lookup; on=:country_alpha_2)
    end

    if !debug
        Arrow.write(f, df)
    end
    return df
end

function calculate_country_rollup(df)
    country_rollup = @chain df begin
        @subset(!ismissing(:countryLabel))
        @groupby(:countryLabel)
        @combine(:report_count = length(:countryLabel))
        @transform(:report_count = coalesce(:report_count, 0))
        @sort(:report_count; rev=true)
    end
    return country_rollup
end
